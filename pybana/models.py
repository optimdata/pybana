# -*- coding: utf-8 -*-

import json
from typing import List, Optional, TypeVar
from abc import abstractmethod

from elasticsearch import NotFoundError
from elasticsearch_dsl import Document, Keyword, Search as EsSearch

from pybana.kibana_refs import (
    first_input_control_index_pattern_ref,
    resolve_index_pattern_document_id,
)

__all__ = (
    "BaseDocument",
    "Config",
    "DataView",
    "IndexPattern",
    "Visualization",
    "DocumentT",
    "Dashboard",
    "get_index_pattern_or_data_view",
    "get_index_pattern_or_data_view_flexible",
)


class BaseDocument(Document):
    type = Keyword()
    # _using = None

    # List of json attributes.
    json_attrs = []

    class Meta:
        doc_type = "doc"

    class Index:
        # We use elasticsearch_dsl 6.3 behaviour here as we don't know in advance
        # the name of the kibana index
        name = "*"

    def __init__(self, **kwargs):
        super().__init__(type=self._type, **kwargs)
        self._json_attrs_cache = {}

    def __getattr__(self, key):
        if key in self.json_attrs:
            if key not in self._json_attrs_cache:
                self._json_attrs_cache[key] = json.loads(
                    self.to_dict()[self._type].get(key, "null")
                )
            return self._json_attrs_cache[key]
        return super().__getattr__(key)


DocumentT = TypeVar(
    "DocumentT", bound=BaseDocument
)  # the variable name must coincide with the string


class KibanaInterface:
    @abstractmethod
    def get_document(
        self, klass: type[DocumentT], id: str, using=None, raise_on_missing=True
    ) -> Optional[DocumentT]:
        raise NotImplementedError()

    @abstractmethod
    def all_documents(
        self,
        klass: type[DocumentT],
        ids: Optional[List[str]] = None,
        raise_on_missing=True,
    ) -> List[DocumentT]:
        raise NotImplementedError()


def get_index_pattern_or_data_view(
    document_id, index, using=None, kibana: Optional[KibanaInterface] = None
):
    """
    Load an index-pattern or data-view saved object by full Elasticsearch document _id.
    """
    if kibana is not None:
        assert isinstance(kibana, KibanaInterface)
        print(f"kibana: {kibana.__class__.__name__}, document_id:{document_id}")
        if document_id.startswith("data-view:"):
            return kibana.get_document(
                klass=DataView, id=document_id.split(":")[-1], raise_on_missing=False
            )
        return kibana.get_document(
            klass=IndexPattern, id=document_id.split(":")[-1], raise_on_missing=False
        )
    if document_id.startswith("data-view:"):
        return DataView.get(id=document_id, index=index, using=using)
    return IndexPattern.get(id=document_id, index=index, using=using)


def get_index_pattern_or_data_view_flexible(
    raw_ref, index, using=None, kibana: Optional[KibanaInterface] = None
):
    """
    Resolve an index-pattern or data-view from a raw ``visState`` reference (id or title).

    Tries ``index-pattern:{raw_ref}`` and ``data-view:{raw_ref}``, then an exact
    title match on ``index-pattern.title`` / ``data-view.title`` (first hit).
    """
    assert kibana is None or isinstance(kibana, KibanaInterface)

    if not raw_ref:
        return None
    if raw_ref.startswith("data-view:") or raw_ref.startswith("index-pattern:"):
        return get_index_pattern_or_data_view(
            raw_ref, index, using=using, kibana=kibana
        )
    for prefix in ("index-pattern:", "data-view:"):
        doc_id = prefix + raw_ref
        try:
            return get_index_pattern_or_data_view(
                doc_id, index, using=using, kibana=kibana
            )
        except NotFoundError:
            pass
    for obj_type, klass in (("index-pattern", IndexPattern), ("data-view", DataView)):
        field = "%s.title" % obj_type
        hits = (
            EsSearch(index=index, using=using)
            .filter("term", type=obj_type)
            .filter("match_phrase", **{field: raw_ref})
            .extra(size=1)
            .execute()
        )
        if hits.hits:
            return klass.get(id=hits.hits[0].meta.id, index=index, using=using)
    return None


class KibanaSavedObjectReferencesMixin(object):
    """
    Kibana 8+ stores outbound links in a root-level ``references`` array on saved objects.
    elasticsearch-dsl drops unknown fields unless we capture them in ``from_es``.
    """

    @classmethod
    def from_es(cls, hit, *args, **kwargs):
        src = hit.get("_source") or {}
        refs = src.get("references")
        inst = super(KibanaSavedObjectReferencesMixin, cls).from_es(
            hit, *args, **kwargs
        )
        inst._kibana_references = list(refs) if refs else []
        return inst


class Config(BaseDocument):
    _type = "config"


class IndexPattern(BaseDocument):
    _type = "index-pattern"
    json_attrs = ["fields", "fieldFormatMap"]


class DataView(BaseDocument):
    """
    Kibana 8+ data view saved object (same role as index-pattern for queries).
    Stored under ``type: data-view`` with a ``data-view`` property bag in ``_source``.
    """

    _type = "data-view"
    json_attrs = ["fields", "fieldFormatMap"]


class Search(KibanaSavedObjectReferencesMixin, BaseDocument):
    _type = "search"

    def index(
        self, using=None, raise_error=True, kibana: Optional[KibanaInterface] = None
    ):
        """
        Returns the index-pattern associated to the visualization. Go through the
        search if needed.
        """
        search_source = self.search["kibanaSavedObjectMeta"]["searchSourceJSON"]
        refs = getattr(self, "_kibana_references", [])
        doc_id = resolve_index_pattern_document_id(search_source, refs)
        if not doc_id:
            if raise_error:
                raise ValueError(
                    "Could not resolve data source from searchSourceJSON (missing index / references)"
                )
            return None
        return get_index_pattern_or_data_view(
            doc_id, self.meta.index, using=using, kibana=kibana
        )


class Visualization(KibanaSavedObjectReferencesMixin, BaseDocument):
    _type = "visualization"
    json_attrs = ["visState", "uiStateJSON"]

    def related_search(self, using):
        """
        Returns the search associated to the visualization.

        An error is raised if the visualization is not associated to any search.
        """
        return Search.get(
            id=f"search:{self.visualization.savedSearchId}",
            index=self.meta.index,
            using=using,
        )

    def index(self, using=None, raise_error=True, kibana: Optional[KibanaInterface] = None):
        """
        Returns the index-pattern associated to the visualization. Go through the
        search if needed.
        """
        if hasattr(self.visualization, "savedSearchId"):
            return self.related_search(using=using).index(
                using=using, raise_error=raise_error
            )
        search_source = self.visualization.kibanaSavedObjectMeta.searchSourceJSON
        refs = getattr(self, "_kibana_references", [])
        doc_id = resolve_index_pattern_document_id(search_source, refs)
        if doc_id:
            return get_index_pattern_or_data_view(
                doc_id, self.meta.index, using=using, kibana=kibana
            )
        raw = first_input_control_index_pattern_ref(self.visState)
        if raw:
            resolved = get_index_pattern_or_data_view_flexible(
                raw, self.meta.index, using=using, kibana=kibana
            )
            if resolved is not None:
                return resolved
        if raise_error:
            raise ValueError(
                "Could not resolve data source from searchSourceJSON, references, "
                "or input control visState (missing index / indexPattern)"
            )
        return None

    def filters(self):
        """
        Returns the search filters
        :return elasticsearch_dsl.Q
        """
        from pybana import FilterTranslator

        search_source = self.visualization.kibanaSavedObjectMeta.searchSourceJSON
        filters = json.loads(search_source).get("filter", [])
        return FilterTranslator().translate(filters)


class Dashboard(BaseDocument):
    _type = "dashboard"
    json_attrs = ["panelsJSON", "optionsJSON"]

    def visualizations(
        self, *, using=None, missing="skip", kibana: Optional[KibanaInterface] = None
    ):
        """
        Does the join automatically by parsing references.

        :param str missing: Check https://elasticsearch-dsl.readthedocs.io/en/latest/api.html#elasticsearch_dsl.Document.mget
        :param str using: connection alias to use, defaults to ``'default'``
        """
        panels_ref = self.references if hasattr(self, "references") else self.panelsJSON
        panels = [panel for panel in panels_ref if panel.type == "visualization"]
        if not panels:
            return []
        if missing == "skip" and kibana is not None:
            return kibana.all_documents(
                Visualization,
                ids=[panel["id"] for panel in panels],
                raise_on_missing=False,
            )
        return Visualization.mget(
            docs=["visualization:" + panel["id"] for panel in panels],
            index=self.meta.index,
            missing=missing,
            using=using,
        )

    def searches(self, *, using, missing="skip"):
        """
        Does the join automatically by parsing references.

        :param str missing: Check https://elasticsearch-dsl.readthedocs.io/en/latest/api.html#elasticsearch_dsl.Document.mget
        :param str using: connection alias to use, defaults to ``'default'``
        """
        panels_ref = self.references if hasattr(self, "references") else self.panelsJSON
        panels = [panel for panel in panels_ref if panel.type == "search"]
        return (
            Search.mget(
                docs=["search:" + panel["id"] for panel in panels],
                index=self.meta.index,
                missing=missing,
                using=using,
            )
            if panels
            else []
        )
