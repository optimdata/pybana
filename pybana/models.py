# -*- coding: utf-8 -*-

import json

from elasticsearch_dsl import Document, Keyword

from pybana.kibana_refs import resolve_index_pattern_document_id

__all__ = (
    "BaseDocument",
    "Config",
    "DataView",
    "IndexPattern",
    "Visualization",
    "Dashboard",
    "get_index_pattern_or_data_view",
)


def get_index_pattern_or_data_view(document_id, index, using=None):
    """
    Load an index-pattern or data-view saved object by full Elasticsearch document _id.
    """
    if document_id.startswith("data-view:"):
        return DataView.get(id=document_id, index=index, using=using)
    return IndexPattern.get(id=document_id, index=index, using=using)


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


class BaseDocument(Document):
    type = Keyword()

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

    def index(self, using=None):
        """
        Returns the index-pattern associated to the visualization. Go through the
        search if needed.
        """
        search_source = self.search["kibanaSavedObjectMeta"]["searchSourceJSON"]
        refs = getattr(self, "_kibana_references", [])
        doc_id = resolve_index_pattern_document_id(search_source, refs)
        if not doc_id:
            raise ValueError(
                "Could not resolve data source from searchSourceJSON (missing index / references)"
            )
        return get_index_pattern_or_data_view(doc_id, self.meta.index, using=using)


class Visualization(KibanaSavedObjectReferencesMixin, BaseDocument):
    _type = "visualization"
    json_attrs = ["visState", "uiStateJSON"]

    def related_search(self, using=None):
        """
        Returns the search associated to the visualization.

        An error is raised if the visualization is not associated to any search.
        """
        return Search.get(
            id=f"search:{self.visualization.savedSearchId}",
            index=self.meta.index,
            using=using,
        )

    def index(self, using=None):
        """
        Returns the index-pattern associated to the visualization. Go through the
        search if needed.
        """
        if hasattr(self.visualization, "savedSearchId"):
            return self.related_search(using=using).index(using=using)
        search_source = self.visualization.kibanaSavedObjectMeta.searchSourceJSON
        refs = getattr(self, "_kibana_references", [])
        doc_id = resolve_index_pattern_document_id(search_source, refs)
        if not doc_id:
            raise ValueError(
                "Could not resolve data source from searchSourceJSON (missing index / references)"
            )
        return get_index_pattern_or_data_view(doc_id, self.meta.index, using=using)

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

    def visualizations(self, missing="skip", using=None):
        """
        Does the join automatically by parsing panelsJSON.

        :param str missing: Check https://elasticsearch-dsl.readthedocs.io/en/latest/api.html#elasticsearch_dsl.Document.mget
        :param str using: connection alias to use, defaults to ``'default'``
        """
        panels = [panel for panel in self.panelsJSON if panel.type == "visualization"]
        return (
            Visualization.mget(
                docs=["visualization:" + panel["id"] for panel in panels],
                index=self.meta.index,
                missing=missing,
                using=using,
            )
            if panels
            else []
        )

    def searches(self, missing="skip", using=None):
        """
        Does the join automatically by parsing panelsJSON.

        :param str missing: Check https://elasticsearch-dsl.readthedocs.io/en/latest/api.html#elasticsearch_dsl.Document.mget
        :param str using: connection alias to use, defaults to ``'default'``
        """
        panels = [panel for panel in self.panelsJSON if panel.type == "search"]
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
