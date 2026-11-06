# -*- coding: utf-8 -*-

import copy
import json
import os
from typing import Dict, List, Optional, TypeVar, Set, Tuple

from elasticsearch import NotFoundError, Elasticsearch
import elasticsearch_dsl

from pybana.elastic.elastic_client import ElasticsearchExtClient
from pybana.elastic.elastic_client_interface import ESClientInterface

from .models import (
    BaseDocument,
    Config,
    Dashboard,
    DataView,
    IndexPattern,
    Visualization,
    Search,
)

__all__ = ("Kibana",)

T = TypeVar("T", bound=BaseDocument)  # the variable name must coincide with the string


DEFAULT_CONFIG = {
    "timepicker:timeDefaults": json.dumps(
        {"from": "now-7d", "to": "now", "mode": "quick"}
    ),
    "dateFormat:tz": None,
    "state:storeInSessionStorage": True,
    "telemetry:optIn": False,
    "defaultIndex": None,
}

# Fields required to search visualizations by type (visState) in synoptics / API.
# Kibana 8 ``.kibana_*_analytics`` indices often omit these under ``dynamic: false``.
VISUALIZATION_MAPPING_PROPERTIES = {
    "kibanaSavedObjectMeta": {
        "properties": {
            "searchSourceJSON": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
            }
        }
    },
    "savedSearchId": {"type": "keyword"},
    "uiStateJSON": {
        "type": "text",
        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
    },
    "visState": {
        "type": "text",
        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
    },
}


class _DocumentsCache:
    def __init__(self) -> None:
        self._cache: Dict[str, Dict[str, Optional[BaseDocument]]] = {}

    def clear(self):
        self._cache.clear()

    def _read_subcache(self, klass: type[T]) -> Optional[Dict[str, Optional[T]]]:
        return self._cache.get(klass._type)

    def _write_subcache(self, klass: type[T]) -> Dict[str, Optional[T]]:
        key = klass._type
        if key not in self._cache:
            self._cache[key] = {}
        return self._cache[key]

    # klass: type[T], ids: Optional[List[str]] = None) -> List[T]:
    def get(self, klass: type[T], key: str) -> Tuple[bool, Optional[T]]:
        assert ":" not in key
        sub_cache = self._read_subcache(klass)
        if sub_cache is None:
            return False, None
        return key in sub_cache, sub_cache.get(key)

    def get_multi(self, klass: type[T], keys: List[str]) -> Tuple[List[T], List[str]]:
        sub_cache = self._read_subcache(klass)
        if sub_cache is None:
            return [], keys
        documents: List[T] = []
        not_founds: List[str] = []
        for key in keys:
            assert ":" not in key
            found = key in sub_cache
            if not found:
                not_founds.append(key)
            else:
                document = sub_cache[key]
                if document is not None:
                    documents.append(document)
        return documents, not_founds

    def get_all(self, klass: type[T]) -> Optional[List[T]]:
        sub_cache = self._read_subcache(klass)
        if sub_cache is None:
            return None
        return [v for v in sub_cache.values() if v is not None]

    def set(self, klass: type[T], key: str, document: Optional[T]) -> Optional[T]:
        assert ":" not in key
        sub_cache = self._write_subcache(klass)
        sub_cache[key] = document
        return document

    def delete_all(self, klass: type[T]):
        self._cache.pop(klass._type, None)

    def set_all(self, klass: type[T], documents: List[T]) -> List[T]:
        self.delete_all(klass)
        self.set_multi(klass=klass, documents=documents)
        self.set(klass=klass, key=".*", document=None)
        return documents

    def set_multi(
        self,
        klass: type[T],
        documents: List[T],
        wished_ids: Optional[Set[str]] = None,
    ) -> List[str]:
        ids = []
        for document in documents:
            doc_id = document.meta.id
            parts = doc_id.split(":")
            assert len(parts) == 2
            assert parts[0] == klass._type
            key = parts[1]
            assert bool(key)
            assert key != ".*"
            if wished_ids is not None:
                assert key in wished_ids
            ids.append(key)
            self.set(klass, key, document)
        if wished_ids is not None:
            set_ids = set(ids)
            for id in wished_ids:
                if id not in set_ids:
                    self.set(klass, id, None)
                    ids.append(id)
        return ids


class Kibana:
    """
    Kibana client (with support of multi elasticsearch).
    """

    klasses = {
        "dashboard": Dashboard,
        "visualization": Visualization,
        "index-pattern": IndexPattern,
        "data-view": DataView,
        "search": Search,
    }

    def get_es(self, using) -> ESClientInterface:
        using = using or self._default
        if isinstance(using, ESClientInterface):
            return using
        es = elasticsearch_dsl.connections.get_connection(using)
        if isinstance(es, ESClientInterface):
            # normal use case
            return es
        if not isinstance(es, Elasticsearch):
            # must check more:
            # raise RuntimeError(f"unexpected class: {es.__class__.__name__}: {es}")
            return es
        es_ext = ElasticsearchExtClient(es)
        if isinstance(using, str):
            elasticsearch_dsl.connections.add_connection(using, es_ext)
        return es_ext

    def __init__(self, *, using, index=".kibana", start_cache=False):
        """
        Initialize a client to kibana.

        :param index string: Index used by kibana (default: .kibana).
        """
        self._default = self.get_es(using)
        self._index = index
        self._cache: Optional[_DocumentsCache] = None
        if start_cache:
            self.cache_initialize()

    @property
    def cache_active(self):
        return self._cache is not None

    def cache_initialize(self):
        if self._cache is None:
            self._cache = _DocumentsCache()

    def cache_stop(self):
        self._cache = None

    def cache_clear(self):
        if self._cache is not None:
            self._cache.clear()

    def _cache_get(self, klass: type[T], key: str) -> Tuple[bool, Optional[T]]:
        return (False, None) if self._cache is None else self._cache.get(klass, key)

    def _cache_get_multi(
        self, klass: type[T], keys: List[str]
    ) -> Tuple[List[T], List[str]]:
        return ([], keys) if not self._cache else self._cache.get_multi(klass, keys)

    def _cache_get_all(self, klass: type[T]) -> Optional[List[T]]:
        return None if not self._cache else self._cache.get_all(klass)

    def _cache_set(
        self, klass: type[T], key: str, document: Optional[T]
    ) -> Optional[T]:
        return (
            document if self._cache is None else self._cache.set(klass, key, document)
        )

    def _cache_delete_all(self, klass: type[T]):
        if self._cache is not None:
            self._cache.delete_all(klass)

    def _cache_set_all(self, klass: type[T], documents: List[T]) -> List[T]:
        return (
            documents if self._cache is None else self._cache.set_all(klass, documents)
        )

    def _cache_set_multi(
        self,
        klass: type[T],
        documents: List[T],
        wished_ids: Optional[Set[str]] = None,
    ) -> List[str]:
        return (
            []
            if self._cache is None
            else self._cache.set_multi(klass, documents, wished_ids=wished_ids)
        )

    @property
    def using(self):
        return self._default

    def _search(self, type, using):
        klass = self.klasses.get(type)
        search = klass.search if klass else elasticsearch_dsl.Search
        es = self.get_es(using)
        return search(index=self._index, using=es)

    def _get(
        self, klass: type[T], id: str, using, raise_on_missing=True
    ) -> Optional[T]:
        found, document = self._cache_get(klass, id)
        if found:
            if document is None and raise_on_missing:
                raise NotFoundError()
            return document
        try:
            return self._cache_set(
                klass=klass,
                key=id,
                document=klass.get(
                    index=self._index,
                    id=f"{klass._type}:{id}",
                    using=self.get_es(using),
                ),
            )
        except NotFoundError:
            self._cache_set(klass=klass, key=id, document=None)
            if not raise_on_missing:
                return None
            raise

    def objects(self, type, using=None):
        return self._search(type, using=using).filter("term", type=type)

    def all_documents(self, klass: type[T], ids: Optional[List[str]] = None) -> List[T]:
        if ids is None:
            values = self._cache_get_all(klass=klass)
            if values is not None:
                return values
            values = list(self.objects(type=klass._type, using=None).scan())
            for v in values:
                assert v is None or isinstance(v, klass)
            return self._cache_set_all(klass=klass, documents=values)
        if not ids:
            return []
        documents, short_ids = self._cache_get_multi(
            klass=klass, keys=[id.split(":")[-1] for id in ids]
        )
        doc_ids = [f"{klass._type}:{id}" for id in short_ids]
        if not doc_ids:
            return documents

        response = self.get_es(using=None).mget(
            body={"ids": doc_ids}, index=self._index, doc_type="doc"
        )

        for doc in response.get("docs", []):
            if not doc.get("found"):
                continue
            # ES 7+ mget hits have no ``_type``; elasticsearch-dsl 6 expects one.
            if "_type" not in doc:
                doc["_type"] = "doc"
            document = self._cache_set(klass=klass, key="", document=klass.from_es(doc))
            if document is not None:
                documents.append(document)
        return documents

    def config_id(self, using=None):
        elastic = self.get_es(using)
        return "config:%s" % elastic.info()["version"]["number"]

    def config(self, using=None, raise_on_missing=True) -> Config:
        """
        Return the config associated to the current version of elastic
        """
        config_id = self.config_id(using)
        try:
            return self._get(
                Config,
                config_id.split(":")[-1],
                using=using,
                raise_on_missing=raise_on_missing,
            )
        except NotFoundError:
            # Kibana config ids are versioned (e.g. config:8.18.4). In tests the
            # seeded config may exist for another patch version, so fallback to
            # any config document for the same major version.
            major_version = config_id.split(":")[-1].split(".")[0]
            hits = self.objects("config", using=using).extra(size=50).execute()
            for hit in hits:
                hit_id = hit.meta.id
                if hit_id.startswith(f"config:{major_version}."):
                    return self._get(
                        Config,
                        hit_id.split(":")[-1],
                        using=using,
                        raise_on_missing=raise_on_missing,
                    )
            raise

    def is_v8(self, using=None):
        elastic = self.get_es(using)
        version = elastic.info()["version"]["number"].split(".")[0]
        return version >= "8"

    def init_index(self, using=None):
        """
        Create the elasticsearch index as kibana would do.
        """
        elastic = self.get_es(using)
        mappingsfn = os.path.join(os.path.dirname(__file__), "mappings.json")
        suffix = 1
        created = False
        while not elastic.indices.exists(self._index):
            index = f"{self._index}_{suffix}"
            if not elastic.indices.exists(index):
                with open(mappingsfn) as fd:
                    elastic.indices.create(index, body=json.load(fd))
                    elastic.indices.put_alias(index=index, name=self._index)
                created = True
                break
            suffix += 1
        # Existing Kibana 8 analytics indices may lack searchable visState; fix them.
        self.ensure_visualization_mapping(using=using, reindex=not created)

    def _visualization_properties_from_mapping(self, index_mapping):
        """
        Return visualization.properties from a get_mapping response entry.
        """
        mappings = index_mapping.get("mappings", {})
        if "doc" in mappings:
            properties = mappings["doc"].get("properties", {})
        else:
            properties = mappings.get("properties", {})
        return properties.get("visualization", {}).get("properties", {})

    def visualization_mapping_needs_update(self, using=None):
        """
        True if visualization.visState is missing from the index mapping.
        """
        elastic = self.get_es(using)
        if not elastic.indices.exists(self._index):
            return False
        mappings = elastic.indices.get_mapping(index=self._index)
        for index_mapping in mappings.values():
            visu_props = self._visualization_properties_from_mapping(index_mapping)
            if "visState" not in visu_props:
                return True
        return False

    def ensure_visualization_mapping(self, using=None, reindex=True):
        """
        Ensure visualization fields (especially visState) are mapped and searchable.

        Kibana 8 ``.kibana_*_analytics`` indices often define visualization with
        ``dynamic: false`` and omit ``visState`` / ``uiStateJSON``, which breaks
        type-filtered listing used by synoptics iframes.

        :param reindex: If True, reindex existing visualization docs so newly
            mapped fields become searchable (required after put_mapping).
        :return: True if the mapping was updated, False if already complete.
        """
        elastic = self.get_es(using)
        if not elastic.indices.exists(self._index):
            return False
        if not self.visualization_mapping_needs_update(using=using):
            return False

        body = {
            "properties": {
                "visualization": {"properties": VISUALIZATION_MAPPING_PROPERTIES}
            }
        }
        # doc_type is required by the client wrapper; stripped for ES 7+.
        elastic.indices.put_mapping("doc", body, index=self._index)

        if reindex:
            elastic.update_by_query(
                index=self._index,
                body={"query": {"term": {"type": "visualization"}}},
                conflicts="proceed",
                refresh=True,
                wait_for_completion=True,
            )
        return True

    def init_config(self, using=None):
        """
        Create the config document that each kibana requires. This
        document stores all the settings such as timepicker defaults,
        date formats etc
        """
        try:
            self.config(using=using)
        except NotFoundError:
            config = Config(
                config=copy.deepcopy(DEFAULT_CONFIG),
                meta={"id": self.config_id(using=using)},
            )
            config.save(index=self._index, refresh="wait_for", using=self.get_es(using))
            self._cache_set(Config, config.meta.id.split(":")[-1], config)

    def index_patterns(self, using=None):
        """
        Return a Search to all the index-patterns.
        """
        return self.objects("index-pattern", using=using)

    def all_index_patterns(self, ids: Optional[List[str]] = None) -> List[IndexPattern]:
        return self.all_documents(IndexPattern, ids=ids)

    def index_pattern(self, id, using=None, raise_on_missing=True) -> IndexPattern:
        """
        Return a index-pattern identified by its identifier.
        """
        return self._get(
            IndexPattern, id, using=using, raise_on_missing=raise_on_missing
        )

    def searches(self, using=None):
        """
        Return a Search to all the searches.
        """
        return self.objects("search", using=using)

    def search(self, id, using=None, raise_on_missing=True) -> Search:
        """
        Return a index-pattern identified by its identifier.
        """
        return self._get(Search, id, using=using, raise_on_missing=raise_on_missing)

    def visualizations(self, using=None):
        """
        Return a Search to all the visualizations.

        When ``ids`` is set, load those visualizations with Elasticsearch mget.
        """
        return self.objects("visualization", using=using)

    def all_visualizations(
        self, ids: Optional[List[str]] = None
    ) -> List[Visualization]:
        return self.all_documents(Visualization, ids=ids)

    def visualization(self, id, using=None, raise_on_missing=True) -> Visualization:
        """
        Return a visualization identified by its identifier.
        """
        return self._get(
            Visualization, id, using=using, raise_on_missing=raise_on_missing
        )

    def dashboards(self, using=None):
        """
        Return a Search to all the dashboards.

        When ``ids`` is set, load those dashboards with Elasticsearch mget.
        """
        return self.objects("dashboard", using=using)

    def all_dashboards(self, ids: Optional[List[str]] = None) -> List[Dashboard]:
        """
        Return a Search to all the index-patterns.
        """
        return self.all_documents(Dashboard, ids=ids)

    def dashboard(self, id, using=None, raise_on_missing=True) -> Dashboard:
        """
        Return a dashboard identified by its identifier.
        """
        return self._get(Dashboard, id, using=using, raise_on_missing=raise_on_missing)

    def update_or_create_default_index_pattern(self, index_pattern, using=None):
        """
        If config document does not have index pattern, associate the
        first index pattern found.
        """
        config = self.config(using)
        if not config.config.to_dict().get("defaultIndex"):
            config.config.defaultIndex = index_pattern.meta.id.split(":")[-1]
            config.save(refresh="wait_for", using=using or self.using)
