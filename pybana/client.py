# -*- coding: utf-8 -*-

import json
import os

from elasticsearch import NotFoundError, Elasticsearch
import elasticsearch_dsl

from pybana.elastic.elastic_client import ElasticsearchExtClient

from .models import Config, Dashboard, DataView, IndexPattern, Visualization, Search

__all__ = ("Kibana",)

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
                "fields": {
                    "keyword": {"type": "keyword", "ignore_above": 256}
                },
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

    def get_es(self, using):
        using = using or self._default
        if isinstance(using, ElasticsearchExtClient):
            return using
        es = elasticsearch_dsl.connections.get_connection(using)
        if not isinstance(es, Elasticsearch):
            return es
        es_ext = ElasticsearchExtClient(es)
        if isinstance(using, str):
            elasticsearch_dsl.connections.add_connection(using, es_ext)
        return es_ext

    def __init__(self, *, using, index=".kibana"):
        """
        Initialize a client to kibana.

        :param index string: Index used by kibana (default: .kibana).
        """
        self._default = self.get_es(using)
        self._index = index

    @property
    def using(self):
        return self._default

    def _search(self, type, using):
        klass = self.klasses.get(type)
        search = klass.search if klass else elasticsearch_dsl.Search
        es = self.get_es(using)
        return search(index=self._index, using=es)

    def _get(self, klass, id, using):
        return klass.get(index=self._index, id=id, using=self.get_es(using))

    def objects(self, type, using=None):
        return self._search(type, using=using).filter("term", type=type)

    def config_id(self, using=None):
        elastic = self.get_es(using)
        return "config:%s" % elastic.info()["version"]["number"]

    def config(self, using=None):
        """
        Return the config associated to the current version of elastic
        """
        return self._get(Config, self.config_id(using), using=using)

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
            Config(
                config=DEFAULT_CONFIG, meta={"id": self.config_id(using=using)}
            ).save(index=self._index, refresh="wait_for", using=self.get_es(using))

    def index_patterns(self, using=None):
        """
        Return a Search to all the index-patterns.
        """
        return self.objects("index-pattern", using=using)

    def index_pattern(self, id, using=None):
        """
        Return a index-pattern identified by its identifier.
        """
        return self._get(
            self.klasses["index-pattern"], f"index-pattern:{id}", using=using
        )

    def searches(self, using=None):
        """
        Return a Search to all the index-patterns.
        """
        return self.objects("search", using=using)

    def search(self, id, using=None):
        """
        Return a index-pattern identified by its identifier.
        """
        return self._get(self.klasses["search"], f"search:{id}", using=using)

    def visualizations(self, using=None):
        """
        Return a Search to all the visualizations.
        """
        return self.objects("visualization", using=using)

    def visualization(self, id, using=None):
        """
        Return a visualization identified by its identifier.
        """
        return self._get(
            self.klasses["visualization"], f"visualization:{id}", using=using
        )

    def dashboards(self, using=None):
        """
        Return a Search to all the dashboards.
        """
        return self.objects("dashboard", using=using)

    def dashboard(self, id, using=None):
        """
        Return a dashboard identified by its identifier.
        """
        return self._get(self.klasses["dashboard"], f"dashboard:{id}", using=using)

    def update_or_create_default_index_pattern(self, index_pattern, using=None):
        """
        If config document does not have index pattern, associate the
        first index pattern found.
        """
        config = self.config(using)
        if not config.config.to_dict().get("defaultIndex"):
            config.config.defaultIndex = index_pattern.meta.id.split(":")[-1]
            config.save(refresh="wait_for", using=using or self.using)
