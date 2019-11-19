# -*- coding: utf-8 -*-

import json

from elasticsearch_dsl import Document, Keyword

__all__ = ("BaseDocument", "Config", "IndexPattern", "Visualization", "Dashboard")


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


class Search(BaseDocument):
    _type = "search"

    def index(self):
        """
        Returns the index-pattern associated to the visualization. Go through the
        search if needed.
        """
        search_source = self.search["kibanaSavedObjectMeta"]["searchSourceJSON"]
        key = json.loads(search_source).get("index")
        return IndexPattern.get(id=f"index-pattern:{key}", index=self.meta.index)


class Visualization(BaseDocument):
    _type = "visualization"
    json_attrs = ["visState", "uiStateJSON"]

    def related_search(self):
        """
        Returns the search associated to the visualization.

        An error is raised if the visualization is not associated to any search.
        """
        return Search.get(
            id=f"search:{self.visualization.savedSearchId}", index=self.meta.index
        )

    def index(self):
        """
        Returns the index-pattern associated to the visualization. Go through the
        search if needed.
        """
        if hasattr(self.visualization, "savedSearchId"):
            return self.related_search().index()
        search_source = self.visualization.kibanaSavedObjectMeta.searchSourceJSON
        key = json.loads(search_source).get("index")
        return IndexPattern.get(id=f"index-pattern:{key}", index=self.meta.index)

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

        :param missing: Check https://elasticsearch-dsl.readthedocs.io/en/latest/api.html#elasticsearch_dsl.Document.mget
        :type string
        """
        panels = self.panelsJSON
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
