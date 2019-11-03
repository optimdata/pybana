# -*- coding: utf-8 -*-

import json

from elasticsearch_dsl import Document, Keyword

__all__ = ("BaseDocument", "Config", "IndexPattern", "Visualization", "Dashboard")


class BaseDocument(Document):
    type = Keyword()

    class Meta:
        doc_type = "doc"

    def __init__(self, **kwargs):
        super().__init__(type=self._type, **kwargs)


class Config(BaseDocument):
    _type = "config"


class IndexPattern(BaseDocument):
    _type = "index-pattern"


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

    def state(self):
        return json.loads(self.visualization.visState)

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


class Dashboard(BaseDocument):
    _type = "dashboard"

    def panels(self):
        return json.loads(self.dashboard.panelsJSON)

    def visualizations(self, missing="skip"):
        """
        Does the join automatically by parsing panelsJSON.

        :param missing: Check https://elasticsearch-dsl.readthedocs.io/en/latest/api.html#elasticsearch_dsl.Document.mget
        :type string
        """
        panels = self.panels()
        return (
            Visualization.mget(
                docs=["visualization:" + panel["id"] for panel in panels],
                index=self.meta.index,
                missing="skip",
            )
            if panels
            else []
        )
