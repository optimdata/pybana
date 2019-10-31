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


class Visualization(BaseDocument):
    _type = "visualization"

    def state(self):
        return json.loads(self.visualization.visState)

    def index(self):
        """
            Return index associated to the visualization through the search.
        """
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
