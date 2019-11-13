# -*- coding: utf-8 -*-

import json
import os

from elasticsearch import NotFoundError
import elasticsearch_dsl

from .models import Config, Dashboard, IndexPattern, Visualization, Search

__all__ = ("Kibana",)

DEFAULT_CONFIG = {
    "timepicker:timeDefaults": json.dumps(
        {"from": "now-7d", "to": "now", "mode": "quick"}
    ),
    "dateFormat:tz": "UTC",
    "state:storeInSessionStorage": True,
    "telemetry:optIn": False,
    "defaultIndex": None,
}


class Kibana:
    """
    Kibana client.
    """

    klasses = {
        "dashboard": Dashboard,
        "visualization": Visualization,
        "index-pattern": IndexPattern,
        "search": Search,
    }

    def __init__(self, index=".kibana"):
        """
        Initialize a client to kibana.

        :param index string: Index used by kibana (default: .kibana).
        """
        self._index = index

    def _search(self, type, using):
        klass = self.klasses.get(type)
        search = klass.search if klass else elasticsearch_dsl.Search
        return search(index=self._index, using=using)

    def _get(self, klass, id, using):
        ret = klass.get(index=self._index, id=id, using=using)
        return ret

    def objects(self, type, using=None):
        return self._search(type, using=using).filter("term", type=type)

    def config_id(self, using=None):
        elastic = elasticsearch_dsl.connections.get_connection(using or "default")
        return "config:%s" % elastic.info()["version"]["number"]

    def config(self, using=None):
        """
        Return the config associated to the current version of elastic
        """
        return self._get(Config, self.config_id(using), using=using)

    def init_index(self):
        """
        Create the elasticsearch index as kibana would do.
        """
        elastic = elasticsearch_dsl.connections.get_connection("default")
        mappingsfn = os.path.join(os.path.dirname(__file__), "mappings.json")
        suffix = 1
        while not elastic.indices.exists(self._index):
            index = f"{self._index}_{suffix}"
            if not elastic.indices.exists(index):
                with open(mappingsfn) as fd:
                    elastic.indices.create(index, body=json.load(fd))
                    elastic.indices.put_alias(index=index, name=self._index)
                break
            suffix += 1

    def init_config(self, using=None):
        """
        Create the config document that each kibana requires. This
        document stores all the settings such as timepicker defaults,
        date formats etc
        """
        try:
            self.config(using=using)
        except NotFoundError:
            Config(config=DEFAULT_CONFIG, meta={"id": self.config_id()}).save(
                index=self._index, refresh="wait_for", using=using
            )

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
            config.save(refresh="wait_for")
