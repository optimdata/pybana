# -*- coding: utf-8 -*-

import json

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

    def _search(self, type):
        klass = self.klasses.get(type)
        search = klass.search if klass else elasticsearch_dsl.Search
        return search(index=self._index)

    def _get(self, klass, id):
        ret = klass.get(index=self._index, id=id)
        return ret

    def objects(self, type):
        return self._search(type).filter("term", type=type)

    def config_id(self):
        elastic = elasticsearch_dsl.connections.get_connection()
        return "config:%s" % elastic.info()["version"]["number"]

    def config(self):
        return self._get(Config, self.config_id())

    def init_config(self):
        """
        Create the config document that each kibana requires. This
        document stores all the settings such as timepicker defaults,
        date formats etc
        """
        try:
            self.config()
        except NotFoundError:
            Config(config=DEFAULT_CONFIG, meta={"id": self.config_id()}).save(
                index=self._index, refresh="wait_for"
            )

    def index_patterns(self):
        """
        Return a Search to all the index-patterns.
        """
        return self.objects("index-pattern")

    def index_pattern(self, id):
        """
        Return a index-pattern identified by its identifier.
        """
        return self._get(self.klasses["index-pattern"], f"index-pattern:{id}")

    def searches(self):
        """
        Return a Search to all the index-patterns.
        """
        return self.objects("search")

    def search(self, id):
        """
        Return a index-pattern identified by its identifier.
        """
        return self._get(self.klasses["search"], f"search:{id}")

    def visualizations(self):
        """
        Return a Search to all the visualizations.
        """
        return self.objects("visualization")

    def visualization(self, id):
        """
        Return a visualization identified by its identifier.
        """
        return self._get(self.klasses["visualization"], f"visualization:{id}")

    def dashboards(self):
        """
        Return a Search to all the dashboards.
        """
        return self.objects("dashboard")

    def dashboard(self, id):
        """
        Return a dashboard identified by its identifier.
        """
        return self._get(self.klasses["dashboard"], f"dashboard:{id}")

    def update_or_create_default_index_pattern(self, index_pattern):
        """
        If config document does not have index pattern, associate the
        first index pattern found.
        """
        config = self.config()
        if not config.config.to_dict().get("defaultIndex"):
            config.config.defaultIndex = index_pattern.meta.id.split(":")[-1]
            config.save(refresh="wait_for")
