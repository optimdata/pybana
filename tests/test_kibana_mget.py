# -*- coding: utf-8 -*-
"""Unit tests for Kibana saved-object loading, including Elasticsearch mget."""

import os
import sys
from parameterized import parameterized

BASE_DIRECTORY = os.path.join(os.path.dirname(__file__), "..")  # NOQA
sys.path.insert(0, BASE_DIRECTORY)  # NOQA

import unittest  # noqa: E402

import pytest  # noqa: E402
from elasticsearch import NotFoundError  # noqa: E402
from elasticsearch_dsl import Search  # noqa: E402

from pybana.client import Kibana  # noqa: E402
from pybana.elastic.mock_elastic_client import (  # noqa: E402
    MockElasticsearchPolluxClient,
)
from pybana.models import (  # noqa: E402
    Dashboard,
    DataView,
    IndexPattern,
    Search as SavedSearch,
    Visualization,
)


def _clients(start_cache: bool):
    elastic = MockElasticsearchPolluxClient()
    elastic.memory.add_index(".kibana")
    return Kibana(using=elastic, index=".kibana", start_cache=start_cache), elastic


def _index(elastic, doc_id, doc_type, body):
    elastic.index(".kibana", "doc", {"type": doc_type, doc_type: body}, id=doc_id)


class _Scan(object):
    def __init__(self, docs):
        self._docs = docs

    def scan(self):
        return iter(self._docs)


class TestKibanaMget(unittest.TestCase):
    def _seed(self, start_cache: bool):
        kibana, elastic = _clients(start_cache=start_cache)
        _index(elastic, "visualization:a", "visualization", {"title": "A"})
        _index(elastic, "visualization:b", "visualization", {"title": "B"})
        _index(elastic, "dashboard:d", "dashboard", {"title": "D"})
        _index(elastic, "index-pattern:p", "index-pattern", {"title": "logs-*"})
        _index(elastic, "search:s", "search", {"title": "S"})
        _index(elastic, "data-view:v", "data-view", {"title": "dv"})
        return kibana, elastic

    @parameterized.expand([False, True])
    def test_all_documents_scans_when_ids_are_omitted(self, start_cache):
        kibana, _ = self._seed(start_cache)
        visualizations = kibana.all_visualizations()
        self.assertEqual(
            [item.meta.id for item in visualizations],
            ["visualization:a", "visualization:b"],
        )
        self.assertIsInstance(visualizations[0], Visualization)
        self.assertEqual(
            [item.meta.id for item in kibana.all_dashboards()], ["dashboard:d"]
        )
        self.assertEqual(
            [item.meta.id for item in kibana.all_index_patterns()], ["index-pattern:p"]
        )
        self.assertEqual(
            [item.meta.id for item in kibana.all_documents(SavedSearch)], ["search:s"]
        )
        self.assertEqual(
            [item.meta.id for item in kibana.all_documents(DataView)], ["data-view:v"]
        )
        self.assertEqual(kibana.all_documents(Dashboard), kibana.all_dashboards())
        self.assertIsInstance(kibana.visualizations(), Search)
        self.assertIsInstance(kibana.dashboards(), Search)
        self.assertIsInstance(kibana.index_patterns(), Search)
        self.assertIsInstance(kibana.searches(), Search)
        self.assertIsInstance(kibana.objects("missing-type"), Search)

        kibana.objects = lambda type, using=None: _Scan([object()])
        kibana.cache_clear()
        with pytest.raises(AssertionError):
            kibana.all_documents(Visualization)
        kibana.cache_clear()
        with pytest.raises(AttributeError):
            kibana.all_documents(None)

    @parameterized.expand([False, True])
    def test_mget_loads_prefixed_and_bare_ids_and_skips_misses(self, start_cache):
        kibana, elastic = self._seed(start_cache)
        calls = []
        original = elastic.mget

        def mget(body, index=None, doc_type=None, **kwargs):
            calls.append(list(body["ids"]))
            return original(body, index=index, doc_type=doc_type, **kwargs)

        elastic.mget = mget
        loaded = kibana.all_visualizations(ids=["a", "b", "missing"])
        self.assertEqual(
            [item.meta.id for item in loaded], ["visualization:a", "visualization:b"]
        )
        self.assertEqual(loaded[0].meta.doc_type, "doc")
        self.assertEqual(
            calls, [["visualization:a", "visualization:b", "visualization:missing"]]
        )
        self.assertEqual(kibana.all_visualizations(ids=[]), [])
        self.assertEqual(len(calls), 1)

        dashboards = kibana.all_dashboards(ids=["dashboard:d"])
        self.assertEqual([item.meta.id for item in dashboards], ["dashboard:d"])
        self.assertIsInstance(dashboards[0], Dashboard)
        patterns = kibana.all_index_patterns(ids=["p"])
        self.assertEqual([item.meta.id for item in patterns], ["index-pattern:p"])
        self.assertIsInstance(patterns[0], IndexPattern)
        self.assertEqual(kibana.visualization("a").meta.id, "visualization:a")
        self.assertEqual(kibana.dashboard("d").meta.id, "dashboard:d")
        self.assertEqual(kibana.search("s").meta.id, "search:s")
        self.assertEqual(kibana.index_pattern("p").meta.id, "index-pattern:p")
        with pytest.raises(NotFoundError):
            kibana.visualization("missing")

    @parameterized.expand([False, True])
    def test_mget_repairs_missing_type_and_ignores_empty_responses(self, start_cache):
        kibana, elastic = self._seed(start_cache)

        def mget(body, index=None, doc_type=None, **kwargs):
            if body["ids"] == ["visualization:plain"]:
                return {
                    "docs": [
                        {"found": False, "_id": "visualization:gone"},
                        {
                            "found": True,
                            "_index": ".kibana",
                            "_id": "visualization:plain",
                            "_source": {
                                "type": "visualization",
                                "visualization": {"title": "plain"},
                            },
                        },
                    ]
                }
            return {}

        elastic.mget = mget
        self.assertEqual(kibana.all_visualizations(ids=["none"]), [])
        loaded = kibana.all_visualizations(ids=["plain"])
        self.assertEqual([item.meta.id for item in loaded], ["visualization:plain"])
        self.assertEqual(loaded[0].meta.doc_type, "doc")
        self.assertEqual(loaded[0].visualization["title"], "plain")

    @parameterized.expand([False, True])
    def test_init_config_and_default_index_pattern(self, start_cache):
        kibana, elastic = _clients(start_cache)
        kibana.init_config()
        kibana.init_config()
        self.assertIsNone(kibana.config().config.to_dict().get("defaultIndex"))
        pattern = IndexPattern.from_es(
            {
                "_index": ".kibana",
                "_id": "index-pattern:pattern-1",
                "_type": "doc",
                "_source": {
                    "type": "index-pattern",
                    "index-pattern": {"title": "logs-*"},
                },
            }
        )
        kibana.update_or_create_default_index_pattern(pattern)
        self.assertEqual(kibana.config().config.to_dict()["defaultIndex"], "pattern-1")
        kibana.update_or_create_default_index_pattern(pattern)
        current = kibana.config()
        current.config.defaultIndex = None
        current.save(refresh="wait_for", using=elastic)
        kibana.update_or_create_default_index_pattern(pattern, using=elastic)
        self.assertEqual(kibana.config().config.to_dict()["defaultIndex"], "pattern-1")
