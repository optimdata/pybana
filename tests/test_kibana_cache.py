# -*- coding: utf-8 -*-
"""Unit tests for the Kibana saved-object cache."""

import os
import sys

BASE_DIRECTORY = os.path.join(os.path.dirname(__file__), "..")  # NOQA
sys.path.insert(0, BASE_DIRECTORY)  # NOQA

import unittest  # noqa: E402

import pytest  # noqa: E402
from elasticsearch import NotFoundError  # noqa: E402

from pybana.client import Kibana, _DocumentsCache  # noqa: E402
from pybana.elastic.mock_elastic_client import (  # noqa: E402
    MockElasticsearchPolluxClient,
)
from pybana.models import Visualization  # noqa: E402


def _clients(start_cache=False):
    elastic = MockElasticsearchPolluxClient()
    elastic.memory.add_index(".kibana")
    return (
        Kibana(using=elastic, index=".kibana", start_cache=start_cache),
        elastic,
    )


def _visualization(doc_id="visualization:a", title="A"):
    return Visualization.from_es(
        {
            "_index": ".kibana",
            "_id": doc_id,
            "_type": "doc",
            "_source": {
                "type": "visualization",
                "visualization": {"title": title},
            },
        }
    )


def _bare(doc_id):
    meta = type("Meta", (), {"id": doc_id})()
    return type("Doc", (), {"meta": meta})()


class _Scan(object):
    def __init__(self, docs):
        self._docs = docs

    def scan(self):
        return iter(self._docs)


class TestDocumentsCache(unittest.TestCase):
    def test_get_set_and_subcache_reuse(self):
        cache = _DocumentsCache()
        vis = _visualization()
        self.assertEqual(cache.get(Visualization, "a"), (False, None))
        self.assertEqual(cache.get_multi(Visualization, ["a", "b"]), ([], ["a", "b"]))
        self.assertIsNone(cache.get_all(Visualization))
        self.assertIs(cache.set(Visualization, "a", vis), vis)
        self.assertIs(cache.set(Visualization, "empty", None), None)
        self.assertIs(
            cache._write_subcache(Visualization), cache._cache["visualization"]
        )
        self.assertEqual(cache.get(Visualization, "a"), (True, vis))
        self.assertEqual(cache.get(Visualization, "missing"), (False, None))
        self.assertEqual(cache.get(Visualization, "empty"), (True, None))
        documents, missing = cache.get_multi(Visualization, ["a", "empty", "missing"])
        self.assertEqual(documents, [vis])
        self.assertEqual(missing, ["missing"])
        self.assertEqual(cache.get_all(Visualization), [vis])

    def test_set_multi_checks_ids_and_records_missing_wished_ids(self):
        cache = _DocumentsCache()
        for doc_id, wished in (
            ("novalue", None),
            ("a:b:c", None),
            ("search:a", None),
            ("visualization:", None),
            ("visualization:.*", None),
            ("visualization:a", {"b"}),
        ):
            with pytest.raises(AssertionError):
                cache.set_multi(Visualization, [_bare(doc_id)], wished_ids=wished)

        stored = _bare("visualization:a")
        ids = cache.set_multi(Visualization, [stored], wished_ids={"a", "left-out"})
        self.assertEqual(ids[0], "a")
        self.assertEqual(set(ids), {"a", "left-out"})
        self.assertIs(cache.get(Visualization, "a")[1], stored)
        self.assertEqual(cache.get(Visualization, "left-out"), (True, None))

    def test_set_all_replaces_the_type_and_marks_it_complete(self):
        cache = _DocumentsCache()
        cache.delete_all(Visualization)
        first = _visualization("visualization:a", "A")
        second = _visualization("visualization:b", "B")
        cache.set(Visualization, "stale", first)
        returned = cache.set_all(Visualization, [first, second])
        self.assertEqual(returned, [first, second])
        self.assertEqual(cache.get(Visualization, "stale"), (False, None))
        self.assertEqual(cache.get_all(Visualization), [first, second])
        self.assertEqual(cache.get(Visualization, ".*"), (True, None))
        cache.clear()
        self.assertEqual(cache._cache, {})
        self.assertIsNone(cache.get_all(Visualization))


class TestKibanaCache(unittest.TestCase):
    def test_disabled_cache_is_a_passthrough(self):
        kibana, _ = _clients(start_cache=False)
        vis = _visualization()
        self.assertIsNone(kibana._cache)
        self.assertEqual(kibana._cache_get(Visualization, "a"), (False, None))
        self.assertEqual(kibana._cache_get_multi(Visualization, ["a"]), ([], ["a"]))
        self.assertIsNone(kibana._cache_get_all(Visualization))
        self.assertIs(kibana._cache_set(Visualization, "a", vis), vis)
        self.assertEqual(kibana._cache_set_all(Visualization, [vis]), [vis])
        self.assertEqual(kibana._cache_set_multi(Visualization, [vis]), [])
        kibana._cache_delete_all(Visualization)

    def test_listing_is_cached_and_ids_skip_cached_documents(self):
        kibana, elastic = _clients(start_cache=True)
        elastic.index(
            ".kibana",
            "doc",
            {"type": "visualization", "visualization": {"title": "A"}},
            id="visualization:a",
        )
        elastic.index(
            ".kibana",
            "doc",
            {"type": "visualization", "visualization": {"title": "B"}},
            id="visualization:b",
        )
        scans = []
        original_objects = kibana.objects

        def objects(type, using=None):
            scans.append(type)
            return original_objects(type, using=using)

        kibana.objects = objects
        first = kibana.all_visualizations()
        self.assertEqual(
            [item.meta.id for item in first], ["visualization:a", "visualization:b"]
        )
        self.assertEqual(scans, ["visualization"])
        second = kibana.all_visualizations()
        self.assertEqual(
            [item.meta.id for item in second], ["visualization:a", "visualization:b"]
        )
        self.assertEqual(scans, ["visualization"])

        calls = []
        original_mget = elastic.mget

        def mget(body, index=None, doc_type=None, **kwargs):
            calls.append(list(body["ids"]))
            return original_mget(body, index=index, doc_type=doc_type, **kwargs)

        elastic.mget = mget
        self.assertEqual(kibana.all_visualizations(ids=[]), [])
        cached = kibana.all_visualizations(ids=["a", "b"])
        self.assertEqual(
            [item.meta.id for item in cached], ["visualization:a", "visualization:b"]
        )
        self.assertEqual(calls, [])
        mixed = kibana.all_visualizations(ids=["a", "missing"])
        self.assertEqual([item.meta.id for item in mixed], ["visualization:a"])
        self.assertEqual(calls, [["visualization:missing"]])

        kibana.objects = lambda type, using=None: _Scan([None, object()])
        kibana._cache_delete_all(Visualization)
        with pytest.raises(AssertionError):
            kibana.all_visualizations()
        kibana.objects = lambda type, using=None: _Scan([None])
        with pytest.raises(AttributeError):
            kibana.all_visualizations()

    def test_mget_skips_empty_documents_and_repairs_missing_type(self):
        kibana, elastic = _clients(start_cache=True)
        original = Visualization.from_es

        def from_es(doc):
            if doc.get("_id") == "visualization:skip":
                return None
            return original(doc)

        def mget(body, index=None, doc_type=None, **kwargs):
            return {
                "docs": [
                    {"found": False, "_id": "visualization:gone"},
                    {
                        "found": True,
                        "_index": ".kibana",
                        "_id": "visualization:skip",
                        "_source": {
                            "type": "visualization",
                            "visualization": {"title": "skip"},
                        },
                    },
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

        elastic.mget = mget
        Visualization.from_es = staticmethod(from_es)
        try:
            loaded = kibana.all_visualizations(ids=["skip", "plain"])
        finally:
            Visualization.from_es = original
        self.assertEqual([item.meta.id for item in loaded], ["visualization:plain"])
        self.assertEqual(loaded[0].meta.doc_type, "doc")
        self.assertEqual(kibana._cache.get(Visualization, ""), (True, loaded[0]))

    def test_get_caches_hits_and_missing_documents(self):
        kibana, elastic = _clients(start_cache=True)
        elastic.index(
            ".kibana",
            "doc",
            {"type": "visualization", "visualization": {"title": "A"}},
            id="visualization:a",
        )
        first = kibana.visualization("a")
        self.assertIs(kibana.visualization("a"), first)
        self.assertEqual(kibana._cache.get(Visualization, "a"), (True, first))
        with pytest.raises(NotFoundError):
            kibana.visualization("missing")
        self.assertIsNone(kibana.visualization("missing", raise_on_missing=False))
        self.assertEqual(kibana._cache.get(Visualization, "missing"), (True, None))

    def test_cache_lifecycle(self):
        kibana, _ = _clients(start_cache=False)
        self.assertFalse(kibana.cache_active)
        kibana.cache_clear()
        self.assertFalse(kibana.cache_active)
        kibana.cache_initialize()
        self.assertTrue(kibana.cache_active)
        self.assertEqual(kibana._cache._cache, {})

        vis = _visualization()
        kibana._cache.set(Visualization, "a", vis)
        existing = kibana._cache
        kibana.cache_initialize()
        self.assertIs(kibana._cache, existing)
        self.assertEqual(kibana._cache.get(Visualization, "a"), (True, vis))

        kibana.cache_clear()
        self.assertIs(kibana._cache, existing)
        self.assertEqual(kibana._cache._cache, {})
        self.assertTrue(kibana.cache_active)

        kibana.cache_stop()
        self.assertFalse(kibana.cache_active)
        self.assertIsNone(kibana._cache)
        kibana.cache_initialize()
        self.assertTrue(kibana.cache_active)
        self.assertEqual(kibana._cache._cache, {})
