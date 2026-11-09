import os
import sys

BASE_DIRECTORY = os.path.join(os.path.dirname(__file__), "..")  # NOQA
sys.path.insert(0, BASE_DIRECTORY)  # NOQA

import copy  # noqa: E402
import datetime  # noqa: E402
import json  # noqa: E402
import unittest  # noqa: E402
from unittest.mock import patch  # noqa: E402
from elasticsearch.exceptions import TransportError  # noqa: E402
from elasticsearch.helpers import scan  # noqa: E402
from elasticsearch.exceptions import ConflictError  # noqa: E402
from parameterized import parameterized  # noqa: E402
from pybana.elastic import elastic_client as elastic_client_module  # noqa: E402
from pybana.elastic.elastic_client import (  # noqa: E402
    ElasticsearchExt,
    ScrollsCache,
    _get_scroll_id,
    _get_scroll_ids,
)  # noqa: E402

from tests.connection_pool import get_allowed_versions, ELASTIC_CLIENTS  # noqa: E402

# from ..client.base import JSONSerializer
from pybana.elastic.fixes_for_v8 import (  # noqa: E402
    fusion_mappings,
    v6_to_v8,
    v8_to_v6,
)  # noqa: E402
import pytest  # noqa: E402


class TestV8ToV6Case(unittest.TestCase):
    def setUp(self):
        super().setUp()
        pass

    def tearDown(self) -> None:
        super().tearDown()
        pass

    def test_correct_mappings(self):
        for origin, expected, description in [
            ({}, {}, "empty"),
            ({"a": {}}, {"a": {}}, "empty sub mapping"),
            ({"a": "b"}, {"a": "b"}, "a:b"),
            ({"a": "b"}, {"a": "b"}, "a:b"),
            (
                {"a": {"dynamic_templates": {}, "properties": {}}},
                {"a": {"dynamic_templates": {}, "properties": {}}},
                "bad depth",
            ),
            (
                {"a": {"mappings": {"dynamic_templatesx": {}, "propertiesx": {}}}},
                {"a": {"mappings": {"dynamic_templatesx": {}, "propertiesx": {}}}},
                "test bad params",
            ),
            (
                {"a": {"mappings": {"dynamic_templates": {}, "properties": {}}}},
                {
                    "a": {
                        "mappings": {
                            "_default_": {"dynamic_templates": {}},
                            "x": {"properties": {}},
                        }
                    }
                },
                "test ext",
            ),
        ]:
            found = v8_to_v6.correct_mappings(origin, doc_type="x")
            print(f"{description}: {found}")
            assert found == expected, description

    def test_correct_search_result(self):
        assert v8_to_v6.correct_search_result({}, doc_type="x") == {}
        assert v8_to_v6.correct_search_result(
            {"hits": {"total": 13}}, doc_type="x"
        ) == {"hits": {"total": 13}}
        assert v8_to_v6.correct_search_result(
            {"hits": {"total": {"titi": "toto"}}}, doc_type="x"
        ) == {"hits": {"total": {"titi": "toto"}}}
        assert v8_to_v6.correct_search_result(
            {"hits": {"total": {"value": 15, "titi": "toto"}}}, doc_type="x"
        ) == {"hits": {"total": 15, "total_v8": {"value": 15, "titi": "toto"}}}
        assert v8_to_v6.correct_search_result({"took": 1}, doc_type="x") == {"took": 1}
        assert v8_to_v6.correct_search_result({"hits": []}, doc_type="x") == {
            "hits": []
        }


class TestV6ToV8Case(unittest.TestCase):
    def setUp(self):
        super().setUp()
        pass

    def tearDown(self) -> None:
        super().tearDown()
        pass

    def test_fix_actions(self):
        assert list(v6_to_v8.fix_actions(None)) == []
        assert list(
            v6_to_v8.fix_actions([{"_type": "toto", "val": "A"}, {"val": "B"}])
        ) == [{"val": "A"}, {"val": "B"}]
        assert list(
            v6_to_v8.fix_actions(
                [{"meta": [{"_type": "doc", "id": "1"}], "_type": "bulk"}]
            )
        ) == [{"meta": [{"id": "1"}]}]

    def test_fusion_mappings_v8(self):
        with pytest.raises(RuntimeError):
            fusion_mappings({"span": "item"}, "span")
        for test_name, origin, key_to_merge, expected in [
            (
                "simple_test",
                json.loads(
                    '{"span":{"properties":[{"key": "machine_id","data": "x"}]}}'
                ),
                "span",
                '{"properties": [{"data": "x", "key": "machine_id"}]}',
            ),
            (
                "[list-key] not copying _default_ existing value",
                json.loads(
                    '{"_default_":{"properties":[{"key": "machine_id","data": "new"}]},"properties":[{"key": "machine_id","data": "old"}]}'
                ),
                "_default_",
                '{"properties": [{"data": "old", "key": "machine_id"}]}',
            ),
            (
                "[list-key] replacing span value",
                json.loads(
                    '{"span":{"properties":[{"key": "machine_id","data": "new"}]},"properties":[{"key": "machine_id","data": "old"}]}'
                ),
                "span",
                '{"properties": [{"data": "new", "key": "machine_id"}]}',
            ),
            (
                "[list-key] fusioning different keys ",
                json.loads(
                    '{"_default_":{"properties":[{"key": "machine_id","data": "new"}, {"key":"a"}]},"properties":[{"key": "machine_id","data": "old"}, {"key":"b"}]}'
                ),
                "_default_",
                '{"properties": [{"data": "old", "key": "machine_id"}, {"key": "b"}, {"key": "a"}]}',
            ),
            (
                "[dict] not copying _default_ existing value",
                json.loads(
                    '{"_default_":{"properties":{"machine_id":"new"}},"properties":{"machine_id":"old"}}'
                ),
                "_default_",
                '{"properties": {"machine_id": "old"}}',
            ),
            (
                "[dict] replacing span value",
                json.loads(
                    '{"span":{"properties":{"machine_id":"new"}},"properties":{"machine_id":"old"}}'
                ),
                "span",
                '{"properties": {"machine_id": "new"}}',
            ),
            (
                "[dict] fusioning different keys",
                json.loads(
                    '{"_default_":{"properties":{"machine_id": "new", "a":"A"}},"properties":{"machine_id": "old", "b": "B"}}'
                ),
                "_default_",
                '{"properties": {"a": "A", "b": "B", "machine_id": "old"}}',
            ),
            (
                "[list] keep origin",
                json.loads('{"_default_":{"properties":[11,12]},"properties":[1,2]}'),
                "_default_",
                '{"properties": [1, 2]}',
            ),
            (
                "[list] use new and invalid span key",
                json.loads(
                    '{"span":{"properties":[11,12],"x":[21,22]},"properties":[1,2]}'
                ),
                "span",  # test standard new list with override
                '{"properties": [11, 12]}',
            ),
            (
                "Missing key",
                json.loads('{"span":{"properties":[11,12]},"properties":[1,2]}'),
                "spanx",  # test missing key
                '{"properties": [1, 2], "span": {"properties": [11, 12]}}',
            ),
            (
                "[str] keep origin",
                json.loads('{"_default_":{"properties":"new"},"properties":"old"}'),
                "_default_",
                '{"properties": "old"}',
            ),
            (
                "[str] replace with new",
                json.loads('{"span":{"properties":"new"},"properties":"old"}'),
                "span",
                '{"properties": "new"}',
            ),
        ]:
            fusion_mappings(origin, key_to_merge, key_to_merge != "_default_")
            assert (
                json.dumps(origin, sort_keys=True) == expected
            ), f"FAILED: {test_name}"

    def test_fix_dynamic_template(self):
        for origin, expected, title in [
            (None, None, "none"),
            ({"a": 1}, {"a": 1}, "case 0"),
            ({"a": {}}, {"a": {}}, "case 1"),
            (
                {"a": {"match_mapping_type": "string", "mapping": {}}},
                {"a": {"match_mapping_type": "string", "mapping": {}}},
                "empty mapping",
            ),
            (
                {"a": {"match_mapping_type": "string", "mapping": 1}},
                {"a": {"match_mapping_type": "string", "mapping": 1}},
                "bad mapping type",
            ),
            (
                {
                    "a": {
                        "match_mapping_type": "string",
                        "mapping": {
                            "index": "not_analyzed",
                            "type": "string",
                            "omit_norms": True,
                        },
                    }
                },
                {
                    "a": {
                        "match_mapping_type": "string",
                        "mapping": {"index": False, "type": "keyword", "norms": False},
                    }
                },
                "not v8 mapping",
            ),
            (
                {
                    "a": {
                        "match_mapping_type": "string",
                        "mapping": {
                            "ignore_above": 256,
                            "index": True,
                            "type": "keyword",
                        },
                    }
                },
                {
                    "a": {
                        "match_mapping_type": "string",
                        "mapping": {
                            "ignore_above": 256,
                            "index": True,
                            "type": "keyword",
                        },
                    }
                },
                "v8 mapping",
            ),
        ]:
            fixed = copy.deepcopy(origin) if origin else origin
            v6_to_v8.fix_dynamic_template(fixed)
            assert fixed == expected, title

    def test_fix_dynamic_templates(self):
        for origin, expected, title in [
            (None, None, "none"),
            ([{"a": {}}], [{"a": {}}], "case 1"),
            ({"a": {}}, {"a": {}}, "case 2"),
            (
                [
                    {
                        "a": {
                            "match_mapping_type": "string",
                            "mapping": {
                                "index": "not_analyzed",
                                "type": "string",
                                "omit_norms": True,
                            },
                        }
                    }
                ],
                [
                    {
                        "a": {
                            "match_mapping_type": "string",
                            "mapping": {
                                "index": False,
                                "type": "keyword",
                                "norms": False,
                            },
                        }
                    }
                ],
                "changed",
            ),
        ]:
            fixed = copy.deepcopy(origin) if origin else origin
            v6_to_v8.fix_dynamic_templates(fixed)
            assert fixed == expected, title

    def test_fix_mappings(self):
        for origin, expected, title in [
            (None, {}, "none"),
            (
                {"docx": {"properties": {"a": "b"}}},
                {"properties": {"a": "b"}},
                "keys not in ALLOWED_MAPPINGS_KEYS",
            ),
            (
                {"properties": {"a": "b"}},
                {"properties": {"a": "b"}},
                "keys in ALLOWED_MAPPINGS_KEYS",
            ),
        ]:
            assert v6_to_v8.fix_mappings(origin) == expected, title

    def test_fix_histogram(self):
        fixed_origin = {"date_histogram": {"interval": "10m"}}
        fixed_dest = {"date_histogram": {"fixed_interval": "10m"}}
        calendar_origin = {"date_histogram": {"interval": "1m"}}
        calendar_dest = {"date_histogram": {"calendar_interval": "1m"}}
        invalid_origin = {"date_histogram": {"interval": 1}}
        invalid_dest = {"date_histogram": {"fixed_interval": 1}}
        for origin, expected, title in [
            (None, None, "none"),
            (1, 1, "invalid format"),
            (invalid_origin, invalid_dest, "invalid_origin 1"),
            (fixed_dest, fixed_dest, "unchanged case 1"),
            (fixed_origin, fixed_dest, "fixed interval"),
            (calendar_origin, calendar_dest, "calendar interval"),
            ([[fixed_origin]], [[fixed_dest]], "list in list"),
            ([{"a": fixed_origin}], [{"a": fixed_dest}], "dict in list"),
            ([1, calendar_origin], [1, calendar_dest], "scalar in list"),
        ]:
            found = copy.deepcopy(origin)
            v6_to_v8.fix_histogram(found)
            self.assertEqual(found, expected, title)

    def test_fix_search_body(self):
        for origin, expected, title in [
            (None, None, "none"),
            ({}, {}, "empty"),
            (1, 1, "invalid format"),
            ('{"another":"toto"}', '{"another":"toto"}', "unchanged"),
            ('{"doc_type":"a","another":"toto"}', '{"another": "toto"}', "doctype 1"),
            ({"doc_type": "a", "another": "toto"}, {"another": "toto"}, "doctype 2"),
            ("{", "{", "invalid json"),
        ]:
            found = v6_to_v8.fix_search_body(origin)
            assert found == expected, title

    def test_fix_search_params(self):
        for origin, expected, title in [
            ({}, {}, "empty"),
            (
                {"doc_type": "a", "another": "toto"},
                {"doc_type": "a", "another": "toto"},
                "no change 1",
            ),
            (
                {"_source_exclude": "toto"},
                {"_source_excludes": "toto"},
                "source_exclude",
            ),
            (
                {"_source_include": "toto"},
                {"_source_includes": "toto"},
                "source_include",
            ),
            (
                {"_source_include": "toto", "_source_includes": "toto2"},
                {"_source_include": "toto", "_source_includes": "toto2"},
                "source_include no change",
            ),
        ]:
            found = v6_to_v8.fix_search_params(origin)
            assert found == expected, title

    def test_fix_transport_error_args(self):
        for origin, expected, title in [
            ((), (), "empty"),
            (("a", "b"), ("a", "b"), "2 items"),
            (
                ("a", "b", {"error": {"root_cause": [{"reason": "this is an error"}]}}),
                ("a", "b", {"error": {"root_cause": [{"reason": "this is an error"}]}}),
                "unchanged",
            ),
            (
                ("a", "b", {"errorx": "my_error"}),
                (
                    "a",
                    "b",
                    {"error": {"root_cause": [{"reason": {"errorx": "my_error"}}]}},
                ),
                "strange dict",
            ),
            (
                ("a", "b", {"error": "my_error"}),
                ("a", "b", {"error": {"root_cause": [{"reason": "my_error"}]}}),
                "standard dict",
            ),
            (
                ("a", "b", "my_error"),
                ("a", "b", {"error": {"root_cause": [{"reason": "my_error"}]}}),
                "string in tuple",
            ),
            (
                ("a", "b", {"error": {"root_cause": [{"type": "parse"}]}}),
                (
                    "a",
                    "b",
                    {
                        "error": {
                            "root_cause": [
                                {"reason": {"root_cause": [{"type": "parse"}]}}
                            ]
                        }
                    },
                ),
                "root cause without reason",
            ),
            (
                ("a", "b", {"error": {"root_cause": []}}),
                (
                    "a",
                    "b",
                    {"error": {"root_cause": [{"reason": {"root_cause": []}}]}},
                ),
                "empty root cause",
            ),
            (
                ("a", "b", {"error": {"root_cause": "nope"}}),
                (
                    "a",
                    "b",
                    {"error": {"root_cause": [{"reason": {"root_cause": "nope"}}]}},
                ),
                "root cause not a list",
            ),
            (
                ("a", "b", {"error": {"root_cause": ["text"]}}),
                (
                    "a",
                    "b",
                    {"error": {"root_cause": [{"reason": {"root_cause": ["text"]}}]}},
                ),
                "root cause item not a dict",
            ),
        ]:
            found = v6_to_v8.fix_transport_error_args(origin)
            assert found == expected, title

    def test_fix_template(self):
        for origin, expected, title in [
            (None, {}, "none"),
            (
                {"a": {}},
                {"a": {}, "settings": {"index": {"codec": "best_compression"}}},
                "empty",
            ),
            (
                {"mappings": {}},
                {"mappings": {}, "settings": {"index": {"codec": "best_compression"}}},
                "mappings",
            ),
        ]:
            found = copy.deepcopy(origin)
            found = v6_to_v8.fix_template(found)
            self.assertEqual(found, expected, title)

    def test_fix_transport_instance(self):
        class _Transport(object):
            def __init__(self):
                self.error = None

            def perform_request(
                self, method, url, headers=None, params=None, body=None
            ):
                if self.error:
                    raise self.error
                return {"method": method, "url": url}

        transport = _Transport()
        v6_to_v8.fix_transport_instance(transport)
        wrapped = transport.perform_request
        v6_to_v8.fix_transport_instance(transport)
        assert transport.perform_request is wrapped
        assert transport.perform_request("GET", "/ok") == {
            "method": "GET",
            "url": "/ok",
        }

        transport.error = TransportError(404, "missing")
        with pytest.raises(TransportError) as caught:
            transport.perform_request("GET", "/missing")
        assert caught.value.args == (404, "missing")


class TestScrollsCache(unittest.TestCase):
    def test_scroll_cache(self):
        sc = ScrollsCache()
        self.assertEqual(len(sc), 0)
        self.assertTrue(sc.add_item(results={"_scroll_id": "a"}, doc_type="toto"))
        self.assertEqual(len(sc), 1)
        self.assertFalse(sc.add_item(results={"_scroll_id": "b"}, doc_type=None))
        self.assertFalse(sc.add_item(results={"_scroll_idx": "c"}, doc_type="toto"))
        self.assertEqual(sc.nb_scrolls_added, 1)
        self.assertTrue(sc.add_item(results={"_scroll_id": "d"}, doc_type="toto"))
        self.assertEqual(sc.nb_scrolls_added, 2)
        self.assertEqual(len(sc), 2)
        # Test on auto clear
        self.assertEqual(sc.clear_scroll_cache(), 0)
        self.assertEqual(sc.nb_scrolls_added, 2)
        self.assertIn("a", sc.cache)
        sc.cache["a"].date = datetime.datetime.now() - datetime.timedelta(hours=2)
        self.assertEqual(sc.clear_scroll_cache(), 1)
        self.assertEqual(sc.nb_scrolls_added, 2)
        self.assertEqual(len(sc), 1)
        self.assertNotIn("a", sc.cache)
        self.assertEqual(sc.fix_results({}), {})
        untouched = {"_scroll_id": "unknown", "hits": {"total": 0, "hits": []}}
        self.assertEqual(sc.fix_results(untouched), untouched)
        sc.add_item(
            results={
                "_scroll_id": "cached",
                "hits": {"total": {"value": 1, "relation": "eq"}, "hits": []},
            },
            doc_type="toto",
        )
        fixed = sc.fix_results(
            {
                "_scroll_id": "cached",
                "hits": {
                    "total": {"value": 1, "relation": "eq"},
                    "hits": [{"_id": "1"}],
                },
            }
        )
        self.assertEqual(fixed["hits"]["total"], 1)
        self.assertEqual(fixed["hits"]["hits"][0]["_type"], "toto")
        sc.remove("cached")
        self.assertNotIn("cached", sc.cache)
        sc.remove("missing")

    def test_scroll_id(self):
        self.assertEqual(_get_scroll_id("", {}), "")
        self.assertEqual(_get_scroll_id("", {"scroll_id": "a", "_scroll_id": "b"}), "b")
        self.assertEqual(_get_scroll_id("", {"scroll_id": "a"}), "a")
        self.assertEqual(_get_scroll_id("c", {"_scroll_id": "b"}), "c")
        self.assertEqual(_get_scroll_id(["e", "f"], {}), "e")
        self.assertEqual(_get_scroll_ids(1, None), [])
        self.assertEqual(_get_scroll_ids(None, "x"), [])
        self.assertEqual(_get_scroll_ids(None, {"scroll_id": 3}), [])
        self.assertEqual(_get_scroll_ids(None, {"_scroll_id": ["a", "b"]}), ["a", "b"])


class TestElaticsearchClientCase(unittest.TestCase):

    @parameterized.expand(get_allowed_versions(versions=["v6", "v8"]) or ["skip"])
    def test_scroll_non_dict_response(self, version):
        if version == "skip":
            self.skipTest("test function not working for mock")

        elastic = ElasticsearchExt(
            hosts=[
                "http://localhost:9200" if version == "v6" else "http://localhost:9201"
            ],
            timeout=10,
        )
        elastic.es.transport.perform_request = lambda *args, **kwargs: "raw"
        self.assertEqual(elastic.scroll(body={"scroll_id": None}), "raw")

    @parameterized.expand(get_allowed_versions())
    def test_scroll_ops_process(self, version):
        elastic = ELASTIC_CLIENTS[version]
        index = "test_scroll_ops"
        # step 1: create index
        elastic.indices.delete(f"{index}*")
        elastic.indices.create(index)
        # step 2: add 10 records
        nb_items = 10
        for i in range(nb_items):
            elastic.create(
                index, doc_type="toto", id=f"id_{i}", body={"key1": f"val{i}"}
            )
        elastic.indices.refresh(index)

        # step 3: search all records using scroll (with 2 records limit)....
        hits = {}
        for hit in scan(elastic, index=index, doc_type="toto", size=2):
            self.assertEqual(hit.get("_type"), "toto")
            self.assertEqual(list(hit["_source"].keys()), ["key1"])
            hits[hit["_id"]] = hit["_source"]["key1"]
        # step 4: check results
        assert len(hits) == nb_items
        for i in range(nb_items):
            self.assertIn(f"id_{i}", hits)
            self.assertEqual(hits[f"id_{i}"], f"val{i}")
        self.assertEqual(
            len([h for h in scan(elastic, index=index, doc_type=["toto"], size=2)]), 10
        )
        self.assertEqual(len(elastic.scroll_cache), 0)
        self.assertEqual(
            elastic.scroll_cache.nb_scrolls_added,
            2 if elastic.version_major >= 7 else 0,
        )
        # assert False

    @parameterized.expand(get_allowed_versions())
    def test_simple_es_operations(self, version: str):
        elastic = ELASTIC_CLIENTS[version]
        index = "test_simple_es_operations"
        elastic.indices.delete(f"{index}*")
        elastic.indices.create(f"{index}_1")

        with pytest.raises(ValueError):
            elastic.create(f"{index}_1", doc_type="toto", id="id_a", body=None)
        elastic.create(f"{index}_1", doc_type="toto", id="id_a", body={"key1": "val1"})
        with pytest.raises(ValueError):
            elastic.get(f"{index}_1", id="", doc_type="toto")
        content = elastic.get(f"{index}_1", id="id_a", doc_type="toto")
        print(f"content: {json.dumps(content)}")
        assert content == json.loads(
            '{"_index": "test_simple_es_operations_1", "_type": "toto", "_id": "id_a", "_version": 1, "_seq_no": 0, "_primary_term": 1, "found": true, "_source": {"key1": "val1"}}'
        )

        elastic.delete(f"{index}_1", id="id_a", doc_type="toto", version=1)

        assert elastic.indices.delete("") == 0
        if elastic.version_major >= 7:
            assert elastic.indices.delete(f"{index}_x*") == 0
        assert elastic.indices.delete_safe(f"{index}_x*") == 0
        assert elastic.indices.delete_safe(f"{index}_*") == 1

        try:
            elastic.get(f"{index}_2", id="id_a", doc_type="toto")
            assert False
        except TransportError as e:
            assert "index_not_found_exception" in str(e)

    @parameterized.expand(get_allowed_versions(versions=["mock", "v8"]))
    def test_extended_client_operations(self, version: str):
        elastic = ELASTIC_CLIENTS[version]
        index = "test_elastic_client_ext"
        dest = index + "_dest"
        alias = index + "_alias"
        template = "test_elastic_client_template"
        pipeline = "test_elastic_client_pipeline"
        try:
            self._cleanup_extended(elastic, index, dest, template, pipeline)
            elastic.indices.create(
                index + "_1", body={"settings": {"index": {"number_of_shards": 1}}}
            )
            self.assertEqual(elastic.indices.fix_mappings(None), {})
            mapped = elastic.indices.fix_mappings({"toto": {"properties": {"a": {}}}})
            if elastic.version_major >= 7:
                self.assertIn("properties", mapped)
            else:
                self.assertIn("toto", mapped)
            self.assertTrue(elastic.indices.exists(index + "_1"))
            self.assertIn(index + "_1", elastic.indices.get_settings(index + "_1"))
            self.assertTrue(
                elastic.indices.put_alias(index=index + "_1", name=alias)[
                    "acknowledged"
                ]
            )
            self.assertTrue(elastic.indices.exists_alias(name=alias))
            self.assertIn(index + "_1", elastic.indices.get_alias(name=alias))
            fetched = elastic.indices.get(index + "_1")
            self.assertIn(index + "_1", fetched)
            self.assertIn("aliases", fetched[index + "_1"])

            elastic.index(index + "_1", "toto", {"a": 1}, id="1")
            elastic.indices.refresh(index + "_1")
            got = elastic.get(index + "_1", "toto", "1")
            self.assertEqual(got["_source"], {"a": 1})
            self.assertEqual(got.get("_type"), "toto")
            self.assertEqual(elastic.get(index + "_1", "", "1")["_id"], "1")
            self.assertGreaterEqual(
                elastic.count(index=index + "_1", doc_type="toto")["count"], 1
            )
            docs = elastic.mget(
                body={"ids": ["1", "missing"]}, index=index + "_1", doc_type="toto"
            )
            self.assertTrue(docs["docs"][0]["found"])
            self.assertFalse(docs["docs"][1]["found"])

            found = elastic.search(
                index=index + "_1",
                doc_type="toto",
                body={"query": {"match_all": {}}},
            )
            self.assertGreaterEqual(found["hits"]["total"], 1)
            elastic.search(index=index + "_1", doc_type=["toto"], body={})
            if elastic.version_major >= 7:
                elastic.search(index=index + "_1", doc_type=["toto", "other"], body={})
            elastic.search(index=index + "_1", body={})

            string_bulk = (
                '{"index": {"_index": "%s", "_type": "toto", "_id": "2"}}\n'
                '{"a": 2}\n' % (index + "_1")
            )
            self.assertFalse(elastic.bulk(string_bulk)["errors"])
            self.assertFalse(
                elastic.bulk(
                    [{"index": {"_index": index + "_1", "_id": "3"}}, {"a": 3}],
                    doc_type="toto",
                )["errors"]
            )
            actions = [
                {
                    "_op_type": "index",
                    "_index": index + "_1",
                    "_type": "toto",
                    "_id": "4",
                    "_source": {"a": 4},
                }
            ]
            success, errors = elastic.helpers_bulk(actions)
            self.assertEqual(success, 1)
            self.assertEqual(errors, [])
            self.assertIn("_type", actions[0])

            updated = elastic.update_by_query(
                index + "_1",
                doc_type="toto",
                body={"query": {"match_all": {}}},
            )
            self.assertGreaterEqual(updated["total"], 1)
            copied = elastic.reindex(
                body={"source": {"index": index + "_1"}, "dest": {"index": dest}}
            )
            self.assertGreaterEqual(copied["total"], 1)

            self.assertTrue(elastic.cat.indices(index=index + "*"))
            self.assertTrue(elastic.cat.count(index=index + "_1"))
            self.assertIsNotNone(elastic.cat.templates())
            self.assertIsNotNone(elastic.cat.repositories())
            self.assertEqual(elastic.name, elastic.version)
            self.assertIsNotNone(elastic.info()["version"]["number"])
            self.assertIsNotNone(elastic.tasks.list())

            template_body = {
                "index_patterns": [index + "-tpl-*"],
                "mappings": {"properties": {"a": {"type": "keyword"}}},
            }
            self.assertTrue(
                elastic.indices.put_template(template, template_body)["acknowledged"]
            )
            self.assertTrue(elastic.indices.exists_template(template))
            stored = elastic.indices.get_template(template, doc_type="toto")
            self.assertIn(template, stored)
            self.assertIn(template, elastic.indices.get_template(template))
            mapping = elastic.indices.get_mapping(index=index + "_1", doc_type="toto")
            self.assertIn(index + "_1", mapping)
            self.assertIn(index + "_1", elastic.indices.get_mapping(index=index + "_1"))
            self.assertTrue(
                elastic.indices.put_mapping(
                    "toto",
                    {"properties": {"b": {"type": "keyword"}}},
                    index=index + "_1",
                )["acknowledged"]
            )
            with pytest.raises(ValueError):
                elastic.indices.put_mapping(
                    None, {"properties": {}}, index=index + "_1"
                )
            self.assertIsNotNone(elastic.indices.recovery(index + "_1"))
            self.assertIsNotNone(
                elastic.indices.recovery(index + "_1", active_only="false")
            )

            self.assertTrue(
                elastic.indices.update_aliases(
                    {
                        "actions": [
                            {"add": {"index": index + "_1", "alias": alias + "_2"}}
                        ]
                    }
                )["acknowledged"]
            )
            elastic.indices.delete_alias(index=index + "_1", name=alias + "_2")
            rollover_index = index + "-000001"
            elastic.indices.create(rollover_index)
            elastic.indices.put_alias(index=rollover_index, name=alias + "_roll")
            rolled = elastic.indices.rollover(alias + "_roll")
            self.assertTrue(rolled.get("rolled_over") or rolled.get("acknowledged"))

            self.assertTrue(
                elastic.ingest.put_pipeline(
                    pipeline, {"description": "cov", "processors": []}
                )["acknowledged"]
            )
            self.assertTrue(elastic.ingest.delete_pipeline(pipeline)["acknowledged"])

            if elastic.version_major >= 7:
                elastic.index(index + "_1", "toto", {"a": 1}, id="ver")
                conflicted = elastic.index(
                    index + "_1", "toto", {"a": 5}, id="ver", version=1
                )
                self.assertGreaterEqual(conflicted["_version"], 2)
                with pytest.raises(ConflictError):
                    elastic.index(
                        index + "_1",
                        "toto",
                        {"a": 6},
                        id="ver",
                        version="1",
                        version_type="external",
                    )
                elastic.delete(index + "_1", "toto", "2")
                elastic.index(index + "_1", "toto", {"a": 3}, id="delint")
                elastic.delete(index + "_1", "toto", "delint", version=1)
                elastic.index(index + "_1", "toto", {"a": 4}, id="delstr")
                with pytest.raises(ConflictError):
                    elastic.delete(index + "_1", "toto", "delstr", version="1")
                self.assertEqual(elastic.indices.delete(index + "_no_alias*"), 0)
            else:
                elastic.index(index + "_1", "toto", {"a": 5}, id="5")
                elastic.delete(index + "_1", "toto", "2")
                elastic.indices.delete(index + "_1")

            bare = index + "_bare"
            elastic.indices.create(bare)
            elastic.indices.put_alias(index=bare, name=bare + "_alias")
            self.assertGreaterEqual(elastic.indices.delete_safe(bare + "_alias"), 1)
            elastic.indices.create(bare)
            self.assertEqual(elastic.indices.delete_safe(bare), 1)
            self.assertEqual(elastic.indices.delete_safe(index + "_missing*"), 0)

            elastic.indices.create(index + "_oc")
            self.assertTrue(elastic.indices.close(index + "_oc"))
            self.assertTrue(elastic.indices.open(index + "_oc"))

            removed = elastic.delete_by_query(
                dest, doc_type="toto", body={"query": {"match_all": {}}}
            )
            self.assertGreaterEqual(removed["deleted"], 0)
            self.assertEqual(elastic.indices.delete(dest), 1)
            self.assertTrue(elastic.indices.delete_template(template)["acknowledged"])
        finally:
            self._cleanup_extended(elastic, index, dest, template, pipeline)

    @parameterized.expand(get_allowed_versions(versions=["v6"]) or ["skip"])
    def test_legacy_major_version_paths(self, version: str):
        if version == "skip":
            self.skipTest("test function not working for mock")
        elastic = ElasticsearchExt(
            ["http://localhost:9200" if version == "v6" else "http://localhost:9201"],
            timeout=10,
        )
        elastic._version_major = 6
        calls = {}

        def remember(name, result=None):
            def inner(*args, **kwargs):
                calls[name] = (args, kwargs)
                return {"acknowledged": True} if result is None else result

            return inner

        elastic.es.create = remember("create", {"result": "created"})
        elastic.es.get = remember("get", {"_id": "1", "_source": {"a": 1}})
        elastic.es.search = remember(
            "search", {"hits": {"total": {"value": 3}, "hits": []}}
        )
        elastic.es.delete = remember("delete", {"result": "deleted"})
        elastic.es.bulk = remember("bulk", {"errors": False})
        elastic.es.delete_by_query = remember("delete_by_query", {"deleted": 0})
        elastic.es.update_by_query = remember("update_by_query", {"total": 0})
        elastic.es.count = remember("count", {"count": 0})
        elastic.es.index = remember("index", {"_version": 3})
        elastic.es.mget = remember("mget", {"docs": []})
        elastic.indices._indices.delete = remember("indices_delete")
        elastic.indices._indices.put_template = remember("put_template")
        elastic.indices._indices.get_mapping = remember(
            "get_mapping", {"i": {"mappings": {}}}
        )
        elastic.indices._indices.get_template = remember("get_template", {"tpl": {}})
        elastic.indices._indices.exists_alias = lambda **kwargs: False
        elastic.indices._indices.exists = lambda index: False
        transport_calls = []

        def transport(method, url, headers=None, params=None, body=None):
            transport_calls.append((method, url, params, body))
            return {"acknowledged": True}

        elastic._transport_perform_request = transport

        original = {"toto": {"properties": {"a": {}}}}
        self.assertIs(elastic.indices.fix_mappings(original), original)
        self.assertEqual(elastic.indices.fix_mappings(None), {})
        self.assertEqual(elastic.indices.delete("plain"), 1)
        self.assertEqual(calls["indices_delete"][1]["index"], "plain")
        self.assertEqual(elastic.indices.delete("wild*"), 1)

        template_body = {"mappings": {"toto": {"properties": {"a": {}}}}}
        elastic.indices.put_template("tpl", template_body)
        self.assertIs(calls["put_template"][1]["body"], template_body)
        elastic.indices.get_mapping(index="i", doc_type="toto")
        self.assertEqual(calls["get_mapping"][1]["doc_type"], "toto")
        self.assertIn("tpl", elastic.indices.get_template("tpl", doc_type="toto"))

        elastic.indices.recovery("idx", active_only="true", bytes="b")
        self.assertNotIn("active_only", transport_calls[-1][2])
        self.assertIn("bytes", transport_calls[-1][2])
        elastic.indices.put_mapping("toto", {"properties": {"a": {}}}, index="i")
        self.assertIn("toto", transport_calls[-1][1])

        elastic.create("i", "toto", "1", {"a": 1})
        self.assertEqual(calls["create"][1]["doc_type"], "toto")
        elastic.bulk('{"index": {}}\n{}\n', index="i", doc_type="toto")
        self.assertEqual(calls["bulk"][0][0], '{"index": {}}\n{}\n')
        self.assertEqual(calls["bulk"][1]["doc_type"], "toto")
        elastic.delete_by_query("i", {"query": {}}, doc_type="toto")
        self.assertEqual(calls["delete_by_query"][1]["doc_type"], "toto")
        fetched = elastic.get("i", "toto", "1")
        self.assertEqual(calls["get"][1]["doc_type"], "toto")
        self.assertNotIn("_type", fetched)
        elastic.update_by_query("i", doc_type="toto", body={})
        self.assertEqual(calls["update_by_query"][1]["doc_type"], "toto")
        elastic.count(index="i", doc_type="toto")
        self.assertEqual(calls["count"][1]["doc_type"], "toto")
        elastic.index("i", "toto", {"a": 1}, id="1", version=3)
        self.assertEqual(calls["index"][1]["doc_type"], "toto")
        self.assertEqual(calls["index"][1]["version"], 3)
        self.assertNotIn("version_type", calls["index"][1])
        found = elastic.search(index="i", doc_type="toto", body={"query": {}})
        self.assertEqual(found["hits"]["total"], {"value": 3})
        self.assertEqual(calls["search"][1]["doc_type"], "toto")
        elastic.mget(body={"ids": ["1"]}, index="i", doc_type="toto")
        self.assertEqual(calls["mget"][1]["doc_type"], "toto")
        elastic.delete("i", "toto", "1", version=4)
        self.assertEqual(calls["delete"][0][1], "toto")
        self.assertEqual(calls["delete"][1]["version"], 4)
        self.assertNotIn("version_type", calls["delete"][1])
        actions = [{"_op_type": "index", "_type": "toto", "_source": {"a": 1}}]
        with patch.object(
            elastic_client_module.helpers, "bulk", return_value=(1, [])
        ) as bulk:
            self.assertEqual(elastic.helpers_bulk(actions), (1, []))
            self.assertIn("_type", bulk.call_args.kwargs["actions"][0])
        elastic.indices.recovery.__func__.__wrapped__(
            elastic.indices, "idx", params=None
        )
        self.assertIsNone(transport_calls[-1][2])

        elastic._version_major = 8
        elastic.es.search = lambda **kwargs: "raw"
        self.assertEqual(elastic.search(index="i", doc_type="toto", body={}), "raw")
        self.assertEqual(elastic.indices.delete("none*"), 0)
        self.assertEqual(elastic.indices.delete_safe("none*"), 0)
        elastic.indices.recovery.__func__.__wrapped__(
            elastic.indices, "idx", params=None
        )
        self.assertEqual(transport_calls[-1][2]["active_only"], "true")

    def _cleanup_extended(self, elastic, index, dest, template, pipeline):
        elastic.indices.delete_safe(index + "*")
        elastic.indices.delete_safe(dest)
        try:
            elastic.indices.delete_template(template)
        except TransportError:
            pass
        try:
            elastic.ingest.delete_pipeline(pipeline)
        except TransportError:
            pass
