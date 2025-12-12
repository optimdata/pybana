import datetime
import unittest
import copy
import json
import pytest
import sys
import os
from elasticsearch.exceptions import TransportError
from elasticsearch.helpers import scan

BASE_DIRECTORY = os.path.join(os.path.dirname(__file__), "..")  # NOQA
sys.path.insert(0, BASE_DIRECTORY)  # NOQA

from pybana.elastic.elastic_client import ElasticsearchExt, ScrollsCache, _get_scroll_id

# from ..client.base import JSONSerializer
from pybana.elastic.fixes_for_v8 import fusion_mappings, v6_to_v8, v8_to_v6


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
        for (origin, expected, title) in [
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
        for (origin, expected, title) in [
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
        for (origin, expected, title) in [
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
        for (origin, expected, title) in [
            (None, None, "none"),
            (1, 1, "invalid format"),
            (invalid_origin, invalid_dest, "invalid_origin 1"),
            (fixed_dest, fixed_dest, "unchanged case 1"),
            (fixed_origin, fixed_dest, "fixed interval"),
            (calendar_origin, calendar_dest, "calendar interval"),
            ([[fixed_origin]], [[fixed_dest]], "list in list"),
            ([{"a": fixed_origin}], [{"a": fixed_dest}], "dict in list"),
        ]:
            found = copy.deepcopy(origin)
            v6_to_v8.fix_histogram(found)
            self.assertEqual(found, expected, title)

    def test_fix_search_body(self):
        for (origin, expected, title) in [
            (None, None, "none"),
            ({}, {}, "empty"),
            (1, 1, "invalid format"),
            ('{"another":"toto"}', '{"another":"toto"}', "unchanged"),
            ('{"doc_type":"a","another":"toto"}', '{"another": "toto"}', "doctype 1"),
            ({"doc_type": "a", "another": "toto"}, {"another": "toto"}, "doctype 2"),
        ]:
            found = v6_to_v8.fix_search_body(origin)
            assert found == expected, title

    def test_fix_search_params(self):
        for (origin, expected, title) in [
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
        for (origin, expected, title) in [
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
        ]:
            found = v6_to_v8.fix_transport_error_args(origin)
            assert found == expected, title

    def test_fix_template(self):
        for (origin, expected, title) in [
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

    def test_scroll_id(self):
        self.assertEqual(_get_scroll_id("", {}), "")
        self.assertEqual(_get_scroll_id("", {"scroll_id": "a", "_scroll_id": "b"}), "b")
        self.assertEqual(_get_scroll_id("", {"scroll_id": "a"}), "a")
        self.assertEqual(_get_scroll_id("c", {"_scroll_id": "b"}), "c")
        self.assertEqual(_get_scroll_id(["e", "f"], {}), "e")


class TestElaticsearchClientCase(unittest.TestCase):
    def test_simple_es_ops_primary(self):
        self.simple_es_operations("http://localhost:9200")

    def test_simple_es_ops_secondary(self):
        self.simple_es_operations("http://localhost:9201")

    def test_scroll_ops_process_primary(self):
        self.scroll_ops_process("http://localhost:9200")

    def test_scroll_ops_process_secondary(self):
        self.scroll_ops_process("http://localhost:9201")

    def scroll_ops_process(self, host: str):
        elastic = ElasticsearchExt(
            hosts=[host],
            # serializer=JSONSerializer(),
            timeout=10,
        )
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

    def simple_es_operations(self, host: str):
        elastic = ElasticsearchExt(
            hosts=[host],
            # serializer=JSONSerializer(),
            timeout=10,
        )
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
