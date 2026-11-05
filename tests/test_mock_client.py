import os
import sys
import tempfile
import unittest
from unittest.mock import patch

from elasticsearch.exceptions import ConflictError, NotFoundError, RequestError
from elasticsearch.helpers import BulkIndexError, scan

BASE_DIRECTORY = os.path.join(os.path.dirname(__file__), "..")  # NOQA
sys.path.insert(0, BASE_DIRECTORY)  # NOQA

from pybana.elastic.mock_elastic_client import (  # noqa: E402
    ElasticMemory,
    MemoryDocument,
    MockElasticsearchCatPollux,
    MockElasticsearchIndicePollux,
    MockElasticsearchIngestPollux,
    MockElasticsearchPolluxClient,
    _metric,
    _sort_documents,
    new_stats,
    reset_stats,
)


class TestMockElasticsearchPolluxClient(unittest.TestCase):
    def _ids(self, response):
        return [hit["_id"] for hit in response["hits"]["hits"]]

    def test_version_is_v8(self):
        client = MockElasticsearchPolluxClient(
            hosts=["http://localhost:9201"], timeout=1
        )
        self.assertIsInstance(client.indices, MockElasticsearchIndicePollux)
        self.assertIsInstance(client.cat, MockElasticsearchCatPollux)
        self.assertIsInstance(client.ingest, MockElasticsearchIngestPollux)
        self.assertEqual(client.version, "8.19.3")
        self.assertEqual(client.version_major, 8)
        self.assertEqual(client.name, "8.19.3")
        self.assertEqual(client.info()["version"]["number"], "8.19.3")
        self.assertEqual(client.info()["tagline"], "You Know, for Search")

    def test_memory_can_be_set_from_a_test(self):
        client = MockElasticsearchPolluxClient()
        client.load(
            {
                "indices": {
                    "logs-0001": {
                        "aliases": ["logs"],
                        "documents": {
                            "1": {"machine_id": 7, "value": 1.5, "message": "alpha"},
                            "2": {"machine_id": 8, "value": 3, "message": "beta"},
                        },
                    }
                },
                "pipelines": {"geo": {"description": "geo", "processors": []}},
                "repositories": [{"id": "backup", "type": "fs"}],
            }
        )
        found = client.search(
            index="logs",
            doc_type="span",
            body={"query": {"term": {"machine_id": 7}}},
        )
        self.assertEqual(found["hits"]["total"], 1)
        self.assertEqual(found["hits"]["hits"][0]["_id"], "1")
        self.assertEqual(found["hits"]["hits"][0]["_type"], "span")
        self.assertEqual(found["hits"]["hits"][0]["_source"]["message"], "alpha")

        client.memory.add_document(
            "logs-0001", "3", {"machine_id": 7, "message": "gamma"}
        )
        self.assertEqual(
            client.count(index="logs", body={"query": {"term": {"machine_id": 7}}})[
                "count"
            ],
            2,
        )

    def test_constructor_documents_and_queries(self):
        client = MockElasticsearchPolluxClient(
            documents={
                "metrics": {
                    "a": {"status": "ok", "n": 1},
                    "b": {"status": "ko", "n": 5},
                    "c": {"status": "ok", "n": 9},
                }
            }
        )
        found = client.search(
            index="metrics",
            body={
                "query": {"match_all": {}},
                "aggs": {"by_status": {"terms": {"field": "status"}}},
                "sort": [{"n": "desc"}],
                "size": 10,
            },
        )
        self.assertEqual([hit["_id"] for hit in found["hits"]["hits"]], ["c", "b", "a"])
        buckets = {
            bucket["key"]: bucket["doc_count"]
            for bucket in found["aggregations"]["by_status"]["buckets"]
        }
        self.assertEqual(buckets, {"ok": 2, "ko": 1})
        filtered = client.search(
            index="metrics",
            body={
                "query": {
                    "bool": {
                        "filter": [
                            {"terms": {"status": ["ok"]}},
                            {"range": {"n": {"gte": 5}}},
                        ]
                    }
                }
            },
        )
        self.assertEqual([hit["_id"] for hit in filtered["hits"]["hits"]], ["c"])

    def test_document_and_index_ops(self):
        client = MockElasticsearchPolluxClient()
        index = "test_simple_es_operations_1"
        self.assertEqual(client.indices.delete(""), 0)
        self.assertEqual(client.indices.delete("test_simple_es_operations*"), 0)
        self.assertEqual(client.indices.create(index), 1)
        self.assertTrue(client.indices.exists(index))

        with self.assertRaises(ValueError):
            client.create(index, doc_type="toto", id="id_a", body=None)
        client.create(
            index, doc_type="toto", id="id_a", body={"key1": "val1"}, refresh="wait_for"
        )
        with self.assertRaises(ConflictError):
            client.create(index, doc_type="toto", id="id_a", body={"key1": "other"})
        with self.assertRaises(ValueError):
            client.get(index, id="", doc_type="toto")

        content = client.get(index, id="id_a", doc_type="toto")
        self.assertEqual(content["_index"], index)
        self.assertEqual(content["_type"], "toto")
        self.assertEqual(content["_id"], "id_a")
        self.assertEqual(content["_version"], 1)
        self.assertTrue(content["found"])
        self.assertEqual(content["_source"], {"key1": "val1"})

        client.index(index, doc_type="toto", id="id_b", body={"key1": "val2"})
        self.assertEqual(
            client.get(index, id="id_b", doc_type="toto")["_source"], {"key1": "val2"}
        )
        self.assertTrue(
            client.indices.put_alias(name="test_simple_es_operations", index=index)[
                "acknowledged"
            ]
        )
        self.assertTrue(client.indices.exists_alias(name="test_simple_es_operations"))
        self.assertFalse(client.indices.exists_alias(index="missing-index"))
        self.assertTrue(
            client.indices.delete_alias(name="test_simple_es_operations", index="_all")[
                "acknowledged"
            ]
        )

        client.delete(index, id="id_a", doc_type="toto", version=1)
        with self.assertRaises(NotFoundError):
            client.get(index, id="id_a", doc_type="toto")

        self.assertTrue(client.indices.close(index))
        self.assertTrue(client.indices.open(index))
        self.assertEqual(client.indices.delete("test_simple_es_operations_x*"), 0)
        self.assertEqual(client.indices.delete_safe("test_simple_es_operations_x*"), 0)
        self.assertEqual(client.indices.delete_safe("test_simple_es_operations_*"), 1)
        with self.assertRaises(NotFoundError) as raised:
            client.get("test_simple_es_operations_2", id="id_a", doc_type="toto")
        self.assertIn("index_not_found_exception", str(raised.exception))

    def test_templates_mappings_pipelines_and_cat(self):
        client = MockElasticsearchPolluxClient()
        self.assertEqual(
            client.indices.fix_mappings(
                {"span": {"properties": {"a": {"type": "keyword"}}}}
            ),
            {"properties": {"a": {"type": "keyword"}}},
        )
        template = {
            "index_patterns": ["logs-*"],
            "settings": {"index": {"number_of_shards": "1"}},
            "mappings": {"properties": {"machine_id": {"type": "long"}}},
        }
        self.assertEqual(
            client.indices.put_template("logs", template)["acknowledged"], True
        )
        self.assertTrue(client.indices.exists_template("logs"))
        stored = client.indices.get_template("logs")
        self.assertIn("logs", stored)
        self.assertEqual(
            stored["logs"]["mappings"]["properties"]["machine_id"]["type"], "long"
        )

        self.assertEqual(client.indices.create("logs-0001"), 1)
        mapping = client.indices.get_mapping(index="logs-0001")
        self.assertEqual(
            mapping["logs-0001"]["mappings"]["properties"]["machine_id"]["type"],
            "long",
        )
        client.indices.put_mapping(
            "span",
            {"properties": {"value": {"type": "float"}}},
            index="logs-0001",
        )
        mapping = client.indices.get_mapping(index="logs-0001", doc_type="span")
        properties = mapping["logs-0001"]["mappings"]["span"]["properties"]
        self.assertIn("value", properties)
        self.assertEqual(client.indices.delete_template("logs")["acknowledged"], True)

        client.ingest.put_pipeline("geo", {"processors": []})
        self.assertIn("geo", client.memory.pipelines)
        self.assertEqual(client.ingest.delete_pipeline("geo")["acknowledged"], True)
        with self.assertRaises(NotFoundError):
            client.ingest.delete_pipeline("geo")

        client.memory.repositories.append({"id": "backup", "type": "fs"})
        self.assertEqual(client.cat.repositories(), [{"id": "backup", "type": "fs"}])
        client.memory.add_document("logs-0001", "1", {"machine_id": 1})
        rows = client.cat.indices(index="logs-*")
        self.assertEqual(rows[0]["index"], "logs-0001")
        self.assertEqual(rows[0]["docs.count"], "1")
        self.assertEqual(client.cat.count(index="logs-0001")[0]["count"], "1")
        self.assertEqual(client.cat.templates(), [])

    def test_bulk_mget_update_and_delete_by_query(self):
        client = MockElasticsearchPolluxClient()
        client.indices.create("idx")
        success, errors = client.helpers_bulk(
            [
                {
                    "_index": "idx",
                    "_id": "1",
                    "_type": "span",
                    "_source": {"machine_id": 1, "review": "old"},
                },
                {
                    "_op_type": "index",
                    "_index": "idx",
                    "_id": "2",
                    "machine_id": 2,
                    "review": "keep",
                },
            ]
        )
        self.assertEqual(success, 2)
        self.assertEqual(errors, [])
        original = [
            {"_type": "span", "_index": "idx", "_id": "3", "_source": {"machine_id": 1}}
        ]
        client.helpers_bulk(original)
        self.assertIn("_type", original[0])

        body = (
            '{"index": {"_index": "idx", "_id": "4", "_type": "span"}}\n'
            '{"machine_id": 9}\n'
            '{"delete": {"_index": "idx", "_id": "2"}}\n'
        )
        bulked = client.bulk(body)
        self.assertFalse(bulked["errors"])
        self.assertFalse(client.get("idx", "span", "2", ignore=404).get("found", False))

        docs = client.mget(
            body={"ids": ["1", "missing"]},
            index="idx",
            doc_type="span",
            params={"_source_includes": "review"},
        )
        self.assertTrue(docs["docs"][0]["found"])
        self.assertEqual(docs["docs"][0]["_source"], {"review": "old"})
        self.assertFalse(docs["docs"][1]["found"])

        updated = client.update_by_query(
            index="idx",
            body={
                "query": {"term": {"machine_id": 1}},
                "script": {
                    "source": "ctx._source.review = params.review",
                    "params": {"review": "new"},
                },
            },
        )
        self.assertEqual(updated["updated"], 2)
        self.assertEqual(client.get("idx", "span", "1")["_source"]["review"], "new")

        deleted = client.delete_by_query(
            index="idx", body={"query": {"term": {"machine_id": 9}}}
        )
        self.assertEqual(deleted["deleted"], 1)
        copied = client.reindex(
            body={"source": {"index": "idx"}, "dest": {"index": "idx-copy"}}
        )
        self.assertGreater(copied["created"], 0)
        self.assertTrue(client.indices.exists("idx-copy"))

    def test_scroll_scan(self):
        client = MockElasticsearchPolluxClient()
        index = "test_scroll_ops"
        client.indices.create(index)
        for i in range(10):
            client.create(
                index, doc_type="toto", id="id_%s" % i, body={"key1": "val%s" % i}
            )
        client.indices.refresh(index)

        hits = {}
        for hit in scan(client, index=index, doc_type="toto", size=2):
            self.assertEqual(hit.get("_type"), "toto")
            self.assertEqual(list(hit["_source"].keys()), ["key1"])
            hits[hit["_id"]] = hit["_source"]["key1"]
        self.assertEqual(len(hits), 10)
        for i in range(10):
            self.assertEqual(hits["id_%s" % i], "val%s" % i)
        self.assertEqual(
            len(list(scan(client, index=index, doc_type=["toto"], size=2))), 10
        )
        self.assertEqual(len(client.scroll_cache), 0)
        self.assertEqual(client.scroll_cache.nb_scrolls_added, 2)

    def test_alias_rollover_and_replaced_memory(self):
        client = MockElasticsearchPolluxClient()
        client.indices.create("logs-000001")
        client.indices.put_alias(index="logs-000001", name="logs")
        rolled = client.indices.rollover("logs")
        self.assertEqual(rolled["old_index"], "logs-000001")
        self.assertTrue(rolled["rolled_over"])
        self.assertIn("logs", client.memory.indices[rolled["new_index"]].aliases)
        self.assertNotIn("logs", client.memory.indices["logs-000001"].aliases)

        replacement = ElasticMemory()
        replacement.add_document("other", "1", {"ok": True})
        client.memory = replacement
        self.assertEqual(list(client.memory.indices), ["other"])
        self.assertEqual(client.get("other", "span", "1")["_source"], {"ok": True})

    def test_call_stats(self):
        client = MockElasticsearchPolluxClient()
        client.indices.exists("logs")
        client.indices.exists("logs")
        client.indices.exists(index="other")
        client.ingest.put_pipeline("geo", {"processors": []})

        stats = client.stats
        self.assertGreater(stats["stats"]["count"], 0)
        self.assertEqual(
            stats["stats"]["count"],
            sum(
                class_stats["stats"]["count"]
                for class_stats in stats["classes"].values()
            ),
        )
        indice = stats["classes"]["MockElasticsearchIndicePollux"]
        self.assertEqual(
            indice["stats"]["count"],
            sum(fn["stats"]["count"] for fn in indice["functions"].values()),
        )
        exists = indice["functions"]["exists"]
        self.assertEqual(exists["stats"]["count"], 3)
        self.assertEqual(exists["parameters"]["logs"]["stats"]["count"], 2)
        self.assertEqual(exists["parameters"]["index=other"]["stats"]["count"], 1)
        pipeline = stats["classes"]["MockElasticsearchIngestPollux"]["functions"][
            "put_pipeline"
        ]
        self.assertEqual(
            pipeline["parameters"]['geo,{"processors":[]}']["stats"]["count"], 1
        )

        fresh = MockElasticsearchPolluxClient()
        self.assertEqual(fresh.stats["stats"]["count"], 0)
        self.assertIsNot(fresh.stats, client.stats)
        shared = MockElasticsearchPolluxClient(stats=client.stats)
        shared.indices.exists("shared")
        self.assertEqual(exists["parameters"]["shared"]["stats"]["count"], 1)

    def test_parameter_stats_cap(self):
        client = MockElasticsearchPolluxClient()
        for i in range(501):
            client.indices.exists("n-%s" % i)
        params = client.stats["classes"]["MockElasticsearchIndicePollux"]["functions"][
            "exists"
        ]["parameters"]
        specific = [key for key in params if key != "too_much_parameters"]
        self.assertEqual(len(specific), 500)
        self.assertEqual(params["too_much_parameters"]["stats"]["count"], 1)
        client.indices.exists("n-0")
        self.assertEqual(params["n-0"]["stats"]["count"], 2)
        self.assertEqual(params["too_much_parameters"]["stats"]["count"], 1)

    def test_save_and_load_json_and_yaml(self):
        client = MockElasticsearchPolluxClient()
        client.memory.add_index(
            "logs-0001",
            aliases=["logs"],
            mappings={"properties": {"machine_id": {"type": "long"}}},
            settings={"index": {"number_of_shards": "1"}},
        )
        client.memory.add_document("logs-0001", "b", {"machine_id": 2}, doc_type="span")
        client.memory.add_document(
            "logs-0001",
            "a",
            {"machine_id": 1, "note": "café"},
            doc_type="span",
            version=4,
        )
        client.indices.create("closed-idx")
        client.indices.close("closed-idx")
        client.indices.put_template(
            "logs",
            {
                "index_patterns": ["logs-*"],
                "mappings": {"properties": {"machine_id": {"type": "long"}}},
            },
        )
        client.ingest.put_pipeline("geo", {"description": "geo", "processors": []})
        client.memory.repositories.append({"id": "backup", "type": "fs"})
        original = client.memory.to_dict()

        with tempfile.TemporaryDirectory() as directory:
            json_path = os.path.join(directory, "cluster.json")
            yaml_path = os.path.join(directory, "cluster.yml")
            self.assertEqual(client.save(json_path), json_path)
            self.assertEqual(client.memory.save(yaml_path), yaml_path)

            restored = MockElasticsearchPolluxClient()
            restored.load(json_path)
            self.assertEqual(restored.memory.to_dict(), original)
            content = restored.get("logs", "span", "a")
            self.assertEqual(content["_version"], 4)
            self.assertEqual(content["_source"]["note"], "café")
            self.assertTrue(restored.memory.indices["closed-idx"].closed)
            self.assertIn("logs", restored.memory.templates)
            self.assertEqual(restored.memory.pipelines["geo"]["description"], "geo")
            self.assertEqual(restored.memory.repositories[0]["id"], "backup")

            from_yaml = ElasticMemory()
            from_yaml.load(yaml_path)
            self.assertEqual(from_yaml.to_dict(), original)

            with self.assertRaises(ValueError):
                client.save(os.path.join(directory, "cluster.txt"))

    def test_stats_reset_and_untracked_calls(self):
        stats = new_stats()
        stats["stats"]["count"] = 4
        stats["classes"]["X"] = {"stats": {"count": 1}}
        reset_stats(stats)
        self.assertEqual(stats, {"stats": {"count": 0}, "classes": {}})

        client = MockElasticsearchPolluxClient()
        circular = {}
        circular["self"] = circular
        client.info(blob=circular)
        client.stats = None
        self.assertEqual(client.info()["name"], "mock-elasticsearch")
        self.assertFalse(client.indices.exists("logs"))
        self.assertEqual(client.indices.version_major, 8)
        self.assertEqual(client.cat.version_major, 8)
        self.assertEqual(client.tasks.list(), {"nodes": {}})
        self.assertEqual(client.tasks.get(task_id="t1")["task"]["id"], "t1")
        self.assertIn("MockElasticsearchPolluxClient", repr(client))
        self.assertIn("indices=[]", repr(client))
        with self.assertRaises(NotImplementedError):
            client.transport.perform_request("GET", "/_search")
        with self.assertRaises(NotImplementedError):
            client._transport_perform_request("POST", "/_bulk")

    def test_memory_documents_and_file_edges(self):
        memory = ElasticMemory()
        memory.add_index("logs", settings={"a": 1}, closed=False)
        memory.add_index(
            "logs",
            mappings={"properties": {}},
            aliases={"logs": {}, "bad": "x"},
            closed=True,
        )
        memory.add_document("logs", "1", None, doc_type="", seq_no=5, primary_term=3)
        memory.add_document("logs", "2", {"n": 1}, seq_no=3)
        memory.add_documents("logs", {})
        memory.add_documents(
            "logs",
            {
                "wrap": {
                    "_id": "real",
                    "_source": {"ok": True},
                    "_type": "span",
                    "_version": 2,
                    "_seq_no": 8,
                    "_primary_term": 4,
                },
                "plain": {"n": 2},
            },
        )
        memory.add_documents(
            "other",
            [{"_id": "z", "_source": "text"}, None, {"bare": True}],
        )
        self.assertEqual(memory.indices["logs"].documents["1"].source, {})
        self.assertEqual(memory.indices["logs"].documents["1"].doc_type, "_doc")
        self.assertEqual(memory.indices["logs"].documents["1"].primary_term, 3)
        self.assertEqual(memory.indices["logs"].documents["2"].seq_no, 3)
        self.assertEqual(memory.indices["logs"].documents["real"].version, 2)
        self.assertEqual(memory.indices["logs"].documents["real"].doc_type, "span")
        self.assertGreaterEqual(memory._seq, 8)
        self.assertTrue(memory.indices["logs"].closed)
        self.assertEqual(memory.indices["logs"].aliases["bad"], {})
        self.assertEqual(memory.indices["other"].documents["z"].source, {})
        self.assertEqual(memory.indices["other"].documents["1"].source, {})
        self.assertEqual(memory.indices["other"].documents["2"].source, {"bare": True})

        memory.load(None)
        self.assertEqual(list(memory.indices), [])
        memory.load(
            {"seq": 4, "indices": {"bare": None, "with-docs": {"documents": []}}}
        )
        self.assertEqual(memory._seq, 4)
        self.assertIn("bare", memory.indices)
        self.assertEqual(memory.indices["with-docs"].documents, {})

        with tempfile.TemporaryDirectory() as directory:
            previous = os.getcwd()
            os.chdir(directory)
            try:
                self.assertEqual(memory.save("cluster.json"), "cluster.json")
                nested = os.path.join("sub", "cluster.yaml")
                self.assertEqual(memory.save(nested), nested)
                reloaded = ElasticMemory()
                reloaded.load("cluster.json")
                self.assertEqual(reloaded.to_dict(), memory.to_dict())

                empty = os.path.join(directory, "empty.json")
                with open(empty, "w", encoding="utf-8") as handle:
                    handle.write("  \n")
                blank = ElasticMemory()
                blank.load(empty)
                self.assertEqual(blank.indices, {})

                with open(
                    os.path.join(directory, "list.json"), "w", encoding="utf-8"
                ) as handle:
                    handle.write("[]\n")
                with self.assertRaises(ValueError):
                    ElasticMemory().load(os.path.join(directory, "list.json"))

                with open(
                    os.path.join(directory, "bad.txt"), "w", encoding="utf-8"
                ) as handle:
                    handle.write("{}\n")
                with self.assertRaises(ValueError):
                    ElasticMemory().load(os.path.join(directory, "bad.txt"))

                with open(
                    os.path.join(directory, "null.yml"), "w", encoding="utf-8"
                ) as handle:
                    handle.write("null\n")
                ElasticMemory().load(os.path.join(directory, "null.yml"))

                with open(
                    os.path.join(directory, "list.yml"), "w", encoding="utf-8"
                ) as handle:
                    handle.write("- 1\n")
                with self.assertRaises(ValueError):
                    ElasticMemory().load(os.path.join(directory, "list.yml"))

                with patch.dict(sys.modules, {"yaml": None}):
                    with self.assertRaises(ImportError):
                        ElasticMemory().save(os.path.join(directory, "needs-yaml.yml"))
            finally:
                os.chdir(previous)

    def _query_client(self):
        return MockElasticsearchPolluxClient(
            documents={
                "docs": {
                    "a": {
                        "status": "ok",
                        "n": 1,
                        "tags": ["red", None],
                        "note": "Hello World",
                        "nums": [True],
                        "when": "2020-01-02",
                        "nested": {"field": "zz"},
                        "maybe": None,
                        "flag": True,
                    },
                    "b": {
                        "status": "ok",
                        "n": 9,
                        "tags": ["blue"],
                        "note": "hello",
                        "nums": [1],
                        "when": "2020-01-20",
                    },
                    "c": {
                        "status": "ko",
                        "n": "10",
                        "tags": "red",
                        "note": 5,
                        "extra": {"inner": ["yes"]},
                        "when": "2019-05-01",
                    },
                    "d": {
                        "status": "ko",
                        "tags": ["red", "blue"],
                        "note": "other",
                        "when": "2020-06-01",
                    },
                }
            }
        )

    def test_query_clauses(self):
        client = self._query_client()
        cases = [
            ({"query": {"match_none": {}}}, []),
            ({"query": {"match_all": {}}}, ["a", "b", "c", "d"]),
            ({"query": {"fuzzy": {"note": "hello"}}}, ["a", "b", "c", "d"]),
            ({"query": {"ids": {"values": ["b", "d", "missing"]}}}, ["b", "d"]),
            (
                {
                    "query": {
                        "term": {
                            "status": {"value": "ok", "boost": 2},
                            "boost": 1,
                            "_name": "named",
                        },
                        "boost": 1,
                    }
                },
                ["a", "b"],
            ),
            ({"query": {"term": {"status": "nope"}}}, []),
            ({"query": {"term": {"tags": "red"}}}, ["a", "c", "d"]),
            ({"query": {"term": {"tags": "green"}}}, []),
            ({"query": {"terms": {"status": ["ok"], "boost": 2}}}, ["a", "b"]),
            ({"query": {"terms": {"tags": {"values": ["blue"]}}}}, ["b", "d"]),
            ({"query": {"terms": {"tags": ["green"]}}}, []),
            ({"query": {"match": {"note": "hello", "boost": 1}}}, ["a", "b"]),
            ({"query": {"match": {"note": {"query": "missingword"}}}}, []),
            ({"query": {"match": {"n": 1}}}, ["a"]),
            ({"query": {"match_phrase": {"tags": "red"}}}, ["a", "c", "d"]),
            ({"query": {"exists": {"field": "nested.field"}}}, ["a"]),
            ({"query": {"exists": {"field": "maybe"}}}, []),
            ({"query": {"exists": {}}}, []),
            ({"query": {"exists": {"field": "status"}}}, ["a", "b", "c", "d"]),
            ({"query": {"wildcard": {"note": {"value": "He*"}}}}, ["a"]),
            ({"query": {"wildcard": {"n": "1*"}}}, ["c"]),
            ({"query": {"prefix": {"note": {"value": "hello"}}}}, ["b"]),
            ({"query": {"prefix": {"n": "1"}}}, ["c"]),
            (
                {
                    "query": {
                        "range": {"when": {"gte": "2020-01-10", "lt": "2020-02-01"}}
                    }
                },
                ["b"],
            ),
            ({"query": {"range": {"nums": {"gte": 2}}}}, ["a"]),
            (
                {"query": {"range": {"n": {"boost": 1, "gte": 1, "lte": 9}}}},
                ["a", "b", "c"],
            ),
            ({"query": {"range": {"n": {"gt": 100}}}}, []),
            ({"query": {"range": {"absent": {"gte": 0}}}}, []),
            ({"query": {"range": {"n": "1"}}}, []),
            (
                {"query": {"constant_score": {"filter": {"term": {"status": "ko"}}}}},
                ["c", "d"],
            ),
            ({"query": {"query_string": "yes"}}, ["c"]),
            ({"query": {"query_string": {"query": "hello AND world"}}}, ["a"]),
            ({"query": {"query_string": {"query": "AND OR"}}}, ["a", "b", "c", "d"]),
            ({"query": {"query_string": {"query": ""}}}, ["a", "b", "c", "d"]),
            (
                {"query": {"bool": {"must_not": [{"term": {"status": "ko"}}]}}},
                ["a", "b"],
            ),
            (
                {
                    "query": {
                        "bool": {
                            "should": [
                                {"term": {"status": "ok"}},
                                {"term": {"status": "ko"}},
                            ],
                            "minimum_should_match": 2,
                        }
                    }
                },
                [],
            ),
            (
                {
                    "query": {
                        "bool": {
                            "should": [
                                {"term": {"status": "ok"}},
                                {"term": {"status": "nope"}},
                            ],
                            "minimum_should_match": 1,
                        }
                    }
                },
                ["a", "b"],
            ),
            (
                {
                    "query": {
                        "bool": {
                            "must": [{"term": {"status": "ok"}}],
                            "should": [{"term": {"status": "ko"}}],
                        }
                    }
                },
                ["a", "b"],
            ),
            (
                {
                    "query": {
                        "bool": {
                            "must": ["ignored", {"term": {"status": "ok"}}],
                        }
                    }
                },
                ["a", "b"],
            ),
            ({"query": {"bool": {"filter": {"term": {"status": "ok"}}}}}, ["a", "b"]),
            (
                {"query": {"range": {"boost": 2, "_name": "r", "absent": {"gte": 0}}}},
                [],
            ),
            ({"query": {"wildcard": {"boost": 1, "note": "He*"}}}, ["a"]),
            ({"query": {"prefix": {"_name": "p", "note": "hello"}}}, ["b"]),
            (
                {
                    "query": {"match_all": {}},
                    "post_filter": {"term": {"status": "ko"}},
                },
                ["c", "d"],
            ),
        ]
        for body, expected in cases:
            with self.subTest(body=body):
                self.assertEqual(
                    self._ids(client.search(index="docs", body=body)), expected
                )

        as_text = client.search(
            index="docs", body='{"query": {"term": {"status": "ko"}}}'
        )
        self.assertEqual(self._ids(as_text), ["c", "d"])
        self.assertEqual(client.search(index="docs", body="  ")["hits"]["total"], 4)
        self.assertEqual(client.search(index="docs", body="[]")["hits"]["total"], 4)
        self.assertEqual(client.search(index="docs", body=0)["hits"]["total"], 4)
        self.assertEqual(client.search()["hits"]["total"], 4)

        filtered = client.get("docs", "span", "a", _source_includes="status,missing")
        self.assertEqual(filtered["_source"], {"status": "ok"})
        nested = client.search(
            index="docs",
            body={
                "_source": ["status", "nested.field", "missing"],
                "query": {"ids": {"values": ["a"]}},
            },
        )
        self.assertEqual(
            nested["hits"]["hits"][0]["_source"],
            {"status": "ok", "nested": {"field": "zz"}},
        )
        includes = client.search(
            index="docs",
            body={
                "_source": {"includes": ["note"]},
                "query": {"ids": {"values": ["b"]}},
            },
        )
        self.assertEqual(includes["hits"]["hits"][0]["_source"], {"note": "hello"})
        hidden = client.search(
            index="docs", body={"_source": False, "size": 1, "query": {"match_all": {}}}
        )
        self.assertEqual(hidden["hits"]["hits"][0]["_source"], {})
        paged = client.search(index="docs", from_=3, size=None)
        self.assertEqual(self._ids(paged), ["d"])

        self.assertEqual(
            self._ids(client.search(index="docs", body={"sort": ["_id"], "size": 10})),
            ["a", "b", "c", "d"],
        )
        self.assertEqual(
            self._ids(client.search(index="docs", body={"sort": ["_doc"], "size": 10})),
            ["a", "b", "c", "d"],
        )
        self.assertEqual(
            self._ids(
                client.search(
                    index="docs", body={"sort": ["status", "_id"], "size": 10}
                )
            ),
            ["c", "d", "a", "b"],
        )
        self.assertEqual(
            self._ids(
                client.search(
                    index="docs",
                    body={"sort": [{"status": {"order": "desc"}}], "size": 10},
                )
            ),
            ["a", "b", "c", "d"],
        )
        self.assertEqual(
            self._ids(client.search(index="docs", body={"sort": "status", "size": 10})),
            ["c", "d", "a", "b"],
        )
        by_n = self._ids(
            client.search(
                index="docs",
                body={"sort": {"n": "asc"}, "size": 10},
            )
        )
        self.assertEqual(by_n[-1], "d")
        self.assertEqual(set(by_n), {"a", "b", "c", "d"})
        self.assertEqual(
            self._ids(
                client.search(index="docs", body={"sort": [{"status": 1}], "size": 10})
            ),
            ["c", "d", "a", "b"],
        )
        tied = MockElasticsearchPolluxClient(
            documents={"ids": {"b": {"k": 1}, "a": {"k": 1}}}
        )
        self.assertEqual(
            self._ids(tied.search(index="ids", body={"sort": ["k"], "size": 10})),
            ["a", "b"],
        )

        with self.assertRaises(NotFoundError):
            client.search(index="missing")
        self.assertEqual(client.search(index="missing-*")["hits"]["total"], 0)
        with self.assertRaises(NotFoundError):
            client.search(index=["docs", "missing"])

        self.assertTrue(client.indices.close("docs"))
        self.assertEqual(client.search(index="docs")["hits"]["total"], 0)
        rows = client.cat.indices(index=None, h="index,status")
        self.assertEqual(rows, [{"index": "docs", "status": "close"}])
        self.assertTrue(client.indices.open("docs"))

    def test_aggregations(self):
        client = self._query_client()
        found = client.search(
            index="docs",
            body={
                "size": 0,
                "aggregations": {
                    "ok_docs": {
                        "filter": {"term": {"status": "ok"}},
                        "aggs": {"avg_n": {"avg": {"field": "n"}}},
                    },
                    "sum_n": {"sum": {"field": "n"}},
                    "sum_nums": {"sum": {"field": "nums"}},
                    "min_n": {"min": {"field": "n"}},
                    "max_n": {"max": {"field": "n"}},
                    "avg_n": {"avg": {"field": "n"}},
                    "count_note": {"value_count": {"field": "note"}},
                    "card_status": {"cardinality": {"field": "status"}},
                    "count_flag": {"value_count": {"field": "flag"}},
                    "sum_flag": {"sum": {"field": "flag"}},
                    "sum_missing": {"sum": {"field": "absent"}},
                    "avg_missing": {"avg": {"field": "absent"}},
                    "min_missing": {"min": {"field": "absent"}},
                    "by_status": {
                        "terms": {"field": "status", "size": 1},
                        "aggs": {"max_n": {"max": {"field": "n"}}},
                    },
                    "by_tag": {"terms": {"field": "tags", "size": 10}},
                    "by_absent": {"terms": {"field": "absent"}},
                    "all_docs": {"filter": {"match_all": {}}},
                    "unknown": {"weird": {}},
                },
            },
        )
        self.assertEqual(found["hits"]["hits"], [])
        aggs = found["aggregations"]
        self.assertEqual(aggs["ok_docs"]["doc_count"], 2)
        self.assertEqual(aggs["ok_docs"]["avg_n"]["value"], 5)
        self.assertEqual(aggs["sum_n"]["value"], 10)
        self.assertEqual(aggs["sum_nums"]["value"], 1)
        self.assertEqual(aggs["min_n"]["value"], 1)
        self.assertEqual(aggs["max_n"]["value"], 9)
        self.assertEqual(aggs["avg_n"]["value"], 5)
        self.assertEqual(aggs["count_note"]["value"], 4)
        self.assertEqual(aggs["card_status"]["value"], 2)
        self.assertEqual(aggs["count_flag"]["value"], 1)
        self.assertEqual(aggs["sum_flag"]["value"], 0)
        self.assertEqual(aggs["sum_missing"]["value"], 0)
        self.assertIsNone(aggs["avg_missing"]["value"])
        self.assertIsNone(aggs["min_missing"]["value"])
        self.assertEqual(aggs["by_status"]["sum_other_doc_count"], 2)
        self.assertEqual(aggs["by_status"]["buckets"][0]["key"], "ko")
        self.assertEqual(aggs["by_status"]["buckets"][0]["doc_count"], 2)
        tags = {
            bucket["key"]: bucket["doc_count"] for bucket in aggs["by_tag"]["buckets"]
        }
        self.assertEqual(tags, {"red": 3, "blue": 2})
        self.assertEqual(aggs["by_absent"]["buckets"], [])
        self.assertEqual(aggs["all_docs"]["doc_count"], 4)
        self.assertEqual(aggs["unknown"], {})

    def test_histogram_range_top_hits_and_bucket_pipeline(self):
        client = MockElasticsearchPolluxClient(
            documents={
                "events": {
                    "a": {"ts": "2019-01-01T00:30:00", "n": 1, "s": "a"},
                    "b": {"ts": "2019-01-01T01:10:00", "n": 3, "s": "b"},
                    "c": {"ts": "2019-01-01T01:40:00", "n": 5, "s": "b"},
                }
            }
        )
        found = client.search(
            index="events",
            body={
                "size": 0,
                "aggs": {
                    "by_hour": {
                        "date_histogram": {
                            "field": "ts",
                            "calendar_interval": "1h",
                        },
                        "aggs": {"avg_n": {"avg": {"field": "n"}}},
                    },
                    "by_range": {
                        "range": {
                            "field": "n",
                            "ranges": [{"from": 0, "to": 4}, {"from": 4, "to": 10}],
                        }
                    },
                    "stats": {"extended_stats": {"field": "n"}},
                    "median": {"percentiles": {"field": "n", "percents": [50]}},
                    "top": {
                        "top_hits": {
                            "size": 1,
                            "sort": {"n": {"order": "desc"}},
                            "_source": "s",
                        }
                    },
                    "avg_of_hours": {"avg_bucket": {"buckets_path": "by_hour>_count"}},
                },
            },
        )
        aggs = found["aggregations"]
        self.assertEqual(
            [bucket["doc_count"] for bucket in aggs["by_hour"]["buckets"]], [1, 2]
        )
        self.assertEqual(aggs["by_hour"]["buckets"][0]["avg_n"]["value"], 1)
        self.assertEqual(
            [bucket["doc_count"] for bucket in aggs["by_range"]["buckets"]], [2, 1]
        )
        self.assertEqual(aggs["by_range"]["buckets"][0]["key"], "0.0-4.0")
        self.assertEqual(aggs["stats"]["count"], 3)
        self.assertIn("std_deviation", aggs["stats"])
        self.assertIn("50.0", aggs["median"]["values"])
        self.assertEqual(aggs["top"]["hits"]["hits"][0]["_source"], {"s": "b"})
        self.assertEqual(aggs["avg_of_hours"]["value"], 1.5)

    def test_scripts_update_and_version_conflicts(self):
        client = MockElasticsearchPolluxClient()
        client.index("docs", "span", {"name": "ada", "n": 1}, id="1")
        client.index("docs", None, {"name": "grace"}, id="2")
        generated = client.index("docs", "span", {"name": "lin"})
        self.assertTrue(generated["_id"])
        self.assertEqual(generated["result"], "created")

        untouched = client.update_by_query(
            index="docs", body={"query": {"term": {"name": "ada"}}}
        )
        self.assertEqual(untouched["updated"], 1)
        self.assertEqual(client.get("docs", "span", "1")["_source"]["name"], "ada")

        client.update_by_query(
            index="docs",
            body={
                "query": {"ids": {"values": ["1"]}},
                "script": "",
            },
        )
        updated = client.update_by_query(
            index="docs",
            body={
                "query": {"match_all": {}},
                "script": (
                    "ctx._source.ok = true; ctx._source.flag = false; "
                    "ctx._source.gone = null; ctx._source.title = 'x'; "
                    "ctx._source.ratio = 1.5; ctx._source.whole = 4; "
                    "ctx._source.bad = nope; skip this; "
                    "ctx._source.nested.value = params.v; "
                    "ctx._source.nested.extra = 2; ctx._source. = 1"
                ),
                "params": {"v": "z"},
            },
        )
        self.assertEqual(updated["updated"], 3)
        source = client.get("docs", "span", "1")["_source"]
        self.assertIs(source["ok"], True)
        self.assertIs(source["flag"], False)
        self.assertIsNone(source["gone"])
        self.assertEqual(source["title"], "x")
        self.assertEqual(source["ratio"], 1.5)
        self.assertEqual(source["whole"], 4)
        self.assertNotIn("bad", source)
        self.assertEqual(source["nested"]["value"], "z")
        self.assertEqual(source["nested"]["extra"], 2)

        client.update_by_query(
            index="docs",
            body={
                "query": {"ids": {"values": ["2"]}},
                "script": {
                    "source": "ctx._source.name = params.name",
                    "params": {"name": "inner"},
                },
                "params": {"name": "outer"},
            },
        )
        self.assertEqual(client.get("docs", "span", "2")["_source"]["name"], "outer")

        conflicted = client.index(
            "docs",
            "span",
            {"name": "ada2"},
            id="1",
            version=1,
            version_type="external",
        )
        self.assertEqual(conflicted["_version"], 2)
        same = client.index(
            "docs",
            "span",
            {"name": "ada3"},
            id="1",
            version=2,
            version_type="external_gte",
        )
        self.assertEqual(same["_version"], 2)
        client.index(
            "docs", "span", {"name": "ada4"}, id="1", version=5, version_type="external"
        )
        with self.assertRaises(ConflictError):
            client.index(
                "docs",
                "span",
                {"name": "ada5"},
                id="1",
                version=3,
                version_type="external_gte",
            )
        with self.assertRaises(ConflictError):
            client.index(
                "docs",
                "span",
                {"name": "ada6"},
                id="1",
                version="5",
                version_type="external",
            )
        kept = client.index(
            "docs",
            "span",
            {"name": "ada7"},
            id="1",
            version=5,
            version_type="internal",
        )
        self.assertEqual(kept["_version"], 5)
        with self.assertRaises(ConflictError):
            client.index(
                "docs",
                "span",
                {"name": "ada8"},
                id="1",
                version=1,
                version_type="internal",
            )
        with self.assertRaises(ConflictError):
            client.index(
                "docs",
                "span",
                {"name": "new"},
                id="brand-new",
                version=1,
                version_type="internal",
            )

        client.delete("docs", "span", "1", version=5)
        with self.assertRaises(NotFoundError):
            client.get("docs", "span", "1")
        with self.assertRaises(ConflictError):
            client.delete("docs", "span", "2", version=1, version_type="internal")
        self.assertTrue(client.get("docs", "span", "2")["found"])
        with self.assertRaises(NotFoundError):
            client.delete("docs", "span", "missing")
        missing = client.delete("docs", "span", "missing", ignore=404)
        self.assertEqual(missing["status"], 404)
        with self.assertRaises(NotFoundError):
            client.get("no-index", "span", "1")
        self.assertEqual(client.get("no-index", "span", "1", ignore=404)["status"], 404)
        self.assertNotIn("_type", client.get("docs", None, "2"))
        with self.assertRaises(NotFoundError):
            client.delete("no-index", "span", "1")
        self.assertEqual(
            client.delete("no-index", "span", "1", ignore=404)["status"], 404
        )
        external = client.index("docs", "span", {"name": "ext"}, id="ext", version=3)
        self.assertEqual(external["_version"], 3)

        fresh = MockElasticsearchPolluxClient()
        fresh.indices.create("seq")
        first = fresh.index("seq", "span", {"a": 1}, id="1")
        second = fresh.index("seq", "span", {"a": 2}, id="2")
        self.assertEqual(first["_seq_no"], 0)
        self.assertEqual(second["_seq_no"], 1)
        fresh.index("other", "span", {"a": 1}, id="1")
        self.assertEqual(fresh.get("other", "span", "1")["_seq_no"], 0)
        with self.assertRaises(ConflictError):
            fresh.delete("seq", "span", "1", version="1")

        client.indices.create("i1")
        client.indices.create("i2")
        client.indices.put_alias(index=["i1", "i2"], name="both")
        client.index("i2", "span", {"ok": True}, id="9")
        self.assertEqual(client.get("both", "span", "9")["_index"], "i2")
        self.assertEqual(client.delete("both", "span", "9")["result"], "deleted")

    def test_version_6_write_paths(self):
        client = MockElasticsearchPolluxClient()
        client._version_major = 6
        self.assertEqual(client.indices.version_major, 6)
        self.assertEqual(client.ingest.version_major, 6)
        self.assertEqual(client.indices.fix_mappings(None), {})
        self.assertEqual(
            client.indices.fix_mappings(
                {"span": {"properties": {"a": {"type": "keyword"}}}}
            ),
            {"span": {"properties": {"a": {"type": "keyword"}}}},
        )
        self.assertEqual(
            client.indices.put_template("events", {"index_patterns": ["events-*"]})[
                "acknowledged"
            ],
            True,
        )
        self.assertNotIn("settings", client.memory.templates["events"])
        self.assertEqual(
            client.indices.put_template(
                "maps",
                {
                    "index_patterns": ["idx"],
                    "mappings": {"properties": {"b": {"type": "long"}}},
                },
            )["acknowledged"],
            True,
        )
        self.assertNotIn("settings", client.memory.templates["maps"])
        self.assertEqual(
            client.indices.create(
                "idx",
                body={"mappings": {"span": {"properties": {"a": {"type": "keyword"}}}}},
            ),
            1,
        )
        self.assertNotIn("index", client.memory.indices["idx"].settings)
        created = client.create(
            "idx", "span", "1", {"a": 1}, version=4, version_type="external"
        )
        self.assertEqual(created["_version"], 4)
        self.assertEqual(client.index("idx", None, {"a": 2})["result"], "created")
        success, errors = client.helpers_bulk(
            [{"_index": "idx", "_type": "span", "_id": "2", "_source": {"a": 3}}]
        )
        self.assertEqual(success, 1)
        self.assertEqual(errors, [])
        self.assertEqual(client.memory.indices["idx"].documents["2"].doc_type, "span")
        mapping = client.indices.get_mapping(index="idx", doc_type="span")
        self.assertIn("span", mapping["idx"]["mappings"])
        self.assertEqual(client.delete("idx", "span", "1")["result"], "deleted")
        self.assertNotIn("1", client.memory.indices["idx"].documents)

    def test_index_admin_aliases_and_cat(self):
        client = MockElasticsearchPolluxClient()
        self.assertFalse(client.indices.exists(""))
        self.assertFalse(client.indices.exists(None))
        self.assertEqual(client.indices.refresh(None)["_shards"]["successful"], 1)
        self.assertTrue(client.indices.open("missing-*"))
        self.assertTrue(client.indices.open("missing", ignore=404))
        with self.assertRaises(NotFoundError):
            client.indices.close("missing")
        with self.assertRaises(NotFoundError):
            client.indices._meta("missing")
        self.assertIsNone(client.indices._meta("missing", ignore=404))

        self.assertEqual(
            client.indices.put_template(
                "low",
                {
                    "template": "events-*,other-*",
                    "order": 0,
                    "settings": {
                        "index": {"number_of_shards": "1", "number_of_replicas": "0"}
                    },
                    "aliases": {"events": {}},
                },
            )["acknowledged"],
            True,
        )
        self.assertEqual(
            client.indices.put_template(
                "maps-only",
                {
                    "index_patterns": ["events-*"],
                    "order": 2,
                    "mappings": {"properties": {"b": {"type": "long"}}},
                },
            )["acknowledged"],
            True,
        )
        self.assertEqual(
            client.indices.put_template(
                "high",
                {
                    "index_patterns": ["events-*"],
                    "order": 5,
                    "settings": {"index": {"number_of_shards": "3"}},
                    "mappings": {"properties": {"a": {"type": "keyword"}}},
                },
            )["acknowledged"],
            True,
        )
        self.assertEqual(client.indices.create("events-0001"), 1)
        meta = client.memory.indices["events-0001"]
        self.assertEqual(meta.settings["index"]["number_of_shards"], "3")
        self.assertEqual(meta.settings["index"]["number_of_replicas"], "0")
        self.assertEqual(meta.mappings["properties"]["a"]["type"], "keyword")
        self.assertEqual(meta.mappings["properties"]["b"]["type"], "long")
        self.assertIn("events", meta.aliases)
        self.assertEqual(client.indices._meta("events-0001").name, "events-0001")

        self.assertEqual(
            client.indices.create(
                "body-idx",
                body={
                    "settings": {"index": {"number_of_shards": "1"}},
                    "mappings": {"properties": {"a": {"type": "keyword"}}},
                    "aliases": {"body-alias": {}, "loose": "nope"},
                },
            ),
            1,
        )
        body_meta = client.memory.indices["body-idx"]
        self.assertEqual(body_meta.settings["index"]["codec"], "best_compression")
        self.assertEqual(body_meta.settings["index"]["number_of_shards"], "1")
        self.assertEqual(body_meta.aliases["loose"], {})
        with self.assertRaises(RequestError):
            client.indices.create("body-idx")
        self.assertEqual(client.indices.create("body-idx", ignore=400)["status"], 400)

        settings = client.indices.get_settings("events-*")
        self.assertIn("events-0001", settings)
        full = client.indices.get("body-alias")
        self.assertIn("mappings", full["body-idx"])
        self.assertIn("settings", full["body-idx"])
        self.assertIn("aliases", full["body-idx"])
        only = client.indices.get("body-idx", feature="mappings")
        self.assertEqual(list(only["body-idx"]), ["mappings"])
        partial = client.indices.get("body-idx", feature=("settings", "aliases"))
        self.assertEqual(set(partial["body-idx"]), {"settings", "aliases"})
        self.assertEqual(client.indices.get("missing", ignore_unavailable=True), {})
        with self.assertRaises(NotFoundError):
            client.indices.get("missing")
        self.assertEqual(client.indices.get("missing-*", ignore_unavailable=True), {})
        self.assertEqual(client.indices.recovery(index="body-idx"), [])

        self.assertTrue(client.indices.exists("_all"))
        self.assertTrue(client.indices.exists("*"))
        self.assertTrue(client.indices.exists(["events-*", "body-idx"]))
        self.assertTrue(client.indices.exists_template("*"))
        self.assertFalse(client.indices.exists_template("no-such-*"))
        client.indices.create("plain")
        self.assertNotIn("plain", client.indices.get_alias())
        self.assertIn("events-0001", client.indices.get_alias())
        self.assertIn(
            "body-alias",
            client.indices.get_alias(index="body-idx")["body-idx"]["aliases"],
        )
        named = client.indices.get_alias(index="body-idx", name="body*")
        self.assertEqual(list(named["body-idx"]["aliases"]), ["body-alias"])
        self.assertIn("body-idx", client.indices.get_alias(name="body-alias"))
        with self.assertRaises(NotFoundError):
            client.indices.get_alias(index="body-idx", name="no-such*")
        with self.assertRaises(NotFoundError):
            client.indices.get_alias(name="missing-alias")
        self.assertFalse(client.indices.exists_alias(name="missing-alias"))

        self.assertEqual(
            client.indices.delete_alias(index="body-idx", name="loose")["acknowledged"],
            True,
        )
        with self.assertRaises(NotFoundError):
            client.indices.delete_alias(index="body-idx", name="loose")
        self.assertEqual(
            client.indices.delete_alias(index="body-idx", name="loose", ignore=404)[
                "acknowledged"
            ],
            True,
        )
        self.assertEqual(
            client.indices.delete_alias(index="_all", name="not-there")["acknowledged"],
            True,
        )
        self.assertEqual(
            client.indices.delete_alias(index="*", name="not-there")["acknowledged"],
            True,
        )

        client.indices.create("drop-me")
        self.assertEqual(
            client.indices.update_aliases(
                {
                    "actions": [
                        {"add": {"index": "drop-me", "alias": "temp"}},
                        {"remove": {"index": "drop-me", "alias": "temp"}},
                        {"noop": {}},
                        {"remove_index": {"index": "drop-me"}},
                    ]
                }
            )["acknowledged"],
            True,
        )
        self.assertFalse(client.indices.exists("drop-me"))
        self.assertEqual(
            client.indices.update_aliases('{"actions": []}')["acknowledged"], True
        )

        client.indices.create("logs-000001")
        client.indices.put_alias(index="logs-000001", name="logs")
        client.indices.close("logs-000001")
        with self.assertRaises(NotFoundError):
            client.indices.rollover("logs")
        client.indices.open("logs-000001")
        rolled = client.indices.rollover(
            "logs",
            new_index="logs-000099",
            body={"settings": {"index": {"number_of_shards": "2"}}},
        )
        self.assertEqual(rolled["new_index"], "logs-000099")
        self.assertEqual(rolled["old_index"], "logs-000001")
        with self.assertRaises(NotFoundError):
            client.indices.rollover("missing-alias")

        template_rows = client.cat.templates()
        self.assertIn("low", [row["name"] for row in template_rows])
        self.assertIn("high", [row["name"] for row in template_rows])
        stored = client.indices.get_template("high", doc_type="span")
        self.assertIn("span", stored["high"]["mappings"])
        self.assertEqual(client.indices.get_template("missing", ignore=404), {})
        with self.assertRaises(NotFoundError):
            client.indices.get_template("missing")
        self.assertEqual(
            client.indices.delete_template("missing", ignore=404)["acknowledged"], True
        )
        with self.assertRaises(NotFoundError):
            client.indices.delete_template("missing")
        self.assertEqual(client.indices.delete_template("low")["acknowledged"], True)

        with self.assertRaises(NotFoundError):
            client.indices.put_mapping(
                "span", {"properties": {"b": {"type": "keyword"}}}
            )
        with self.assertRaises(ValueError):
            client.indices.put_mapping(None, {"properties": {}}, index="body-idx")
        self.assertEqual(
            client.indices.put_mapping(
                "span",
                {"properties": {"b": {"type": "keyword"}}},
                index="body-idx",
            )["acknowledged"],
            True,
        )

        with self.assertRaises(NotFoundError):
            client.indices.delete("missing-concrete")
        self.assertEqual(client.indices.delete("missing-concrete", ignore=404), 0)
        self.assertEqual(client.indices.delete("events-0001"), 1)
        self.assertFalse(client.indices.exists("events-0001"))

        client.indices.create("alias-a")
        client.indices.create("alias-b")
        client.indices.create("exact-idx")
        client.indices.create("wild-1")
        client.indices.put_alias(index="alias-a,alias-b", name="both")
        self.assertEqual(client.indices.delete_safe("both"), 2)
        self.assertEqual(client.indices.delete_safe("exact-idx"), 1)
        self.assertEqual(client.indices.delete_safe("wild-*"), 1)
        client.indices.create("star-1")
        client.indices.put_alias(index="star-1", name="star-alias")
        self.assertEqual(client.indices.delete_safe("star-*"), 1)
        self.assertFalse(client.indices.exists("star-1"))

        rows = client.cat.indices(index="body-idx", h=["index", "docs.count"])
        self.assertEqual(rows, [{"index": "body-idx", "docs.count": "0"}])
        templates = client.cat.templates(name="high", h="name,order")
        self.assertEqual(templates, [{"name": "high", "order": "5"}])
        self.assertEqual(client.cat.templates(name="missing"), [])
        self.assertEqual(client.cat.count()[0]["count"], "0")
        client.ingest.put_pipeline("empty", None)
        self.assertEqual(client.memory.pipelines["empty"], {})
        self.assertEqual(
            client.ingest.delete_pipeline("missing", ignore=404)["acknowledged"], True
        )

    def test_scroll_clear_and_reindex(self):
        client = MockElasticsearchPolluxClient()
        client.index("docs", "span", {"n": 1}, id="1")
        client.index("docs", "span", {"n": 2}, id="2")
        first = client.search(
            index="docs",
            doc_type="span",
            scroll="1m",
            size=1,
            body={"query": {"match_all": {}}, "sort": ["_id"]},
        )
        sid = first["_scroll_id"]
        self.assertEqual(len(first["hits"]["hits"]), 1)
        second = client.scroll(body={"scroll_id": sid})
        self.assertEqual(second["_scroll_id"], sid)
        self.assertEqual(len(second["hits"]["hits"]), 1)
        third = client.scroll(scroll_id=sid)
        self.assertEqual(third["hits"]["hits"], [])
        missing = client.scroll(scroll_id="missing", ignore=404)
        self.assertEqual(missing["status"], 404)
        with self.assertRaises(NotFoundError):
            client.scroll(scroll_id="still-missing")
        self.assertEqual(client.clear_scroll(scroll_id=[sid, "other"])["num_freed"], 2)
        self.assertEqual(client.clear_scroll(body={"scroll_id": [sid]})["num_freed"], 1)
        self.assertEqual(client.clear_scroll(body={"_scroll_id": sid})["num_freed"], 1)
        self.assertEqual(client.clear_scroll(body={"scroll_id": 5})["num_freed"], 0)
        self.assertEqual(client.clear_scroll()["num_freed"], 0)

        client.indices.create("dest")
        copied = client.reindex(
            body={
                "source": {"index": "docs", "query": {"term": {"n": 1}}},
                "dest": {"index": "dest"},
            }
        )
        self.assertEqual(copied["created"], 1)
        self.assertEqual(client.get("dest", "span", "1")["_source"], {"n": 1})
        self.assertFalse(
            client.get("dest", "span", "2", ignore=404).get("found", False)
        )

    def test_bulk_errors_and_partial_updates(self):
        client = MockElasticsearchPolluxClient()
        client.indices.create("idx")
        result = client.bulk(
            '{"index": {"_id": "1"}}\n'
            '{"a": 1, "review": "old"}\n'
            '{"delete": {"_id": "missing"}}\n',
            index="idx",
            doc_type="span",
        )
        self.assertTrue(result["errors"])
        self.assertEqual(result["items"][0]["index"]["status"], 201)
        self.assertEqual(result["items"][1]["delete"]["status"], 404)
        self.assertEqual(client.get("idx", "span", "1")["_source"]["a"], 1)

        updated = client.bulk(
            [
                {"create": {"_index": "idx", "_id": "c"}},
                {"a": 1},
                {"update": {"_index": "idx", "_id": "c"}},
                {
                    "doc": {"b": 2},
                    "script": {"source": "ctx._source.a = 5; ctx._source.nested.x = 1"},
                },
                {"create": {"_index": "idx"}},
                {"a": 3},
                {"index": {"_index": "idx", "_id": "1"}},
                {"a": 9},
                {"update": {"_index": "idx", "_id": "c"}},
                {"doc": {"review": "doc-only"}},
                {"update": {"_index": "idx", "_id": "c"}},
                {"script": {"source": "ctx._source.scripted = true"}},
            ]
        )
        self.assertTrue(updated["errors"])
        self.assertEqual(updated["items"][0]["create"]["status"], 201)
        self.assertEqual(updated["items"][1]["update"]["status"], 200)
        self.assertEqual(updated["items"][2]["create"]["status"], 400)
        self.assertEqual(updated["items"][3]["index"]["status"], 200)
        stored = client.get("idx", "span", "c")["_source"]
        self.assertEqual(stored["a"], 5)
        self.assertEqual(stored["b"], 2)
        self.assertEqual(stored["nested"]["x"], 1)
        self.assertEqual(stored["review"], "doc-only")
        self.assertIs(stored["scripted"], True)
        plain = client.mget(body={"ids": ["c"]}, index="idx", doc_type="span")
        self.assertEqual(plain["docs"][0]["_source"]["b"], 2)

        docs = client.mget(
            body={"docs": [{"_id": "c", "_source": ["b", "missing"]}, {"_id": "nope"}]},
            index="idx",
            doc_type="span",
        )
        self.assertEqual(docs["docs"][0]["_source"], {"b": 2})
        self.assertFalse(docs["docs"][1]["found"])

        with self.assertRaises(RequestError):
            client.bulk({"index": {"_index": "idx"}})
        with self.assertRaises(RequestError):
            client.bulk([{"index": {"_index": "idx", "_id": "x"}}])

        self.assertEqual(client.helpers_bulk([]), (0, []))
        self.assertEqual(client.helpers_bulk(None), (0, []))
        success, failed = client.helpers_bulk(
            [{"_op_type": "delete", "_index": "idx", "_id": "missing"}],
            raise_on_error=False,
            stats_only=True,
        )
        self.assertEqual((success, failed), (0, 1))
        success, errors = client.helpers_bulk(
            [{"_op_type": "delete", "_index": "idx", "_id": "missing"}],
            raise_on_error=False,
        )
        self.assertEqual(success, 0)
        self.assertEqual(len(errors), 1)
        with self.assertRaises(BulkIndexError):
            client.helpers_bulk(
                [{"_op_type": "delete", "_index": "idx", "_id": "missing"}]
            )
        success, errors = client.helpers_bulk(['{"hello": "world"}'])
        self.assertEqual(success, 1)
        self.assertEqual(errors, [])
        self.assertIn(None, client.memory.indices)

    def test_sort_identity_and_unknown_metric(self):
        same = MemoryDocument("1", {"k": 1})
        twin = MemoryDocument("1", {"k": 1})
        self.assertEqual(_sort_documents([same, same], ["k"]), [same, same])
        self.assertEqual(
            [doc.id for doc in _sort_documents([same, twin], ["k"])], ["1", "1"]
        )
        self.assertIsNone(_metric("unknown", [same], "k"))

    def test_calendar_floors_keyed_filters_and_unparsed_dates(self):
        client = MockElasticsearchPolluxClient(
            documents={
                "events": {
                    "a": {
                        "ts": "2019-05-15T03:05:05.750Z",
                        "status": "ok",
                    },
                    "b": {"ts": "not-a-date", "status": "ko"},
                }
            }
        )
        found = client.search(
            index="events",
            body={
                "size": 0,
                "aggs": {
                    "seconds": {
                        "date_histogram": {
                            "field": "ts",
                            "calendar_interval": "2s",
                            "min_doc_count": 1,
                        }
                    },
                    "minutes": {
                        "date_histogram": {
                            "field": "ts",
                            "calendar_interval": "2m",
                            "min_doc_count": 1,
                        }
                    },
                    "hours": {
                        "date_histogram": {
                            "field": "ts",
                            "calendar_interval": "2h",
                            "min_doc_count": 1,
                        }
                    },
                    "days": {
                        "date_histogram": {
                            "field": "ts",
                            "calendar_interval": "2d",
                            "min_doc_count": 1,
                        }
                    },
                    "by_status": {
                        "filters": {
                            "filters": {
                                "ok": {"term": {"status": "ok"}},
                                "ko": {"term": {"status": "ko"}},
                            }
                        }
                    },
                    "when": {
                        "date_range": {
                            "field": "ts",
                            "ranges": [
                                {
                                    "key": "may",
                                    "from": "2019-05-15T00:00:00Z",
                                    "to": "2019-05-16T00:00:00Z",
                                }
                            ],
                        }
                    },
                },
            },
        )
        aggs = found["aggregations"]
        self.assertEqual(
            aggs["seconds"]["buckets"][0]["key_as_string"],
            "2019-05-15T03:05:04.000Z",
        )
        self.assertEqual(
            aggs["minutes"]["buckets"][0]["key_as_string"],
            "2019-05-15T03:04:00.000Z",
        )
        self.assertEqual(
            aggs["hours"]["buckets"][0]["key_as_string"],
            "2019-05-15T02:00:00.000Z",
        )
        self.assertEqual(
            aggs["days"]["buckets"][0]["key_as_string"],
            "2019-05-14T00:00:00.000Z",
        )
        self.assertEqual(aggs["by_status"]["buckets"]["ok"]["doc_count"], 1)
        self.assertEqual(aggs["by_status"]["buckets"]["ko"]["doc_count"], 1)
        self.assertEqual(aggs["when"]["buckets"][0]["doc_count"], 1)


if __name__ == "__main__":
    unittest.main()
