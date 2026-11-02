import os
import sys
import tempfile
import unittest
from elasticsearch.exceptions import ConflictError, NotFoundError
from elasticsearch.helpers import scan

BASE_DIRECTORY = os.path.join(os.path.dirname(__file__), "..")  # NOQA
sys.path.insert(0, BASE_DIRECTORY)  # NOQA

from pybana.elastic.mock_elastic_client import (  # noqa: E402
    ElasticMemory,
    MockElasticsearchCatPollux,
    MockElasticsearchIndicePollux,
    MockElasticsearchIngestPollux,
    MockElasticsearchPolluxClient,
)


class TestMockElasticsearchPolluxClient(unittest.TestCase):
    def test_version_is_v8(self):
        client = MockElasticsearchPolluxClient(
            hosts=["http://localhost:9201"], timeout=1
        )
        self.assertIsInstance(client.indices, MockElasticsearchIndicePollux)
        self.assertIsInstance(client.cat, MockElasticsearchCatPollux)
        self.assertIsInstance(client.ingest, MockElasticsearchIngestPollux)
        self.assertEqual(client.version, "8.19.3")
        self.assertEqual(client.version_major, 8)
        self.assertTrue(client._version_is_valid)
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


if __name__ == "__main__":
    unittest.main()
