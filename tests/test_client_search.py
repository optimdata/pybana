import os
import sys
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

os.environ["SKIP_ES"] = "TRUE"

BASE_DIRECTORY = os.path.join(os.path.dirname(__file__), "..")  # NOQA
sys.path.insert(0, BASE_DIRECTORY)  # NOQA

from pybana.elastic.mock_elastic_client import (  # noqa: E402
    ElasticMemory,
    MemoryDocument,
    MockElasticsearchPolluxClient,
    _bucket_start,
    _epoch_millis,
    _eval_date_histogram,
    _histogram_keys,
    _interval_delta,
)


class ClientSearchTest(unittest.TestCase):
    def _aggs(self, documents, aggs, query=None):
        client = MockElasticsearchPolluxClient()
        for doc_id, source in documents.items():
            client.index("events", "_doc", source, id=doc_id)
        body = {"size": 0, "aggs": aggs}
        if query:
            body["query"] = query
        return client.search(index="events", body=body)["aggregations"]

    def _strings(self, agg):
        buckets = agg["buckets"]
        if isinstance(buckets, dict):
            buckets = list(buckets.values())
        return [bucket["key_as_string"] for bucket in buckets]

    def test_date_histogram_calendar_floors(self):
        documents = {"a": {"ts": "2019-05-15T03:05:05.750"}}
        aggs = self._aggs(
            documents,
            {
                "minute": {
                    "date_histogram": {
                        "field": "ts",
                        "calendar_interval": "minute",
                    }
                },
                "hour": {
                    "date_histogram": {"field": "ts", "calendar_interval": "hour"}
                },
                "day": {"date_histogram": {"field": "ts", "calendar_interval": "day"}},
                "week": {
                    "date_histogram": {"field": "ts", "calendar_interval": "week"}
                },
                "month": {
                    "date_histogram": {"field": "ts", "calendar_interval": "month"}
                },
                "quarter": {
                    "date_histogram": {"field": "ts", "calendar_interval": "quarter"}
                },
                "year": {
                    "date_histogram": {"field": "ts", "calendar_interval": "year"}
                },
                "two_seconds": {
                    "date_histogram": {"field": "ts", "calendar_interval": "2s"}
                },
                "two_minutes": {
                    "date_histogram": {"field": "ts", "calendar_interval": "2m"}
                },
                "two_hours": {
                    "date_histogram": {"field": "ts", "calendar_interval": "2h"}
                },
                "two_days": {
                    "date_histogram": {"field": "ts", "calendar_interval": "2d"}
                },
                "two_weeks": {
                    "date_histogram": {"field": "ts", "calendar_interval": "2w"}
                },
                "two_months": {
                    "date_histogram": {"field": "ts", "calendar_interval": "2M"}
                },
                "two_quarters": {
                    "date_histogram": {"field": "ts", "calendar_interval": "2q"}
                },
                "two_years": {
                    "date_histogram": {"field": "ts", "calendar_interval": "2y"}
                },
            },
        )
        self.assertEqual(self._strings(aggs["minute"]), ["2019-05-15T03:05:00.000Z"])
        self.assertEqual(self._strings(aggs["hour"]), ["2019-05-15T03:00:00.000Z"])
        self.assertEqual(self._strings(aggs["day"]), ["2019-05-15T00:00:00.000Z"])
        self.assertEqual(self._strings(aggs["week"]), ["2019-05-13T00:00:00.000Z"])
        self.assertEqual(self._strings(aggs["month"]), ["2019-05-01T00:00:00.000Z"])
        self.assertEqual(self._strings(aggs["quarter"]), ["2019-04-01T00:00:00.000Z"])
        self.assertEqual(self._strings(aggs["year"]), ["2019-01-01T00:00:00.000Z"])
        self.assertEqual(
            self._strings(aggs["two_seconds"]), ["2019-05-15T03:05:04.000Z"]
        )
        self.assertEqual(
            self._strings(aggs["two_minutes"]), ["2019-05-15T03:04:00.000Z"]
        )
        self.assertEqual(self._strings(aggs["two_hours"]), ["2019-05-15T02:00:00.000Z"])
        self.assertEqual(self._strings(aggs["two_days"]), ["2019-05-14T00:00:00.000Z"])
        self.assertEqual(self._strings(aggs["two_weeks"]), ["2019-05-06T00:00:00.000Z"])
        self.assertEqual(self._strings(aggs["two_months"]), ["2019-05-01T00:00:00.000Z"])
        self.assertEqual(
            self._strings(aggs["two_quarters"]), ["2019-01-01T00:00:00.000Z"]
        )
        self.assertEqual(self._strings(aggs["two_years"]), ["2018-01-01T00:00:00.000Z"])

    def test_date_histogram_fixed_intervals_zones_and_invalid_values(self):
        documents = {
            "multi": {
                "ts": [
                    "2019-01-01T03:05:05.750Z",
                    "2019-01-01T03:05:40",
                    "not-a-date",
                    1546311905750,
                    {"bad": 1},
                    True,
                    datetime(2019, 1, 1, 3, 5, 20, tzinfo=timezone.utc),
                ]
            },
            "seconds": {"ts": 1546311905},
            "later": {"ts": "2019-01-01T05:00:00"},
            "invalid": {"ts": "still-not-a-date"},
        }
        aggs = self._aggs(
            documents,
            {
                "paris": {
                    "date_histogram": {
                        "field": "ts",
                        "calendar_interval": "1d",
                        "time_zone": "Europe/Paris",
                    }
                },
                "unknown_zone": {
                    "date_histogram": {
                        "field": "ts",
                        "calendar_interval": "1d",
                        "time_zone": "Not/AZone",
                    }
                },
                "bogus": {
                    "date_histogram": {"field": "ts", "calendar_interval": "bogus"}
                },
                "number_interval": {
                    "date_histogram": {"field": "ts", "calendar_interval": 5}
                },
                "keyed": {
                    "date_histogram": {
                        "field": "ts",
                        "calendar_interval": "1h",
                        "keyed": True,
                    }
                },
                "min_count": {
                    "date_histogram": {
                        "field": "ts",
                        "calendar_interval": "1h",
                        "min_doc_count": 2,
                    }
                },
            },
        )
        fixed = self._aggs(
            {"a": {"ts": "2019-01-01T03:05:05.750Z"}},
            {
                "millis": {
                    "date_histogram": {"field": "ts", "fixed_interval": "500ms"}
                },
                "seconds": {
                    "date_histogram": {"field": "ts", "fixed_interval": "30s"}
                },
                "minutes": {"date_histogram": {"field": "ts", "fixed_interval": "2m"}},
                "days": {"date_histogram": {"field": "ts", "fixed_interval": "1d"}},
            },
        )
        self.assertEqual(fixed["millis"]["buckets"][0]["key"], 1546311905500)
        self.assertEqual(self._strings(fixed["seconds"]), ["2019-01-01T03:05:00.000Z"])
        self.assertEqual(self._strings(fixed["minutes"]), ["2019-01-01T03:04:00.000Z"])
        self.assertEqual(self._strings(fixed["days"]), ["2019-01-01T00:00:00.000Z"])
        self.assertEqual(self._strings(aggs["paris"]), ["2018-12-31T23:00:00.000Z"])
        self.assertEqual(
            self._strings(aggs["unknown_zone"]), ["2019-01-01T00:00:00.000Z"]
        )
        self.assertEqual(aggs["bogus"]["buckets"], [])
        self.assertEqual(aggs["number_interval"]["buckets"], [])
        self.assertIsInstance(aggs["keyed"]["buckets"], dict)
        self.assertEqual(
            aggs["keyed"]["buckets"]["2019-01-01T03:00:00.000Z"]["doc_count"], 2
        )
        self.assertEqual(
            [bucket["doc_count"] for bucket in aggs["min_count"]["buckets"]], [2]
        )

        with patch("zoneinfo.ZoneInfo", side_effect=Exception("zoneinfo unavailable")):
            via_pytz = self._aggs(
                {"a": {"ts": "2019-01-01T03:05:05.750Z"}},
                {
                    "paris": {
                        "date_histogram": {
                            "field": "ts",
                            "calendar_interval": "1d",
                            "time_zone": "Europe/Paris",
                        }
                    }
                },
            )
        self.assertEqual(self._strings(via_pytz["paris"]), ["2018-12-31T23:00:00.000Z"])

    def test_date_histogram_gaps_zero_step_and_bucket_cap(self):
        hours = self._aggs(
            {
                "a": {"ts": "2019-01-01T00:30:00"},
                "b": {"ts": "2019-01-01T02:30:00"},
            },
            {"by_hour": {"date_histogram": {"field": "ts", "calendar_interval": "1h"}}},
        )
        self.assertEqual(
            [
                (bucket["key_as_string"], bucket["doc_count"])
                for bucket in hours["by_hour"]["buckets"]
            ],
            [
                ("2019-01-01T00:00:00.000Z", 1),
                ("2019-01-01T01:00:00.000Z", 0),
                ("2019-01-01T02:00:00.000Z", 1),
            ],
        )

        weeks = self._aggs(
            {
                "a": {"ts": "2019-01-01T00:00:00"},
                "b": {"ts": "2019-01-20T00:00:00"},
            },
            {"by_week": {"date_histogram": {"field": "ts", "calendar_interval": "1w"}}},
        )
        self.assertEqual(
            self._strings(weeks["by_week"]),
            [
                "2018-12-31T00:00:00.000Z",
                "2019-01-07T00:00:00.000Z",
                "2019-01-14T00:00:00.000Z",
            ],
        )

        months = self._aggs(
            {
                "a": {"ts": "2019-01-15T00:00:00"},
                "b": {"ts": "2019-03-15T00:00:00"},
            },
            {
                "by_month": {
                    "date_histogram": {"field": "ts", "calendar_interval": "1M"}
                }
            },
        )
        self.assertEqual(
            [
                (bucket["key_as_string"], bucket["doc_count"])
                for bucket in months["by_month"]["buckets"]
            ],
            [
                ("2019-01-01T00:00:00.000Z", 1),
                ("2019-02-01T00:00:00.000Z", 0),
                ("2019-03-01T00:00:00.000Z", 1),
            ],
        )

        quarters = self._aggs(
            {
                "a": {"ts": "2019-02-01T00:00:00"},
                "b": {"ts": "2019-08-01T00:00:00"},
            },
            {
                "by_quarter": {
                    "date_histogram": {"field": "ts", "calendar_interval": "1q"}
                }
            },
        )
        self.assertEqual(
            self._strings(quarters["by_quarter"]),
            [
                "2019-01-01T00:00:00.000Z",
                "2019-04-01T00:00:00.000Z",
                "2019-07-01T00:00:00.000Z",
            ],
        )

        years = self._aggs(
            {
                "a": {"ts": "2019-06-01T00:00:00"},
                "b": {"ts": "2021-01-15T00:00:00"},
            },
            {"by_year": {"date_histogram": {"field": "ts", "calendar_interval": "1y"}}},
        )
        self.assertEqual(
            [
                (bucket["key_as_string"], bucket["doc_count"])
                for bucket in years["by_year"]["buckets"]
            ],
            [
                ("2019-01-01T00:00:00.000Z", 1),
                ("2020-01-01T00:00:00.000Z", 0),
                ("2021-01-01T00:00:00.000Z", 1),
            ],
        )

        stuck = self._aggs(
            {
                "a": {"ts": "2019-01-01T00:00:05"},
                "b": {"ts": "2019-01-01T00:00:09"},
            },
            {"by_zero": {"date_histogram": {"field": "ts", "calendar_interval": "0s"}}},
        )
        self.assertEqual(
            self._strings(stuck["by_zero"]), ["2019-01-01T00:00:05.000Z"]
        )

        capped = self._aggs(
            {"a": {"ts": 1546300800000}, "b": {"ts": 1546300815000}},
            {"by_ms": {"date_histogram": {"field": "ts", "fixed_interval": "1ms"}}},
        )
        self.assertEqual(
            [bucket["key"] for bucket in capped["by_ms"]["buckets"]],
            [1546300800000, 1546300815000],
        )

    def test_histogram_ranges_filters_and_percentiles(self):
        documents = {
            "a": {
                "n": 1,
                "s": "a",
                "status": "ok",
                "tags": ["red", "red"],
                "flag": True,
                "mixed": [1, True, 1],
                "ts": "2019-01-01T12:00:00",
                "only": 7,
            },
            "b": {
                "n": 3,
                "s": "b",
                "status": "ok",
                "tags": ["blue"],
                "ts": "2019-01-02T00:00:00",
            },
            "c": {
                "n": 10,
                "s": "c",
                "status": "ko",
                "tags": ["red", "blue"],
                "ts": "nope",
            },
        }
        aggs = self._aggs(
            documents,
            {
                "hist": {
                    "histogram": {"field": "n", "interval": 5, "offset": 1},
                    "aggs": {"avg_n": {"avg": {"field": "n"}}},
                },
                "mixed": {"histogram": {"field": "mixed", "interval": 10}},
                "bad": {"histogram": {"field": "n", "interval": "nope"}},
                "zero": {"histogram": {"field": "n", "interval": -1}},
                "missing_interval": {"histogram": {"field": "n"}},
                "fixed": {"histogram": {"field": "n", "fixed_interval": "5"}},
                "nums": {
                    "range": {
                        "field": "n",
                        "ranges": [{"to": 4}, {"from": 0, "to": 4}, {"from": True}],
                    }
                },
                "labels": {
                    "range": {
                        "field": "s",
                        "ranges": [
                            {"from": None},
                            {"from": True},
                            {"from": "a", "to": "z"},
                            "nope",
                        ],
                    }
                },
                "flags": {"range": {"field": "flag", "ranges": [{"from": 0}]}},
                "dates": {
                    "date_range": {
                        "field": "ts",
                        "ranges": [
                            {
                                "from": "2019-01-01T00:00:00",
                                "to": "2019-01-02T00:00:00",
                            },
                            {"from": "2019-01-02T00:00:00"},
                            {"key": "open"},
                            "nope",
                        ],
                    }
                },
                "by_filter": {
                    "filters": {
                        "filters": {
                            "ok": {"term": {"status": "ok"}},
                            "none": {"match_none": {}},
                        }
                    },
                    "aggs": {"avg_n": {"avg": {"field": "n"}}},
                },
                "by_list": {"filters": {"filters": [{"term": {"status": "ko"}}]}},
                "by_flat": {
                    "filters": {
                        "filters": {"ko": {"term": {"status": "ko"}}},
                        "keyed": False,
                    }
                },
                "one": {"percentiles": {"field": "only", "percents": [50]}},
                "empty_pct": {"percentiles": {"field": "flag", "percents": [50]}},
                "empty_stats": {"extended_stats": {"field": "flag"}},
                "ranks": {
                    "percentile_ranks": {"field": "n", "values": [3, 0, 100]}
                },
                "empty_ranks": {"percentile_ranks": {"field": "missing", "values": [1]}},
                "tags": {"terms": {"field": "tags"}},
                "no_field": {"terms": {}},
            },
        )
        self.assertEqual(
            [(bucket["key"], bucket["doc_count"]) for bucket in aggs["hist"]["buckets"]],
            [(1.0, 2), (6.0, 1)],
        )
        self.assertEqual(aggs["hist"]["buckets"][0]["avg_n"]["value"], 2)
        self.assertEqual(aggs["mixed"]["buckets"], [{"key": 0.0, "doc_count": 1}])
        self.assertEqual(aggs["bad"]["buckets"], [])
        self.assertEqual(aggs["zero"]["buckets"], [])
        self.assertEqual(aggs["missing_interval"]["buckets"], [])
        self.assertEqual(
            [bucket["doc_count"] for bucket in aggs["fixed"]["buckets"]], [2, 1]
        )
        self.assertEqual(
            [(bucket["key"], bucket["doc_count"]) for bucket in aggs["nums"]["buckets"]],
            [("*-4.0", 2), ("0.0-4.0", 2), ("True-*", 3)],
        )
        self.assertEqual(
            [bucket["key"] for bucket in aggs["labels"]["buckets"]],
            ["*-*", "True-*", "a-z"],
        )
        self.assertEqual(aggs["flags"]["buckets"][0]["doc_count"], 0)
        self.assertEqual(
            [(bucket["key"], bucket["doc_count"]) for bucket in aggs["dates"]["buckets"]],
            [
                ("2019-01-01T00:00:00-2019-01-02T00:00:00", 1),
                ("2019-01-02T00:00:00-*", 1),
                ("open", 2),
            ],
        )
        self.assertEqual(aggs["by_filter"]["buckets"]["ok"]["doc_count"], 2)
        self.assertEqual(aggs["by_filter"]["buckets"]["ok"]["avg_n"]["value"], 2)
        self.assertEqual(aggs["by_filter"]["buckets"]["none"]["doc_count"], 0)
        self.assertIsNone(aggs["by_filter"]["buckets"]["none"]["avg_n"]["value"])
        self.assertEqual(aggs["by_list"]["buckets"], [{"key": "0", "doc_count": 1}])
        self.assertEqual(aggs["by_flat"]["buckets"], [{"key": "ko", "doc_count": 1}])
        self.assertEqual(aggs["one"]["values"]["50.0"], 7)
        self.assertIsNone(aggs["empty_pct"]["values"]["50.0"])
        self.assertEqual(aggs["empty_stats"]["count"], 0)
        self.assertIsNone(aggs["empty_stats"]["min"])
        self.assertAlmostEqual(aggs["ranks"]["values"]["3.0"], 200.0 / 3)
        self.assertEqual(aggs["ranks"]["values"]["0.0"], 0)
        self.assertEqual(aggs["ranks"]["values"]["100.0"], 100)
        self.assertIsNone(aggs["empty_ranks"]["values"]["1.0"])
        self.assertEqual(
            {bucket["key"]: bucket["doc_count"] for bucket in aggs["tags"]["buckets"]},
            {"red": 2, "blue": 2},
        )
        self.assertEqual(aggs["no_field"]["buckets"], [])

    def test_pipeline_top_hits_top_metrics_and_rate(self):
        documents = {
            "a": {"n": 1, "s": "a", "status": "ok", "ts": "2019-01-01T00:30:00"},
            "b": {"n": 3, "s": "b", "status": "ok", "ts": "2019-01-01T01:30:00"},
            "c": {"n": 10, "s": "c", "status": "ko", "ts": "2019-01-01T01:45:00"},
        }
        aggs = self._aggs(
            documents,
            {
                "by_status": {
                    "terms": {"field": "status"},
                    "aggs": {
                        "avg_n": {"avg": {"field": "n"}},
                        "stats": {"extended_stats": {"field": "n"}},
                        "pct": {"percentiles": {"field": "n", "percents": [50]}},
                        "pct_missing": {
                            "percentiles": {"field": "absent", "percents": [50]}
                        },
                        "nested": {"filter": {"match_all": {}}},
                    },
                },
                "avg_n": {"avg": {"field": "n"}},
                "min_count": {"min_bucket": {"buckets_path": "by_status>_count"}},
                "max_count": {"max_bucket": {"buckets_path": "by_status>_count"}},
                "sum_count": {"sum_bucket": {"buckets_path": "by_status>_count"}},
                "max_avg": {"max_bucket": {"buckets_path": "by_status>avg_n"}},
                "max_std": {"max_bucket": {"buckets_path": "by_status>stats"}},
                "min_pct": {"min_bucket": {"buckets_path": "by_status>pct"}},
                "avg_nested": {"avg_bucket": {"buckets_path": "by_status>nested"}},
                "absent_metric": {"avg_bucket": {"buckets_path": "by_status>absent"}},
                "from_empty_pct": {
                    "max_bucket": {"buckets_path": "by_status>pct_missing"}
                },
                "sum_avg": {"sum_bucket": {"buckets_path": "avg_n"}},
                "nobody": {"terms": {"field": "absent"}},
                "sum_nobody": {"sum_bucket": {"buckets_path": "nobody>_count"}},
                "avg_nobody": {"avg_bucket": {"buckets_path": "nobody>avg_n"}},
                "missing": {"avg_bucket": {"buckets_path": "missing>avg_n"}},
                "not_text": {"sum_bucket": {"buckets_path": 5}},
                "as_dict": {
                    "sum_bucket": {"buckets_path": {"count": "by_status>_count"}}
                },
                "only_ok": {"filter": {"term": {"status": "ok"}}},
                "sum_ok": {"sum_bucket": {"buckets_path": "only_ok"}},
                "hours": {
                    "date_histogram": {
                        "field": "ts",
                        "calendar_interval": "1h",
                        "keyed": True,
                    }
                },
                "sum_hours": {"sum_bucket": {"buckets_path": "hours>_count"}},
                "top_default": {"top_hits": {"size": 1, "sort": {"n": "desc"}}},
                "top_false": {
                    "top_hits": {
                        "size": 1,
                        "sort": {"n": "desc"},
                        "_source": False,
                    }
                },
                "top_none": {
                    "top_hits": {"size": 1, "sort": {"n": "desc"}, "_source": None}
                },
                "top_list": {
                    "top_hits": {
                        "size": 1,
                        "sort": {"n": "desc"},
                        "_source": ["s"],
                    }
                },
                "top_includes": {
                    "top_hits": {
                        "size": 1,
                        "sort": {"n": "desc"},
                        "_source": {"includes": ["n"]},
                    }
                },
                "top_empty_includes": {
                    "top_hits": {
                        "size": 1,
                        "sort": {"n": "desc"},
                        "_source": {},
                    }
                },
                "top_other": {
                    "top_hits": {"size": 1, "sort": {"n": "desc"}, "_source": 5}
                },
                "metrics": {
                    "top_metrics": {
                        "size": 2,
                        "sort": {"n": "desc"},
                        "metrics": {"field": "n"},
                    }
                },
                "metrics_plain": {"top_metrics": {"metrics": ["n"]}},
                "rate_n": {"rate": {"field": "n"}},
            },
        )
        self.assertEqual(aggs["min_count"]["value"], 1)
        self.assertEqual(aggs["max_count"]["value"], 2)
        self.assertEqual(aggs["sum_count"]["value"], 3)
        self.assertEqual(aggs["max_avg"]["value"], 10)
        self.assertGreater(aggs["max_std"]["value"], 0)
        self.assertEqual(aggs["min_pct"]["value"], 2)
        self.assertIsNone(aggs["avg_nested"]["value"])
        self.assertIsNone(aggs["absent_metric"]["value"])
        self.assertIsNone(aggs["from_empty_pct"]["value"])
        self.assertEqual(aggs["sum_avg"]["value"], 0)
        self.assertEqual(aggs["sum_nobody"]["value"], 0)
        self.assertIsNone(aggs["avg_nobody"]["value"])
        self.assertIsNone(aggs["missing"]["value"])
        self.assertEqual(aggs["not_text"]["value"], 0)
        self.assertEqual(aggs["as_dict"]["value"], 3)
        self.assertEqual(aggs["sum_ok"]["value"], 2)
        self.assertEqual(aggs["sum_hours"]["value"], 3)
        self.assertIsInstance(aggs["hours"]["buckets"], dict)
        top = aggs["top_default"]["hits"]["hits"][0]
        self.assertEqual(top["_source"]["s"], "c")
        self.assertNotIn("_source", aggs["top_false"]["hits"]["hits"][0])
        self.assertEqual(aggs["top_none"]["hits"]["hits"][0]["_source"]["n"], 10)
        self.assertEqual(aggs["top_list"]["hits"]["hits"][0]["_source"], {"s": "c"})
        self.assertEqual(aggs["top_includes"]["hits"]["hits"][0]["_source"], {"n": 10})
        self.assertEqual(aggs["top_empty_includes"]["hits"]["hits"][0]["_source"], {})
        self.assertEqual(aggs["top_other"]["hits"]["hits"][0]["_source"]["n"], 10)
        self.assertEqual(
            aggs["metrics"]["top"],
            [{"metrics": {"n": 10}}, {"metrics": {"n": 3}}],
        )
        self.assertEqual(aggs["metrics_plain"]["top"][0]["metrics"], {})
        self.assertIsNone(aggs["rate_n"]["value"])

    def test_search_aggregations_use_the_query(self):
        aggs = self._aggs(
            {
                "a": {"status": "ok", "n": 1},
                "b": {"status": "ko", "n": 9},
            },
            {"by_status": {"terms": {"field": "status"}}},
            query={"term": {"status": "ok"}},
        )
        self.assertEqual(
            [(bucket["key"], bucket["doc_count"]) for bucket in aggs["by_status"]["buckets"]],
            [("ok", 1)],
        )

    def test_bulk_operation_error_fallbacks(self):
        client = MockElasticsearchPolluxClient()
        with patch.object(client, "delete", return_value=None):
            skipped = client.bulk([{"delete": {"_index": "events", "_id": "1"}}])
        self.assertEqual(
            skipped["items"], [{"delete": {"status": 500, "result": "error"}}]
        )
        with patch.object(client, "index", side_effect=ValueError("bad document")):
            failed = client.bulk(
                [{"index": {"_index": "events", "_id": "1"}}, {"n": 1}]
            )
        self.assertEqual(failed["items"][0]["index"]["status"], 400)
        self.assertEqual(
            failed["items"][0]["index"]["error"]["type"],
            "illegal_argument_exception",
        )
        with patch.object(client, "get", return_value={"found": False}):
            conflicted = client.bulk(
                [{"update": {"_index": "events", "_id": "missing"}}, {"doc": {"a": 1}}]
            )
        self.assertEqual(conflicted["items"][0]["update"]["status"], 409)

        client.indices.create("events-1")
        self.assertEqual(client.indices.delete_safe("events-1,missing"), 0)
        self.assertTrue(client.indices.exists("events-1"))


class MockSearchHelperTest(unittest.TestCase):
    def test_histogram_helpers_for_intervals_search_rewrites(self):
        # Search rewrites date_histogram "interval" before evaluation, so the
        # fallback and the empty-delta guards are exercised directly.
        doc = MemoryDocument("1", {"ts": "2019-01-01T00:30:00"})
        result = _eval_date_histogram([doc], {"field": "ts", "interval": "1h"}, None)
        self.assertEqual(result["buckets"][0]["key_as_string"], "2019-01-01T00:00:00.000Z")
        self.assertIsNone(_interval_delta(1, "x"))
        start = datetime(2019, 1, 1, tzinfo=timezone.utc)
        self.assertEqual(
            _bucket_start(start, 1, "x", timezone.utc, False),
            start.replace(microsecond=0),
        )
        naive = datetime(2019, 1, 1)
        self.assertEqual(
            _epoch_millis(naive),
            _epoch_millis(naive.replace(tzinfo=timezone.utc)),
        )
        self.assertEqual(_histogram_keys([], start, 1, "h"), [])
        memory = ElasticMemory()
        self.assertEqual(memory.next_seq(), 1)
        self.assertEqual(memory.next_seq(), 2)


if __name__ == "__main__":
    unittest.main()
