import os
import sys
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

BASE_DIRECTORY = os.path.join(os.path.dirname(__file__), "..")  # NOQA
sys.path.insert(0, BASE_DIRECTORY)  # NOQA

from pybana.elastic.elastic_client_interface import ESClientInterface  # noqa: E402
from tests.connection_pool import get_allowed_versions, ELASTIC_CLIENTS  # noqa: E402
from parameterized import parameterized  # noqa: E402
from pybana.elastic.mock_elastic_client import (  # noqa: E402
    ElasticMemory,
    MemoryDocument,
    _bucket_start,
    _epoch_millis,
    _eval_date_histogram,
    _histogram_keys,
    _interval_delta,
)


class ClientSearchTest(unittest.TestCase):
    def _aggs(
        self, client: ESClientInterface, index_name: str, documents, aggs, query=None
    ):
        client.indices.delete_safe(index_name)
        self.assertFalse(client.indices.exists(index_name))
        # `_doc` is the Elasticsearch 6 mapping type. The v7+ client lifts it out.
        client.indices.create(
            index=index_name,
            body={
                "mappings": {
                    "_doc": {
                        "properties": {"ts": {"type": "date"}},
                        "dynamic_templates": [
                            {
                                "strings_to_keywords": {
                                    "match_mapping_type": "string",
                                    "mapping": {"type": "keyword"},
                                }
                            }
                        ],
                    }
                }
            },
        )
        for doc_id, source in documents.items():
            client.index(index_name, "_doc", source, id=doc_id)
        client.indices.refresh(index_name)
        body = {"size": 0, "aggs": aggs}
        if query:
            body["query"] = query
        return client.search(index=index_name, body=body)["aggregations"]

    def _bucket_list(self, agg):
        buckets = agg["buckets"]
        if isinstance(buckets, dict):
            return list(buckets.values())
        return list(buckets)

    def _strings(self, agg):
        buckets = agg["buckets"]
        if isinstance(buckets, dict):
            buckets = list(buckets.values())
        return [bucket["key_as_string"] for bucket in buckets]

    @parameterized.expand(get_allowed_versions())
    def test_date_histogram_calendar_floors(self, version):
        es_client = ELASTIC_CLIENTS[version]
        index_name = "test_date_histogram_calendar_floors"
        documents = {"a": {"ts": "2019-05-15T03:05:05.750"}}
        aggs = self._aggs(
            es_client,
            index_name,
            documents,
            {
                "minute": {
                    "date_histogram": {
                        "field": "ts",
                        "interval": "minute",
                        "min_doc_count": 1,
                    }
                },
                "hour": {
                    "date_histogram": {
                        "field": "ts",
                        "interval": "hour",
                        "min_doc_count": 1,
                    }
                },
                "day": {
                    "date_histogram": {
                        "field": "ts",
                        "interval": "day",
                        "min_doc_count": 1,
                    }
                },
                "week": {
                    "date_histogram": {
                        "field": "ts",
                        "interval": "week",
                        "min_doc_count": 1,
                    }
                },
                "month": {
                    "date_histogram": {
                        "field": "ts",
                        "interval": "month",
                        "min_doc_count": 1,
                    }
                },
                "quarter": {
                    "date_histogram": {
                        "field": "ts",
                        "interval": "quarter",
                        "min_doc_count": 1,
                    }
                },
                "year": {
                    "date_histogram": {
                        "field": "ts",
                        "interval": "year",
                        "min_doc_count": 1,
                    }
                },
                "two_seconds": {
                    "date_histogram": {
                        "field": "ts",
                        "interval": "2s",
                        "min_doc_count": 1,
                    }
                },
                "two_minutes": {
                    "date_histogram": {
                        "field": "ts",
                        "interval": "2m",
                        "min_doc_count": 1,
                    }
                },
                "two_hours": {
                    "date_histogram": {
                        "field": "ts",
                        "interval": "2h",
                        "min_doc_count": 1,
                    }
                },
                "two_days": {
                    "date_histogram": {
                        "field": "ts",
                        "interval": "2d",
                        "min_doc_count": 1,
                    }
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
        if version == "mock":
            # 2w, 2M, 2q and 2y are rejected by Elasticsearch 6 and 8.
            calendar = self._aggs(
                es_client,
                index_name,
                documents,
                {
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
            self.assertEqual(
                self._strings(calendar["two_weeks"]), ["2019-05-06T00:00:00.000Z"]
            )
            self.assertEqual(
                self._strings(calendar["two_months"]), ["2019-05-01T00:00:00.000Z"]
            )
            self.assertEqual(
                self._strings(calendar["two_quarters"]), ["2019-01-01T00:00:00.000Z"]
            )
            self.assertEqual(
                self._strings(calendar["two_years"]), ["2018-01-01T00:00:00.000Z"]
            )

    @parameterized.expand(get_allowed_versions())
    def test_date_histogram_fixed_intervals_zones_and_invalid_values(self, version):
        es_client = ELASTIC_CLIENTS[version]
        index_name = "test_date_histogram_fixed_intervals_zones_and_invalid_values"
        documents = {
            "on_hour": {"ts": "2019-01-01T03:05:40Z"},
            "later": {"ts": "2019-01-01T05:00:00Z"},
            "millis": {"ts": "2019-01-01T03:05:05.750Z"},
        }
        aggs = self._aggs(
            es_client,
            index_name,
            documents,
            {
                "paris": {
                    "date_histogram": {
                        "field": "ts",
                        "interval": "1d",
                        "time_zone": "Europe/Paris",
                    }
                },
                "keyed": {
                    "date_histogram": {
                        "field": "ts",
                        "interval": "1h",
                        "keyed": True,
                    }
                },
                "min_count": {
                    "date_histogram": {
                        "field": "ts",
                        "interval": "1h",
                        "min_doc_count": 2,
                    }
                },
            },
        )
        fixed = self._aggs(
            es_client,
            index_name,
            {"a": {"ts": "2019-01-01T03:05:05.750Z"}},
            {
                "millis": {
                    "date_histogram": {
                        "field": "ts",
                        "interval": "500ms",
                        "min_doc_count": 1,
                    }
                },
                "seconds": {
                    "date_histogram": {
                        "field": "ts",
                        "interval": "30s",
                        "min_doc_count": 1,
                    }
                },
                "minutes": {
                    "date_histogram": {
                        "field": "ts",
                        "interval": "2m",
                        "min_doc_count": 1,
                    }
                },
                "days": {
                    "date_histogram": {
                        "field": "ts",
                        "interval": "1d",
                        "min_doc_count": 1,
                    }
                },
            },
        )
        self.assertEqual(fixed["millis"]["buckets"][0]["key"], 1546311905500)
        self.assertEqual(self._strings(fixed["seconds"]), ["2019-01-01T03:05:00.000Z"])
        self.assertEqual(self._strings(fixed["minutes"]), ["2019-01-01T03:04:00.000Z"])
        self.assertEqual(self._strings(fixed["days"]), ["2019-01-01T00:00:00.000Z"])
        # Same UTC instant; Elasticsearch prints the zone offset, the mock prints Z.
        self.assertEqual(aggs["paris"]["buckets"][0]["key"], 1546297200000)
        self.assertEqual(aggs["paris"]["buckets"][0]["doc_count"], 3)
        self.assertIsInstance(aggs["keyed"]["buckets"], dict)
        self.assertEqual(
            aggs["keyed"]["buckets"]["2019-01-01T03:00:00.000Z"]["doc_count"], 2
        )
        self.assertEqual(
            [bucket["doc_count"] for bucket in aggs["min_count"]["buckets"]], [2]
        )

        with patch("zoneinfo.ZoneInfo", side_effect=Exception("zoneinfo unavailable")):
            via_pytz = self._aggs(
                es_client,
                index_name,
                {"a": {"ts": "2019-01-01T03:05:05.750Z"}},
                {
                    "paris": {
                        "date_histogram": {
                            "field": "ts",
                            "interval": "1d",
                            "time_zone": "Europe/Paris",
                        }
                    }
                },
            )
        self.assertEqual(via_pytz["paris"]["buckets"][0]["key"], 1546297200000)
        if version == "mock":
            invalid = self._aggs(
                es_client,
                index_name,
                {
                    "multi": {
                        "ts": [
                            "2019-01-01T03:05:05.750Z",
                            "not-a-date",
                            {"bad": 1},
                            True,
                            datetime(2019, 1, 1, 3, 5, 20, tzinfo=timezone.utc),
                        ]
                    }
                },
                {
                    "unknown_zone": {
                        "date_histogram": {
                            "field": "ts",
                            "interval": "1d",
                            "time_zone": "Not/AZone",
                        }
                    },
                    "bogus": {
                        "date_histogram": {"field": "ts", "calendar_interval": "bogus"}
                    },
                    "number_interval": {
                        "date_histogram": {"field": "ts", "calendar_interval": 5}
                    },
                },
            )
            self.assertEqual(
                self._strings(invalid["unknown_zone"]), ["2019-01-01T00:00:00.000Z"]
            )
            self.assertEqual(invalid["bogus"]["buckets"], [])
            self.assertEqual(invalid["number_interval"]["buckets"], [])

    @parameterized.expand(get_allowed_versions())
    def test_date_histogram_gaps_zero_step_and_bucket_cap(self, version):
        es_client = ELASTIC_CLIENTS[version]
        index_name = "test_date_histogram_gaps_zero_step_and_bucket_cap"
        hours = self._aggs(
            es_client,
            index_name,
            {
                "a": {"ts": "2019-01-01T00:30:00"},
                "b": {"ts": "2019-01-01T02:30:00"},
            },
            {"by_hour": {"date_histogram": {"field": "ts", "interval": "1h"}}},
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
            es_client,
            index_name,
            {
                "a": {"ts": "2019-01-01T00:00:00"},
                "b": {"ts": "2019-01-20T00:00:00"},
            },
            {"by_week": {"date_histogram": {"field": "ts", "interval": "1w"}}},
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
            es_client,
            index_name,
            {
                "a": {"ts": "2019-01-15T00:00:00"},
                "b": {"ts": "2019-03-15T00:00:00"},
            },
            {"by_month": {"date_histogram": {"field": "ts", "interval": "1M"}}},
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
            es_client,
            index_name,
            {
                "a": {"ts": "2019-02-01T00:00:00"},
                "b": {"ts": "2019-08-01T00:00:00"},
            },
            {"by_quarter": {"date_histogram": {"field": "ts", "interval": "1q"}}},
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
            es_client,
            index_name,
            {
                "a": {"ts": "2019-06-01T00:00:00"},
                "b": {"ts": "2021-01-15T00:00:00"},
            },
            {"by_year": {"date_histogram": {"field": "ts", "interval": "1y"}}},
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

        if version == "mock":
            # A zero interval and a 1ms span past the bucket cap are mock behavior.
            stuck = self._aggs(
                es_client,
                index_name,
                {
                    "a": {"ts": "2019-01-01T00:00:05"},
                    "b": {"ts": "2019-01-01T00:00:09"},
                },
                {
                    "by_zero": {
                        "date_histogram": {"field": "ts", "calendar_interval": "0s"}
                    }
                },
            )
            self.assertEqual(
                self._strings(stuck["by_zero"]), ["2019-01-01T00:00:05.000Z"]
            )
            capped = self._aggs(
                es_client,
                index_name,
                {"a": {"ts": 1546300800000}, "b": {"ts": 1546300815000}},
                {"by_ms": {"date_histogram": {"field": "ts", "fixed_interval": "1ms"}}},
            )
            self.assertEqual(
                [bucket["key"] for bucket in capped["by_ms"]["buckets"]],
                [1546300800000, 1546300815000],
            )

    @parameterized.expand(get_allowed_versions())
    def test_histogram_ranges_filters_and_percentiles(self, version):
        es_client = ELASTIC_CLIENTS[version]
        index_name = "test_histogram_ranges_filters_and_percentiles"
        documents = {
            "a": {
                "n": 1,
                "s": "a",
                "status": "ok",
                "tags": ["red", "red"],
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
            },
        }
        aggs = self._aggs(
            es_client,
            index_name,
            documents,
            {
                "hist": {
                    "histogram": {"field": "n", "interval": 5, "offset": 1},
                    "aggs": {"avg_n": {"avg": {"field": "n"}}},
                },
                "nums": {
                    "range": {
                        "field": "n",
                        "ranges": [{"to": 4}, {"from": 0, "to": 4}],
                    }
                },
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
                        ],
                    }
                },
                "by_filter": {
                    "filters": {
                        "filters": [
                            {"term": {"status": "ok"}},
                            {"match_none": {}},
                        ]
                    },
                    "aggs": {"avg_n": {"avg": {"field": "n"}}},
                },
                "by_list": {"filters": {"filters": [{"term": {"status": "ko"}}]}},
                "one": {"percentiles": {"field": "only", "percents": [50]}},
                "ranks": {"percentile_ranks": {"field": "n", "values": [3, 0, 100]}},
                "tags": {"terms": {"field": "tags"}},
            },
        )
        self.assertEqual(
            [
                (bucket["key"], bucket["doc_count"])
                for bucket in aggs["hist"]["buckets"]
            ],
            [(1.0, 2), (6.0, 1)],
        )
        self.assertEqual(aggs["hist"]["buckets"][0]["avg_n"]["value"], 2)
        self.assertEqual(
            [
                (bucket["key"], bucket["doc_count"])
                for bucket in aggs["nums"]["buckets"]
            ],
            [("*-4.0", 2), ("0.0-4.0", 2)],
        )
        date_buckets = self._bucket_list(aggs["dates"])
        self.assertEqual(
            next(
                bucket["doc_count"]
                for bucket in date_buckets
                if bucket.get("key") == "open"
            ),
            2,
        )
        self.assertEqual(
            sorted(bucket["doc_count"] for bucket in date_buckets), [1, 1, 2]
        )
        self.assertEqual(aggs["by_filter"]["buckets"][0]["doc_count"], 2)
        self.assertEqual(aggs["by_filter"]["buckets"][0]["avg_n"]["value"], 2)
        self.assertEqual(aggs["by_filter"]["buckets"][1]["doc_count"], 0)
        self.assertIsNone(aggs["by_filter"]["buckets"][1]["avg_n"]["value"])
        self.assertEqual(aggs["by_list"]["buckets"][0]["doc_count"], 1)
        self.assertEqual(aggs["one"]["values"]["50.0"], 7)
        self.assertEqual(aggs["ranks"]["values"]["0.0"], 0)
        self.assertEqual(aggs["ranks"]["values"]["100.0"], 100)
        self.assertGreater(aggs["ranks"]["values"]["3.0"], 0)
        self.assertLess(aggs["ranks"]["values"]["3.0"], 100)
        self.assertEqual(
            {bucket["key"]: bucket["doc_count"] for bucket in aggs["tags"]["buckets"]},
            {"red": 2, "blue": 2},
        )
        if version == "mock":
            edged = self._aggs(
                es_client,
                index_name,
                {
                    "a": {
                        "n": 1,
                        "s": "a",
                        "flag": True,
                        "mixed": [1, True, 1],
                        "tags": ["red", "red"],
                    }
                },
                {
                    "mixed": {"histogram": {"field": "mixed", "interval": 10}},
                    "bad": {"histogram": {"field": "n", "interval": "nope"}},
                    "zero": {"histogram": {"field": "n", "interval": -1}},
                    "missing_interval": {"histogram": {"field": "n"}},
                    "fixed": {"histogram": {"field": "n", "fixed_interval": "5"}},
                    "nums": {
                        "range": {
                            "field": "n",
                            "ranges": [{"from": True}, "nope"],
                        }
                    },
                    "labels": {
                        "range": {
                            "field": "s",
                            "ranges": [
                                {"from": None},
                                {"from": True},
                                {"from": "a", "to": "z"},
                            ],
                        }
                    },
                    "flags": {"range": {"field": "flag", "ranges": [{"from": 0}]}},
                    "no_field": {"terms": {}},
                    "empty_pct": {"percentiles": {"field": "flag", "percents": [50]}},
                    "empty_stats": {"extended_stats": {"field": "flag"}},
                    "empty_ranks": {
                        "percentile_ranks": {"field": "missing", "values": [1]}
                    },
                },
            )
            self.assertEqual(edged["mixed"]["buckets"], [{"key": 0.0, "doc_count": 1}])
            self.assertEqual(edged["bad"]["buckets"], [])
            self.assertEqual(edged["zero"]["buckets"], [])
            self.assertEqual(edged["missing_interval"]["buckets"], [])
            self.assertEqual(edged["fixed"]["buckets"][0]["doc_count"], 1)
            self.assertEqual(edged["nums"]["buckets"][0]["key"], "True-*")
            self.assertEqual(
                [bucket["key"] for bucket in edged["labels"]["buckets"]],
                ["*-*", "True-*", "a-z"],
            )
            self.assertEqual(edged["flags"]["buckets"][0]["doc_count"], 0)
            self.assertEqual(edged["no_field"]["buckets"], [])
            self.assertIsNone(edged["empty_pct"]["values"]["50.0"])
            self.assertEqual(edged["empty_stats"]["count"], 0)
            self.assertIsNone(edged["empty_ranks"]["values"]["1.0"])

    @parameterized.expand(get_allowed_versions())
    def test_pipeline_top_hits_top_metrics_and_rate(self, version):
        es_client = ELASTIC_CLIENTS[version]
        index_name = "test_pipeline_top_hits_top_metrics_and_rate"
        documents = {
            "a": {"n": 1, "s": "a", "status": "ok", "ts": "2019-01-01T00:30:00"},
            "b": {"n": 3, "s": "b", "status": "ok", "ts": "2019-01-01T01:30:00"},
            "c": {"n": 10, "s": "c", "status": "ko", "ts": "2019-01-01T01:45:00"},
        }
        std_path = (
            "by_status>stats" if version == "mock" else "by_status>stats.std_deviation"
        )
        pct_path = "by_status>pct" if version == "mock" else "by_status>pct.50"
        requested = {
            "by_status": {
                "terms": {"field": "status"},
                "aggs": {
                    "avg_n": {"avg": {"field": "n"}},
                    "stats": {"extended_stats": {"field": "n"}},
                    "pct": {"percentiles": {"field": "n", "percents": [50]}},
                },
            },
            "min_count": {"min_bucket": {"buckets_path": "by_status>_count"}},
            "max_count": {"max_bucket": {"buckets_path": "by_status>_count"}},
            "sum_count": {"sum_bucket": {"buckets_path": "by_status>_count"}},
            "max_avg": {"max_bucket": {"buckets_path": "by_status>avg_n"}},
            "max_std": {"max_bucket": {"buckets_path": std_path}},
            "min_pct": {"min_bucket": {"buckets_path": pct_path}},
            "nobody": {"terms": {"field": "absent"}},
            "sum_nobody": {"sum_bucket": {"buckets_path": "nobody>_count"}},
            "hours": {
                "date_histogram": {"field": "ts", "interval": "1h", "keyed": True}
            },
            "sum_hours": {"sum_bucket": {"buckets_path": "hours>_count"}},
            "top_default": {"top_hits": {"size": 1, "sort": {"n": "desc"}}},
            "top_false": {
                "top_hits": {"size": 1, "sort": {"n": "desc"}, "_source": False}
            },
            "top_list": {
                "top_hits": {"size": 1, "sort": {"n": "desc"}, "_source": ["s"]}
            },
            "top_includes": {
                "top_hits": {
                    "size": 1,
                    "sort": {"n": "desc"},
                    "_source": {"includes": ["n"]},
                }
            },
        }
        if version != "v6":
            requested["metrics"] = {
                "top_metrics": {
                    "size": 2,
                    "sort": {"n": "desc"},
                    "metrics": {"field": "n"},
                }
            }
        if version == "mock":
            requested["by_status"]["aggs"]["pct_missing"] = {
                "percentiles": {"field": "absent", "percents": [50]}
            }
            requested["by_status"]["aggs"]["nested"] = {"filter": {"match_all": {}}}
            requested.update(
                {
                    "avg_n": {"avg": {"field": "n"}},
                    "avg_nested": {"avg_bucket": {"buckets_path": "by_status>nested"}},
                    "absent_metric": {
                        "avg_bucket": {"buckets_path": "by_status>absent"}
                    },
                    "from_empty_pct": {
                        "max_bucket": {"buckets_path": "by_status>pct_missing"}
                    },
                    "sum_avg": {"sum_bucket": {"buckets_path": "avg_n"}},
                    "avg_nobody": {"avg_bucket": {"buckets_path": "nobody>avg_n"}},
                    "missing": {"avg_bucket": {"buckets_path": "missing>avg_n"}},
                    "not_text": {"sum_bucket": {"buckets_path": 5}},
                    "as_dict": {
                        "sum_bucket": {"buckets_path": {"count": "by_status>_count"}}
                    },
                    "only_ok": {"filter": {"term": {"status": "ok"}}},
                    "sum_ok": {"sum_bucket": {"buckets_path": "only_ok"}},
                    "top_none": {
                        "top_hits": {"size": 1, "sort": {"n": "desc"}, "_source": None}
                    },
                    "top_empty_includes": {
                        "top_hits": {"size": 1, "sort": {"n": "desc"}, "_source": {}}
                    },
                    "top_other": {
                        "top_hits": {"size": 1, "sort": {"n": "desc"}, "_source": 5}
                    },
                    "metrics_plain": {"top_metrics": {"metrics": ["n"]}},
                    "rate_n": {"rate": {"field": "n"}},
                }
            )
        aggs = self._aggs(es_client, index_name, documents, requested)
        self.assertEqual(aggs["min_count"]["value"], 1)
        self.assertEqual(aggs["max_count"]["value"], 2)
        self.assertEqual(aggs["sum_count"]["value"], 3)
        self.assertEqual(aggs["max_avg"]["value"], 10)
        self.assertGreater(aggs["max_std"]["value"], 0)
        self.assertEqual(aggs["min_pct"]["value"], 2)
        self.assertEqual(aggs["sum_nobody"]["value"], 0)
        self.assertEqual(aggs["sum_hours"]["value"], 3)
        self.assertIsInstance(aggs["hours"]["buckets"], dict)
        top = aggs["top_default"]["hits"]["hits"][0]
        self.assertEqual(top["_source"]["s"], "c")
        self.assertNotIn("_source", aggs["top_false"]["hits"]["hits"][0])
        self.assertEqual(aggs["top_list"]["hits"]["hits"][0]["_source"], {"s": "c"})
        self.assertEqual(aggs["top_includes"]["hits"]["hits"][0]["_source"], {"n": 10})
        if version != "v6":
            self.assertEqual(
                [item["metrics"] for item in aggs["metrics"]["top"]],
                [{"n": 10}, {"n": 3}],
            )
        if version == "mock":
            self.assertIsNone(aggs["avg_nested"]["value"])
            self.assertIsNone(aggs["absent_metric"]["value"])
            self.assertIsNone(aggs["from_empty_pct"]["value"])
            self.assertEqual(aggs["sum_avg"]["value"], 0)
            self.assertIsNone(aggs["avg_nobody"]["value"])
            self.assertIsNone(aggs["missing"]["value"])
            self.assertEqual(aggs["not_text"]["value"], 0)
            self.assertEqual(aggs["as_dict"]["value"], 3)
            self.assertEqual(aggs["sum_ok"]["value"], 2)
            self.assertEqual(aggs["top_none"]["hits"]["hits"][0]["_source"]["n"], 10)
            self.assertEqual(
                aggs["top_empty_includes"]["hits"]["hits"][0]["_source"], {}
            )
            self.assertEqual(aggs["top_other"]["hits"]["hits"][0]["_source"]["n"], 10)
            self.assertEqual(aggs["metrics_plain"]["top"][0]["metrics"], {})
            self.assertIsNone(aggs["rate_n"]["value"])

    @parameterized.expand(get_allowed_versions())
    def test_search_aggregations_use_the_query(self, version):
        es_client = ELASTIC_CLIENTS[version]
        index_name = "test_search_aggregations_use_the_query"
        aggs = self._aggs(
            es_client,
            index_name,
            {
                "a": {"status": "ok", "n": 1},
                "b": {"status": "ko", "n": 9},
            },
            {"by_status": {"terms": {"field": "status"}}},
            query={"term": {"status": "ok"}},
        )
        self.assertEqual(
            [
                (bucket["key"], bucket["doc_count"])
                for bucket in aggs["by_status"]["buckets"]
            ],
            [("ok", 1)],
        )

    @parameterized.expand(get_allowed_versions(["mock"]))
    def test_bulk_operation_error_fallbacks(self, version):
        client = ELASTIC_CLIENTS[version]
        index_name = "test_bulk_operation_error_fallbacks"
        with patch.object(client, "delete", return_value=None):
            skipped = client.bulk([{"delete": {"_index": index_name, "_id": "1"}}])
        self.assertEqual(
            skipped["items"], [{"delete": {"status": 500, "result": "error"}}]
        )
        with patch.object(client, "index", side_effect=ValueError("bad document")):
            failed = client.bulk(
                [{"index": {"_index": index_name, "_id": "1"}}, {"n": 1}]
            )
        self.assertEqual(failed["items"][0]["index"]["status"], 400)
        self.assertEqual(
            failed["items"][0]["index"]["error"]["type"],
            "illegal_argument_exception",
        )
        with patch.object(client, "get", return_value={"found": False}):
            conflicted = client.bulk(
                [
                    {"update": {"_index": index_name, "_id": "missing"}},
                    {"doc": {"a": 1}},
                ]
            )
        self.assertEqual(conflicted["items"][0]["update"]["status"], 409)

        client.indices.create(f"{index_name}-1")
        self.assertEqual(client.indices.delete_safe(f"{index_name}-1,missing"), 0)
        self.assertTrue(client.indices.exists(f"{index_name}-1"))


class MockSearchHelperTest(unittest.TestCase):
    def test_histogram_helpers_for_intervals_search_rewrites(self):
        # Search rewrites date_histogram "interval" before evaluation, so the
        # fallback and the empty-delta guards are exercised directly.
        doc = MemoryDocument("1", {"ts": "2019-01-01T00:30:00"})
        result = _eval_date_histogram([doc], {"field": "ts", "interval": "1h"}, None)
        self.assertEqual(
            result["buckets"][0]["key_as_string"], "2019-01-01T00:00:00.000Z"
        )
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
