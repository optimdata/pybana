# -*- coding: utf-8 -*-

import datetime
import json
import os
import sys
import unittest

BASE_DIRECTORY = os.path.join(os.path.dirname(__file__), "..")  # NOQA
sys.path.insert(0, BASE_DIRECTORY)  # NOQA

import pytest  # noqa: E402
from elasticsearch_dsl import Q, Search  # noqa: E402

from pybana.translators.elastic import ElasticTranslator  # noqa: E402
from pybana.translators.elastic.buckets import (  # noqa: E402
    BucketTranslator,
    TimeSeriesBucket,
    compute_auto_interval,
    duration_from_interval,
    format_from_interval,
)
from pybana.translators.elastic.filter import (  # noqa: E402
    ExistsFilterTranslator,
    FilterTranslator,
    PhraseFilterTranslator,
    PhrasesFilterTranslator,
    RangeFilterTranslator,
)
from pybana.translators.elastic.metrics import (  # noqa: E402
    CardinalityMetric,
    MetricTranslator,
)
from pybana.translators.elastic.utils import (  # noqa: E402
    SearchListProxy,
    get_field_arg,
)

BEG = datetime.datetime(2019, 1, 1, tzinfo=datetime.timezone.utc)
END = datetime.datetime(2019, 1, 3, tzinfo=datetime.timezone.utc)


class Scope:
    def __init__(self, beg=BEG, end=END):
        self.beg = beg
        self.end = end
        self.tzinfo = datetime.timezone.utc


class AggProxy:
    def __init__(self):
        self.buckets = []
        self.metrics = []

    def bucket(self, name, agg_type, **params):
        child = AggProxy()
        self.buckets.append((name, agg_type, params, child))
        return child

    def metric(self, name, agg_type, **params):
        self.metrics.append((name, agg_type, params))


def metric(agg_id, agg_type, params, schema="metric"):
    return {"id": agg_id, "type": agg_type, "schema": schema, "params": params}


def bucket(agg_id, agg_type, params, schema="segment"):
    return {"id": agg_id, "type": agg_type, "schema": schema, "params": params}


PLAIN = {"name": "bytes", "type": "number"}
SCRIPTED = {
    "name": "scripted",
    "type": "number",
    "scripted": True,
    "script": "doc['bytes'].value",
    "lang": "painless",
}


class IndexPattern:
    def to_dict(self):
        return {
            "index-pattern": {
                "title": "logs-*",
                "timeFieldName": "@timestamp",
                "fields": json.dumps(
                    [
                        {"name": "@timestamp", "type": "date"},
                        {"name": "host", "type": "string"},
                        PLAIN,
                        SCRIPTED,
                    ]
                ),
            }
        }


class Visualization:
    def __init__(self, vis_type, state, spec=None):
        self.visState = {"type": vis_type, "params": {"spec": spec or ""}}
        self.visualization = {"visState": json.dumps(state)}

    def index(self, using=None):
        return IndexPattern()

    def filters(self):
        return Q("exists", field="host")


class TestIntervals(unittest.TestCase):
    def test_auto_and_fixed_intervals(self):
        def auto(**delta):
            end = datetime.datetime(2020, 1, 1)
            return compute_auto_interval("auto", end - datetime.timedelta(**delta), end)

        self.assertEqual(auto(days=800), "30d")
        self.assertEqual(auto(days=400), "1w")
        self.assertEqual(auto(days=50), "1d")
        self.assertEqual(auto(days=20), "12h")
        self.assertEqual(auto(days=5), "3h")
        self.assertEqual(auto(days=2), "1h")
        self.assertEqual(auto(days=1), "30m")
        self.assertEqual(auto(seconds=43000), "10m")
        self.assertEqual(auto(seconds=13000), "5m")
        self.assertEqual(auto(seconds=3600), "1m")
        self.assertEqual(auto(seconds=1200), "30s")
        self.assertEqual(auto(seconds=600), "10s")
        self.assertEqual(auto(seconds=240), "5s")
        self.assertEqual(auto(seconds=1), "1s")
        self.assertEqual(compute_auto_interval("d", BEG, END), "1d")

    def test_duration_and_format(self):
        with pytest.raises(TypeError):
            duration_from_interval("1y")
        with pytest.raises(TypeError):
            duration_from_interval("1q")
        with pytest.raises(TypeError):
            duration_from_interval("1M")
        self.assertEqual(duration_from_interval("1w"), datetime.timedelta(weeks=1))
        self.assertEqual(duration_from_interval("1d"), datetime.timedelta(days=1))
        self.assertEqual(duration_from_interval("1h"), datetime.timedelta(hours=1))
        self.assertEqual(duration_from_interval("1s"), datetime.timedelta(seconds=1))
        self.assertEqual(format_from_interval("1y"), "yyyy")
        self.assertEqual(format_from_interval("1q"), "yyyy-MM")
        self.assertEqual(format_from_interval("1M"), "yyyy-MM")
        self.assertEqual(format_from_interval("1d"), "yyyy-MM-dd")
        self.assertEqual(format_from_interval("1w"), "yyyy-MM-dd")
        self.assertEqual(format_from_interval("1h"), "yyyy-MM-dd'T'HH'h'")
        self.assertEqual(format_from_interval("1s"), "date_time")


class TestUtils(unittest.TestCase):
    def test_field_arg(self):
        agg = {"params": {"field": "bytes"}}
        self.assertEqual(get_field_arg(agg, None), {"field": "bytes"})
        self.assertEqual(get_field_arg(agg, PLAIN), {"field": "bytes"})
        self.assertEqual(
            get_field_arg(agg, SCRIPTED),
            {"script": {"source": SCRIPTED["script"], "lang": "painless"}},
        )

    def test_search_list_proxy(self):
        self.assertEqual(len(SearchListProxy()), 0)
        with pytest.raises(AssertionError):
            SearchListProxy(["not-a-search"])
        proxy = SearchListProxy([Search(index="a"), Search(index="b")])
        with pytest.raises(AssertionError):
            proxy.append("not-a-search")
        proxy.append(Search(index="c"))
        self.assertEqual(len(proxy), 3)
        filtered = proxy.filter("term", tag="example")
        self.assertIsInstance(filtered, SearchListProxy)
        self.assertEqual(len(filtered), 3)
        bodies = filtered.to_dict()
        self.assertIsInstance(bodies, list)
        self.assertNotIsInstance(bodies, SearchListProxy)
        self.assertEqual(
            bodies[0]["query"]["bool"]["filter"][0]["term"]["tag"], "example"
        )


class TestFilters(unittest.TestCase):
    def test_filter_types(self):
        exists = ExistsFilterTranslator().translate(
            {"meta": {"key": "host", "params": {}}}
        )
        self.assertEqual(exists.to_dict(), {"exists": {"field": "host"}})
        phrase = PhraseFilterTranslator().translate(
            {"meta": {"key": "msg", "value": "hello", "params": {"query": "ignored"}}}
        )
        self.assertEqual(phrase.to_dict()["match_phrase"]["msg"], {"query": "hello"})
        from_params = PhraseFilterTranslator().translate(
            {"meta": {"key": "msg", "params": {"query": "from-params"}}}
        )
        self.assertEqual(
            from_params.to_dict()["match_phrase"]["msg"], {"query": "from-params"}
        )
        self.assertIsNone(
            PhrasesFilterTranslator().translate({"meta": {"key": "msg", "params": []}})
        )
        phrases = PhrasesFilterTranslator().translate(
            {"meta": {"key": "msg", "params": ["a", "b"]}}
        )
        self.assertIn("should", phrases.to_dict()["bool"])
        ranged = RangeFilterTranslator().translate(
            {"meta": {"key": "bytes", "type": "range", "params": {"gte": 1}}}
        )
        self.assertEqual(ranged.to_dict(), {"range": {"bytes": {"gte": 1}}})

        translated = FilterTranslator().translate(
            [
                {
                    "meta": {
                        "disabled": True,
                        "negate": False,
                        "type": "exists",
                        "key": "skip",
                        "params": {},
                    }
                },
                {
                    "meta": {
                        "disabled": False,
                        "negate": False,
                        "type": "exists",
                        "key": "host",
                        "params": {},
                    }
                },
                {
                    "meta": {
                        "disabled": False,
                        "negate": True,
                        "type": "range",
                        "key": "bytes",
                        "params": {"gte": 1},
                    }
                },
            ]
        )
        body = translated.to_dict()
        self.assertIn("must_not", json.dumps(body))
        self.assertIn("host", json.dumps(body))
        self.assertNotIn("skip", json.dumps(body))


class TestMetrics(unittest.TestCase):
    def test_field_metrics(self):
        proxy = AggProxy()
        state = {}
        specs = [
            ("avg", "avg", {"field": "bytes", "json": '{"missing": 0}'}, PLAIN),
            ("card", "cardinality", {"field": "bytes"}, PLAIN),
            ("max", "max", {"field": "bytes"}, None),
            ("min", "min", {"field": "scripted"}, SCRIPTED),
            ("sum", "sum", {"field": "bytes", "json": ""}, PLAIN),
            ("med", "median", {"field": "bytes"}, PLAIN),
            ("pct", "percentiles", {"field": "bytes", "percents": [50, 95]}, PLAIN),
            ("rank", "percentile_ranks", {"field": "bytes", "values": [1, 2]}, PLAIN),
            ("std", "std_dev", {"field": "bytes"}, PLAIN),
            ("rate", "rate", {"field": "bytes"}, PLAIN),
            ("cnt", "value_count", {"field": "bytes"}, PLAIN),
        ]
        for agg_id, agg_type, params, field in specs:
            MetricTranslator().translate(
                proxy, metric(agg_id, agg_type, params), state, field
            )
        by_id = {name: (agg_type, params) for name, agg_type, params in proxy.metrics}
        self.assertEqual(by_id["avg"], ("avg", {"field": "bytes", "missing": 0}))
        self.assertEqual(by_id["card"][0], "cardinality")
        self.assertEqual(by_id["max"][1], {"field": "bytes"})
        self.assertIn("script", by_id["min"][1])
        self.assertEqual(by_id["med"][0], "percentiles")
        self.assertEqual(by_id["med"][1]["percents"], [50])
        self.assertEqual(by_id["pct"][1]["percents"], [50, 95])
        self.assertEqual(by_id["rank"][1]["values"], [1, 2])
        self.assertEqual(by_id["std"][0], "extended_stats")
        self.assertEqual(by_id["rate"][0], "rate")
        self.assertEqual(by_id["cnt"][0], "value_count")
        direct = CardinalityMetric().params(
            metric("card", "cardinality", {"field": "bytes"}), PLAIN
        )
        self.assertEqual(direct["field"], "bytes")

        single_params = {"field": "bytes", "values": [99]}
        single = metric("one", "single_percentile_rank", single_params)
        MetricTranslator().translate(proxy, single, state, PLAIN)
        self.assertNotIn("values", single_params)
        self.assertEqual(proxy.metrics[-1][1], "percentile_ranks")
        self.assertEqual(proxy.metrics[-1][2]["values"], [0])

        before = len(proxy.metrics)
        MetricTranslator().translate(proxy, metric("n", "count", {}), state, None)
        MetricTranslator().translate(
            proxy,
            metric("formula", "datasweet_formula", {"formula": "agg1"}),
            state,
            None,
        )
        self.assertEqual(len(proxy.metrics), before)

    def test_top_and_bucket_metrics(self):
        proxy = AggProxy()
        MetricTranslator().translate(
            proxy,
            metric(
                "hits",
                "top_hits",
                {"field": "host", "sortField": "bytes", "sortOrder": "desc", "size": 2},
            ),
            {},
            PLAIN,
        )
        MetricTranslator().translate(
            proxy,
            metric(
                "top",
                "top_metrics",
                {"field": "host", "sortField": "bytes", "sortOrder": "asc", "size": 1},
            ),
            {},
            PLAIN,
        )
        self.assertEqual(proxy.metrics[0][2]["_source"], "host")
        self.assertEqual(proxy.metrics[1][2]["metrics"], {"field": "host"})

        custom_bucket = bucket(
            "2",
            "terms",
            {
                "field": "host",
                "size": 5,
                "orderBy": "_key",
                "order": "desc",
            },
            schema="bucket",
        )
        fields = {"host": {"name": "host", "type": "string"}, "bytes": PLAIN}
        state = {"aggs": [custom_bucket]}
        MetricTranslator().translate(
            proxy,
            metric(
                "avg_b",
                "avg_bucket",
                {
                    "customBucket": custom_bucket,
                    "customMetric": metric("3", "count", {}),
                },
            ),
            state,
            None,
            Scope(),
            fields,
        )
        self.assertEqual(proxy.metrics[-1][2]["buckets_path"], "2>_count")
        MetricTranslator().translate(
            proxy,
            metric(
                "sum_b",
                "sum_bucket",
                {
                    "customBucket": custom_bucket,
                    "customMetric": metric("4", "sum", {"field": "bytes"}),
                },
            ),
            state,
            None,
            Scope(),
            fields,
        )
        self.assertEqual(proxy.metrics[-1][2]["buckets_path"], "2>4")
        for agg_type in ("min_bucket", "max_bucket"):
            MetricTranslator().translate(
                proxy,
                metric(
                    agg_type,
                    agg_type,
                    {
                        "customBucket": custom_bucket,
                        "customMetric": metric("4", "sum", {"field": "bytes"}),
                    },
                ),
                state,
                None,
                Scope(),
                fields,
            )
            self.assertEqual(proxy.metrics[-1][0], agg_type)


class TestBuckets(unittest.TestCase):
    def test_bucket_types(self):
        scope = Scope()
        fields = {
            "@timestamp": {"name": "@timestamp", "type": "date"},
            "host": {"name": "host", "type": "string"},
            "bytes": PLAIN,
            "scripted": SCRIPTED,
        }
        count = metric("c", "count", {})
        total = metric("m", "sum", {"field": "bytes"})
        state = {"aggs": [count, total]}
        specs = [
            bucket(
                "dates",
                "date_histogram",
                {
                    "field": "@timestamp",
                    "interval": "auto",
                    "json": '{"min_doc_count": 1}',
                },
            ),
            bucket(
                "scripted_dates",
                "date_histogram",
                {"field": "scripted", "interval": "h", "json": ""},
            ),
            bucket(
                "missing_field",
                "date_histogram",
                {"field": "unknown", "interval": "d"},
            ),
            bucket(
                "dr",
                "date_range",
                {"field": "@timestamp", "ranges": [{"from": "now-1d"}]},
            ),
            bucket(
                "filters",
                "filters",
                {
                    "filters": [
                        {"label": "named", "input": {"query": "status:200"}},
                        {"input": {"query": "host:a"}},
                        {"label": "", "input": {"query": ""}},
                    ]
                },
            ),
            bucket("hist", "histogram", {"field": "bytes", "interval": 10}),
            bucket(
                "rng", "range", {"field": "bytes", "ranges": [{"from": 0, "to": 10}]}
            ),
            bucket(
                "by_count",
                "terms",
                {
                    "field": "host",
                    "size": 5,
                    "orderBy": "c",
                    "order": "desc",
                },
            ),
            bucket(
                "by_custom",
                "terms",
                {
                    "field": "host",
                    "size": 3,
                    "orderBy": "custom",
                    "order": "asc",
                    "orderAgg": {
                        "id": "oa",
                        "type": "histogram",
                        "params": {"field": "bytes", "interval": 10},
                    },
                },
            ),
            bucket(
                "by_metric",
                "terms",
                {"field": "host", "size": 4, "orderBy": "m", "order": "desc"},
            ),
            bucket("rare", "rare_terms", {"field": "host"}),
            bucket("sig", "significant_terms", {"field": "host", "size": 2}),
            bucket("text", "significant_text", {"field": "host"}),
            bucket("ts", "time_series", {"json": '{"keyed": true}'}),
        ]
        for agg in specs:
            proxy = AggProxy()
            BucketTranslator().translate(proxy, agg, state, scope, fields)
            self.assertEqual(proxy.buckets[0][0], agg["id"])
            self.assertEqual(proxy.buckets[0][1], agg["type"])

        dates = AggProxy()
        BucketTranslator().translate(dates, specs[0], state, scope, fields)
        self.assertEqual(dates.buckets[0][2]["interval"], "1h")
        self.assertEqual(dates.buckets[0][2]["time_zone"], "UTC")
        self.assertEqual(dates.buckets[0][2]["min_doc_count"], 1)
        scripted = AggProxy()
        BucketTranslator().translate(scripted, specs[1], state, scope, fields)
        self.assertEqual(scripted.buckets[0][2]["valueType"], "number")
        self.assertIn("script", scripted.buckets[0][2])
        counted = AggProxy()
        BucketTranslator().translate(counted, specs[7], state, scope, fields)
        self.assertEqual(counted.buckets[0][2]["order"], {"_count": "desc"})
        custom = AggProxy()
        BucketTranslator().translate(custom, specs[8], state, scope, fields)
        self.assertEqual(custom.buckets[0][2]["order"], {"oa": "asc"})
        self.assertEqual(custom.buckets[0][3].buckets[0][0], "oa")
        ordered = AggProxy()
        BucketTranslator().translate(ordered, specs[9], state, scope, fields)
        self.assertEqual(ordered.buckets[0][3].metrics[0][0], "m")
        filters = AggProxy()
        BucketTranslator().translate(filters, specs[4], state, scope, fields)
        named = filters.buckets[0][2]["filters"]["named"]
        self.assertEqual(named["kql"]["query"], "status:200")
        self.assertIn("host:a", filters.buckets[0][2]["filters"])
        self.assertEqual(filters.buckets[0][2]["filters"]["*"], {"match_all": {}})
        series = TimeSeriesBucket().translate(
            bucket(
                "ts", "time_series", {"field": "scripted", "json": '{"keyed": true}'}
            ),
            state,
            scope,
            SCRIPTED,
        )
        self.assertEqual(series["valueType"], "number")
        self.assertTrue(series["keyed"])


class TestElasticTranslator(unittest.TestCase):
    def test_legacy_visualization(self):
        state = {
            "aggs": [
                bucket(
                    "d",
                    "date_histogram",
                    {"field": "@timestamp", "interval": "auto"},
                ),
                bucket(
                    "t",
                    "terms",
                    {
                        "field": "scripted",
                        "size": 5,
                        "orderBy": "_key",
                        "order": "asc",
                    },
                    schema="group",
                ),
                bucket(
                    "h",
                    "histogram",
                    {"field": "bytes", "interval": 10},
                    schema="split",
                ),
                metric("m", "sum", {"field": "bytes"}),
                metric("c", "count", {}),
                metric("ignored", "avg", {"field": "bytes"}, schema="other"),
            ]
        }
        search = ElasticTranslator(using="unit").translate(
            Visualization("histogram", state), Scope()
        )
        body = search.to_dict()
        self.assertEqual(body["size"], 0)
        self.assertIn("logs-*", search._index)
        self.assertEqual(body["aggs"]["d"]["date_histogram"]["interval"], "1h")
        self.assertIn("script", body["aggs"]["d"]["aggs"]["t"]["terms"])
        self.assertEqual(
            body["aggs"]["d"]["aggs"]["t"]["aggs"]["h"]["aggs"]["m"]["sum"]["field"],
            "bytes",
        )
        dumped = json.dumps(body["query"])
        self.assertIn("@timestamp", dumped)
        self.assertIn("exists", dumped)

    def test_vega_visualization(self):
        scope = Scope()
        body = {
            "size": 0,
            "aggs": {
                "hist": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "interval": {"%autointerval%": True},
                    }
                },
                "fixed": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "interval": "1d",
                        "time_zone": "Europe/Paris",
                    }
                },
                "auto": {"auto_date_histogram": {"field": "@timestamp", "buckets": 3}},
            },
            "script_fields": {
                "min": {"%timefilter%": "min"},
                "max": {"%timefilter%": "max"},
                "both": {"%timefilter%": True},
                "skip": {"%timefilter%": "nope"},
                "nested": [
                    {"interval": {"%autointerval%": True}},
                    {"interval": {"%autointerval%": False}},
                    "plain",
                ],
            },
        }
        spec = json.dumps(
            {
                "data": [
                    {
                        "url": {
                            "index": "logs",
                            "body": body,
                            "%timefield%": "@timestamp",
                        }
                    },
                    {
                        "url": {
                            "index": "other",
                            "body": {"size": 1, "query": {"match_all": {}}},
                        }
                    },
                    {"name": "static"},
                ]
            }
        )
        searches = ElasticTranslator(using="unit").translate(
            Visualization("vega", {}, spec), scope
        )
        self.assertIsInstance(searches, SearchListProxy)
        self.assertEqual(len(searches), 3)
        first = searches[0].to_dict()
        self.assertEqual(first["aggs"]["hist"]["date_histogram"]["interval"], "1h")
        self.assertEqual(first["aggs"]["hist"]["date_histogram"]["time_zone"], "UTC")
        self.assertEqual(
            first["aggs"]["fixed"]["date_histogram"]["time_zone"], "Europe/Paris"
        )
        self.assertEqual(
            first["aggs"]["auto"]["auto_date_histogram"]["time_zone"], "UTC"
        )
        self.assertEqual(first["script_fields"]["min"], scope.beg.isoformat())
        self.assertEqual(first["script_fields"]["max"], scope.end.isoformat())
        self.assertEqual(
            first["script_fields"]["both"],
            {"min": scope.beg.isoformat(), "max": scope.end.isoformat()},
        )
        self.assertEqual(first["script_fields"]["skip"], {})
        self.assertEqual(first["script_fields"]["nested"][0]["interval"], "1h")
        self.assertEqual(
            first["script_fields"]["nested"][1]["interval"], {"%autointerval%": False}
        )
        self.assertIn("range", json.dumps(first["query"]))
        self.assertNotIn("%timefield%", searches[1].to_dict().get("query", {}))
        self.assertEqual(searches[2].to_dict()["size"], 0)

        single = ElasticTranslator(using="unit").translate(
            Visualization(
                "vega",
                {},
                json.dumps({"data": {"name": "only"}}),
            ),
            scope,
        )
        self.assertIsInstance(single, Search)
        self.assertEqual(single.to_dict()["size"], 0)
