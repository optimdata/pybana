# -*- coding: utf-8 -*-

import ast
import datetime
import elasticsearch
import elasticsearch_dsl
import json
import os
import pytest
import pytz
import sys

BASE_DIRECTORY = os.path.join(os.path.dirname(__file__), "..")  # NOQA
sys.path.insert(0, BASE_DIRECTORY)  # NOQA

from pybana import (
    Scope,
    ElasticTranslator,
    Kibana,
    VegaRenderer,
    VegaTranslator,
    VEGA_METRICS,
)
from pybana.translators.elastic.buckets import (
    format_from_interval,
    compute_auto_interval,
)
from pybana.elastic.elastic_client import ElasticsearchExtClient


PYBANA_INDEX = ".kibana_pybana_test"
ELASTICSEARCH_V6 = elasticsearch.Elasticsearch()
ELASTICSEARCH_V8 = elasticsearch.Elasticsearch(["http://localhost:9201"])
ELASTIC_V6 = ElasticsearchExtClient()
ELASTIC_V8 = ElasticsearchExtClient(ELASTICSEARCH_V8)
ELASTICS = {"default": ELASTIC_V6, "v6": ELASTIC_V6, "v8": ELASTIC_V8}
elasticsearch_dsl.connections.add_connection("default", ELASTIC_V6)
elasticsearch_dsl.connections.add_connection("v6", ELASTIC_V6)
elasticsearch_dsl.connections.add_connection("v8", ELASTIC_V8)


def load_fixtures(elastic, kibana, index):
    print(f"load_fixtures({elastic}, {kibana}, {index})")
    for realindex in elastic.indices.get(index, ignore_unavailable=True):
        elastic.indices.delete(realindex)
    datafn = os.path.join(BASE_DIRECTORY, "pybana/index.json")

    kibana.init_index()

    def actions():
        with open(datafn, "r") as fd:
            for line in fd:
                if not line:
                    continue
                action = json.loads(line)
                action["_index"] = index
                yield action

    elastic.helpers_bulk(actions(), refresh="wait_for")


def load_data(elastic, index):
    ts = datetime.datetime(2019, 1, 1)
    if elastic.indices.exists(index):
        elastic.indices.delete(index)
    print(f"creating index {index} for {elastic}")
    elastic.indices.create(
        index,
        body={
            "mappings": {
                "doc": {
                    "properties": {
                        "s": {"type": "keyword"},
                        "i": {"type": "integer"},
                        "d": {"type": "integer"},
                        "f": {"type": "float"},
                        "t": {"type": "float"},
                        "ts": {"type": "date"},
                    }
                }
            }
        },
    )

    def actions():
        for i in range(100):
            yield {
                "_index": index,
                "_type": "doc",
                "_id": str(i),
                "_source": {
                    "ts": ts + datetime.timedelta(hours=i),
                    "f": float(i),
                    "i": i,
                    "s": chr(100 + (i % 10)),
                    "d": i,
                    "t": [float(i), float(i + 1)],
                },
            }

    elasticsearch.helpers.bulk(elastic, actions(), refresh="wait_for")


def test_client_v6():
    client_test("v6")


def test_client_v8():
    client_test("v8")


def client_test(version):
    kibana = Kibana(index=PYBANA_INDEX, using=version)
    elastic = ELASTICS[version]
    elastic.indices.delete(f"{PYBANA_INDEX}*")
    elastic.indices.create(f"{PYBANA_INDEX}_1")
    load_fixtures(elastic, kibana, PYBANA_INDEX)
    kibana.init_config()
    kibana.init_config()
    assert kibana.config()
    assert len(list(kibana.index_patterns())) == 1
    index_pattern = kibana.index_pattern("6c172f80-fb13-11e9-84e4-078763638bf3")
    index_pattern.fields
    index_pattern.fieldFormatMap
    kibana.update_or_create_default_index_pattern(index_pattern)
    kibana.update_or_create_default_index_pattern(index_pattern)
    kibana.update_or_create_default_index_pattern(index_pattern)
    visualizations = list(kibana.visualizations().scan())
    assert len(visualizations) == 29
    visualization = kibana.visualization("6eab7cb0-fb18-11e9-84e4-078763638bf3")
    visualization.visState
    visualization.uiStateJSON
    assert visualization.index(using=elastic).meta.id == index_pattern.meta.id
    dashboards = list(kibana.dashboards())
    print(f"dashboards: {dashboards[0].__dict__}")
    assert len(dashboards) == 1
    dashboard = kibana.dashboard("f57a7160-fb18-11e9-84e4-078763638bf3")
    dashboard.panelsJSON
    dashboard.optionsJSON
    assert len(dashboard.visualizations(using=elastic)) == 2
    visualization = kibana.visualization("f4a09a00-fe77-11e9-8c18-250a1adff826")
    search = visualization.related_search(using=elastic)
    assert search.meta.id == "search:2139a4e0-fe77-11e9-833a-0fef2d7dd143"
    assert len(list(kibana.searches())) == 1
    search = kibana.search("2139a4e0-fe77-11e9-833a-0fef2d7dd143")
    assert visualization.index(using=elastic).meta.id == index_pattern.meta.id


def test_translators_v6():
    translators_test("v6")


def test_translators_v8():
    translators_test("v8")


def translators_test(version):
    # elasticsearch_dsl.connections.add_connection("default", ELASTIC_V8)

    kibana = Kibana(
        index=PYBANA_INDEX, using=elasticsearch_dsl.connections.get_connection(version)
    )
    elastic = ELASTICS[version]
    print(f"load_fixtures({elastic}, {kibana}, {PYBANA_INDEX})")
    load_fixtures(elastic, kibana, PYBANA_INDEX)
    print(f"load_data({elastic}, pybana)")
    load_data(elastic, "pybana")
    assert isinstance(elastic, ElasticsearchExtClient)

    kibana.init_config()
    # assert False

    translator = ElasticTranslator(using=elastic)
    scope = Scope(
        datetime.datetime(2019, 1, 1, tzinfo=pytz.utc),
        datetime.datetime(2019, 1, 3, tzinfo=pytz.utc),
        pytz.utc,
        kibana.config(),
    )
    for visualization in kibana.visualizations().scan():
        print(
            f"visualization: {visualization.__class__.__name__} {visualization.__dict__}"
        )
        if visualization.visState["type"] in (
            "histogram",
            "metric",
            "pie",
            "line",
            "vega",
            "table",
        ):
            print(f"visu: {visualization}")
            # elastic.indices.refresh()
            # print("after refresh")
            search = translator.translate(visualization, scope)
            print("after translate")
            visualization_id = visualization.meta.id.split(":")[-1]
            if visualization_id in (
                "695c02f0-fb1a-11e9-84e4-078763638bf3",
                "1c7226e0-ffd9-11e9-b6bd-4d907ad3c29d",
                "cdecdff0-ffd9-11e9-b6bd-4d907ad3c29d",
                "fa5fcfc0-ffd9-11e9-b6bd-4d907ad3c29d",
                "3e6c9e50-ffda-11e9-b6bd-4d907ad3c29d",
                "65881000-ffda-11e9-b6bd-4d907ad3c29d",
                "5fa0ea20-ffdc-11e9-b6bd-4d907ad3c29d",
                "86457e20-ffdc-11e9-b6bd-4d907ad3c29d",
                "ad4b9310-ffdc-11e9-b6bd-4d907ad3c29d",
                "e19d9640-ffdc-11e9-b6bd-4d907ad3c29d",
                "2fb77bc0-ffdd-11e9-b6bd-4d907ad3c29d",
                "9a4d3520-013f-11ea-b1ec-3910cd795dc1",
                "53b3da70-fbbc-11e9-84e4-078763638bf3",
                "c36b8b00-6f85-11ea-85b8-8f688e91da4a",
                "e8c08560-7276-11ea-a6e2-834e20d9c131",
                "5da362a0-732e-11ea-9c16-797f1f2fa4aa",
                "96645fc0-d636-11ea-8206-6f7030d7dd42",
            ):
                print(f"search : {search.__class__.__name__} {search.__dict__}")
                response = search.execute()
                VegaTranslator(using=elastic).translate(visualization, response, scope)
            if visualization_id in ("d6c8b900-eea7-11eb-8e30-87c8d06ba6ff",):
                response = search.execute()
                metric = VEGA_METRICS["top_hits"]()
                state = json.loads(visualization.visualization.visState)
                results = {
                    "1": "l, k",
                    "2": 47,
                    "3": 48,
                    "4": 46,
                    "5": 141,
                    "6": "48.0, 49.0, 47.0, 48.0",
                }
                for agg in state["aggs"]:
                    ret = metric.contribute(agg, response.aggregations, response)
                    assert ret == results[agg["id"]]


def test_vega_visualization_v6():
    vega_visualization_test("v6")


def test_vega_visualization_v8():
    vega_visualization_test("v8")


def vega_visualization_test(version):
    kibana = Kibana(index=PYBANA_INDEX, using=version)
    elastic = ELASTICS[version]

    load_fixtures(elastic, kibana, PYBANA_INDEX)
    load_data(elastic, "pybana")
    kibana.init_config()
    translator = ElasticTranslator(using=elastic)
    scope = Scope(
        datetime.datetime(2019, 1, 1, tzinfo=pytz.utc),
        datetime.datetime(2019, 1, 3, tzinfo=pytz.utc),
        pytz.utc,
        kibana.config(),
    )
    keys = [
        "37589ee0-70f3-11ea-9898-e1fb79cbf2bc",
        "da8f8510-7107-11ea-9898-e1fb79cbf2bc",
    ]
    for key in keys:
        visualization = kibana.visualization(key)
        search = translator.translate(visualization, scope)
        search_data = search.to_dict()
        if isinstance(search_data, list) and len(search_data) == 1:
            search_data = search_data[0]
        assert search_data["aggs"]["category"]["date_histogram"]["interval"] == "1h"

        response = search.execute()
        VegaTranslator(using=elastic).translate(visualization, response, scope)


def test_elastic_translator_helpers():
    assert format_from_interval("1y") == "yyyy"
    assert format_from_interval("1q") == "yyyy-MM"
    assert format_from_interval("1M") == "yyyy-MM"
    assert format_from_interval("1d") == "yyyy-MM-dd"
    assert format_from_interval("1h") == "yyyy-MM-dd'T'HH'h'"
    assert format_from_interval("1s") == "date_time"

    assert (
        compute_auto_interval("d", datetime.datetime.now(), datetime.datetime.now())
        == "1d"
    )

    def f(*args, **kwargs):
        delta = datetime.timedelta(*args, **kwargs)
        end = datetime.datetime.now()
        beg = end - delta
        return compute_auto_interval("auto", beg, end)

    assert f(days=800) == "30d"
    assert f(days=400) == "1w"
    assert f(days=50) == "1d"
    assert f(days=20) == "12h"
    assert f(days=5) == "3h"
    assert f(days=2) == "1h"
    assert f(days=1) == "30m"
    assert f(seconds=43000) == "10m"
    assert f(seconds=13000) == "5m"
    assert f(seconds=3600) == "1m"
    assert f(seconds=1200) == "30s"
    assert f(seconds=600) == "10s"
    assert f(seconds=240) == "5s"
    assert f(seconds=1) == "1s"


def test_vega_renderer():
    renderer = VegaRenderer()
    renderer.to_svg({"$schema": "https://vega.github.io/schema/vega/v5.json"})


def test_datasweet():
    import pybana.helpers.datasweet as ds

    assert ds.ds_avg(0, 1) == 0.5
    assert ds.ds_count(0, 1) == 2
    assert ds.ds_cusum(0, 1) == [0, 1]
    assert ds.ds_derivative(0, 1)[1] == 1
    assert ds.ds_max(0, 1) == 1
    assert ds.ds_min(0, 1) == 0
    assert ds.ds_next(0, 1)[0] == 1
    assert ds.ds_prev(0, 1)[1] == 0
    assert ds.ds_sum(0, 1) == 1
    assert ds.ds_if(1, 2, 0) == 2
    assert ds.ds_if([1, 0, 1], "a", "b") == ["a", "b", "a"]
    assert ds.ds_if([1, 0, 1], ["a", "a", "a"], ["b", "b", "b"]) == ["a", "b", "a"]
    assert ds.ds_ifnan(1, 0) == 1
    assert ds.ds_ifnan([1, "test", float("nan"), "1"], "default") == [
        1,
        "default",
        "default",
        "1",
    ]

    assert ds.is_variable("agg1")
    assert not ds.is_variable("xagg1")

    assert (
        ds.datasweet_eval("avg(agg1, agg2) + 1", {"1": {"value": 0}, "2": {"value": 1}})
        == 1.5
    )

    assert ds.datasweet_eval("floor(agg1)", {"1": {"value": 1.23}}) == 1
    assert ds.datasweet_eval("round(agg1)", {"1": {"value": 4.56}}) == 5
    assert ds.datasweet_eval("ceil(agg1)", {"1": {"value": 7.89}}) == 8
    assert ds.datasweet_eval("trunc(agg1)", {"1": {"value": 7.89}}) == 7

    assert ds.datasweet_eval("1 / 0", {}) is None

    with pytest.raises(ValueError):
        tree = ast.parse("x + 1", mode="eval")
        tree = ds.DatasweetTransformer().visit(tree)


def test_datetime():
    import pybana.helpers.datetime as dt

    assert dt.convert("") == ""
    assert dt.convert("Y") == "Y"
    assert dt.convert("w") is None
    assert dt.convert("llll") == "LLLL"
    assert dt.convert("[coucou]") == "[coucou]"
    assert dt.convert("Y [coucou]") == "Y [coucou]"
    assert dt.convert("[coucou] Y"), "[coucou] Y"
    assert dt.convert("[a [coucou]]"), "[a [coucou]]"
