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

from pybana import Scope, ElasticTranslator, Kibana, VegaRenderer, VegaTranslator
from pybana.translators.elastic.buckets import (
    format_from_interval,
    compute_auto_interval,
)

PYBANA_INDEX = ".kibana_pybana_test"
elastic = elasticsearch.Elasticsearch()
elasticsearch_dsl.connections.add_connection("default", elastic)


def load_fixtures(elastic, kibana, index):
    for realindex in elastic.indices.get(index, ignore_unavailable=True):
        elastic.indices.delete(realindex)
    datafn = os.path.join(BASE_DIRECTORY, "pybana/index.json")

    kibana.init_index()

    def actions():
        with open(datafn, "r") as fd:
            for line in fd:

                action = json.loads(line)
                action["_index"] = index
                yield action

    elasticsearch.helpers.bulk(elastic, actions(), refresh="wait_for")


def load_data(elastic, index):
    ts = datetime.datetime(2019, 1, 1)
    if elastic.indices.exists(index):
        elastic.indices.delete(index)
    elastic.indices.create(
        index,
        body={
            "mappings": {
                "doc": {
                    "properties": {
                        "s": {"type": "keyword"},
                        "i": {"type": "integer"},
                        "f": {"type": "float"},
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
                },
            }

    elasticsearch.helpers.bulk(elastic, actions(), refresh="wait_for")


def test_client():
    kibana = Kibana(PYBANA_INDEX)
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
    visualizations = list(kibana.visualizations().scan())
    assert len(visualizations) == 21
    visualization = kibana.visualization("6eab7cb0-fb18-11e9-84e4-078763638bf3")
    visualization.visState
    visualization.uiStateJSON
    assert visualization.index().meta.id == index_pattern.meta.id
    dashboards = list(kibana.dashboards())
    assert len(dashboards) == 1
    dashboard = kibana.dashboard("f57a7160-fb18-11e9-84e4-078763638bf3")
    dashboard.panelsJSON
    dashboard.optionsJSON
    assert len(dashboard.visualizations()) == 2
    visualization = kibana.visualization("f4a09a00-fe77-11e9-8c18-250a1adff826")
    search = visualization.related_search()
    assert search.meta.id == "search:2139a4e0-fe77-11e9-833a-0fef2d7dd143"
    assert len(list(kibana.searches())) == 1
    search = kibana.search("2139a4e0-fe77-11e9-833a-0fef2d7dd143")
    assert visualization.index().meta.id == index_pattern.meta.id


def test_translators():
    kibana = Kibana(PYBANA_INDEX)
    load_fixtures(elastic, kibana, PYBANA_INDEX)
    load_data(elastic, "pybana")
    kibana.init_config()
    translator = ElasticTranslator()
    scope = Scope(
        datetime.datetime(2019, 1, 1, tzinfo=pytz.utc),
        datetime.datetime(2019, 1, 3, tzinfo=pytz.utc),
        pytz.utc,
        kibana.config(),
    )
    for visualization in kibana.visualizations().scan():
        if visualization.visState["type"] in ("histogram", "metric", "pie", "line"):
            search = translator.translate(visualization, scope)
            if visualization.meta.id.split(":")[-1] in (
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
            ):
                response = search.execute()
                # print(visualization)
                # print(visualization.index())
                # print(search.to_dict())
                # print(response.to_dict())
                VegaTranslator().translate(visualization, response, scope)


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

    assert ds.ds_avg(0, 1) == .5
    assert ds.ds_count(0, 1) == 2
    assert ds.ds_cusum(0, 1) == [0, 1]
    assert ds.ds_derivative(0, 1)[1] == 1
    assert ds.ds_max(0, 1) == 1
    assert ds.ds_min(0, 1) == 0
    assert ds.ds_next(0, 1)[0] == 1
    assert ds.ds_prev(0, 1)[1] == 0
    assert ds.ds_sum(0, 1) == 1

    assert ds.is_variable("agg1")
    assert not ds.is_variable("xagg1")

    assert (
        ds.datasweet_eval("avg(agg1, agg2) + 1", {"1": {"value": 0}, "2": {"value": 1}})
        == 1.5
    )

    with pytest.raises(ValueError):
        tree = ast.parse("x + 1", mode="eval")
        tree = ds.DatasweetTransformer().visit(tree)
