# -*- coding: utf-8 -*-

import datetime
import elasticsearch
import elasticsearch_dsl
import json
import os
import pytz
import sys

BASE_DIRECTORY = os.path.join(os.path.dirname(__file__), "..")  # NOQA
sys.path.insert(0, BASE_DIRECTORY)  # NOQA

from pybana import Kibana, ElasticTranslator, Scope
from pybana.translators.elastic.buckets import (
    format_from_interval,
    compute_auto_interval,
)
from pybana.helpers import VegaRenderer

PYBANA_INDEX = ".kibana_pybana_test"
elastic = elasticsearch.Elasticsearch()
elasticsearch_dsl.connections.add_connection("default", elastic)


def loadfixtures(elastic, index):
    if elastic.indices.exists(index):
        elastic.indices.delete(index)
    datafn = os.path.join(BASE_DIRECTORY, "pybana/index.json")
    mappingsfn = os.path.join(BASE_DIRECTORY, "pybana/mappings.json")
    with open(mappingsfn) as fd:
        elastic.indices.create(index, body=json.load(fd))

    def actions():
        with open(datafn, "r") as fd:
            for line in fd:

                action = json.loads(line)
                action["_index"] = index
                yield action

    elasticsearch.helpers.bulk(elastic, actions(), refresh="wait_for")


def test_client():
    loadfixtures(elastic, PYBANA_INDEX)
    kibana = Kibana(PYBANA_INDEX)
    kibana.init_config()
    kibana.init_config()
    assert kibana.config()
    assert len(list(kibana.index_patterns())) == 1
    index_pattern = kibana.index_pattern("6c172f80-fb13-11e9-84e4-078763638bf3")
    kibana.update_or_create_default_index_pattern(index_pattern)
    kibana.update_or_create_default_index_pattern(index_pattern)
    visualizations = list(kibana.visualizations())
    assert len(visualizations) == 10
    visualization = kibana.visualization("6eab7cb0-fb18-11e9-84e4-078763638bf3")
    visualization.state()
    assert visualization.index().meta.id == index_pattern.meta.id
    dashboards = list(kibana.dashboards())
    assert len(dashboards) == 1
    dashboard = kibana.dashboard("f57a7160-fb18-11e9-84e4-078763638bf3")
    assert len(dashboard.visualizations()) == 2
    visualization = kibana.visualization("f4a09a00-fe77-11e9-8c18-250a1adff826")
    search = visualization.related_search()
    assert search.meta.id == "search:2139a4e0-fe77-11e9-833a-0fef2d7dd143"
    assert len(list(kibana.searches())) == 1
    search = kibana.search("2139a4e0-fe77-11e9-833a-0fef2d7dd143")
    assert visualization.index().meta.id == index_pattern.meta.id


def test_elastic_translator():
    loadfixtures(elastic, PYBANA_INDEX)
    kibana = Kibana(PYBANA_INDEX)
    translator = ElasticTranslator()
    scope = Scope(
        datetime.datetime(2019, 1, 1, tzinfo=pytz.utc),
        datetime.datetime(2019, 1, 2, tzinfo=pytz.utc),
        pytz.utc,
    )
    for visualization in kibana.visualizations():
        if visualization.state()["type"] in ("histogram", "metric"):
            translator.translate(visualization, scope)


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
