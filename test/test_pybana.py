# -*- coding: utf-8 -*-

import datetime
import elasticsearch
import elasticsearch_dsl
import json
import os
import pytz
import sys

BASE_DIRECTORY = os.path.join(os.path.dirname(__file__), "../src")  # NOQA
sys.path.insert(0, BASE_DIRECTORY)  # NOQA

from pybana import Kibana, ElasticTranslator, Scope

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
    assert len(visualizations) == 9
    visualization = kibana.visualization("6eab7cb0-fb18-11e9-84e4-078763638bf3")
    visualization.state()
    assert visualization.index().meta.id == index_pattern.meta.id
    dashboards = list(kibana.dashboards())
    assert len(dashboards) == 1
    dashboard = kibana.dashboard("f57a7160-fb18-11e9-84e4-078763638bf3")
    assert len(dashboard.visualizations()) == 2


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
