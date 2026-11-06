# -*- coding: utf-8 -*-

import os
import sys
import unittest

BASE_DIRECTORY = os.path.join(os.path.dirname(__file__), "..")  # NOQA
sys.path.insert(0, BASE_DIRECTORY)  # NOQA

import ast  # noqa: E402
import datetime  # noqa: E402
import elasticsearch  # noqa: E402
import elasticsearch_dsl  # noqa: E402
import json  # noqa: E402
from elasticsearch import NotFoundError  # noqa: E402
from pybana import (  # noqa: E402
    Scope,
    ElasticTranslator,
    Kibana,
    VegaTranslator,
    VegaRenderer,
    VEGA_METRICS,
)
from pybana.elastic.mock_elastic_client import (  # noqa: E402
    MockElasticsearchPolluxClient,
)
from pybana.models import Config, Visualization  # noqa: E402
from pybana.translators.elastic.buckets import (  # noqa: E402
    format_from_interval,
    compute_auto_interval,
)
from pybana.elastic.elastic_client import ElasticsearchExtClient  # noqa: E402
import pytest  # noqa: E402
import pytz  # noqa: E402
from tests.connection_pool import (  # noqa: E402
    ELASTIC_CLIENTS,
    ELASTIC_ORIGINS,
    get_allowed_versions,
    get_versions_and_cache
)
from parameterized import parameterized  # noqa: E402
from pybana.elastic.elastic_client_interface import ESClientInterface  # noqa: E402

PYBANA_INDEX = ".kibana_pybana_test"

for k, v in ELASTIC_CLIENTS.items():
    elasticsearch_dsl.connections.add_connection(k, v)


def _load_fixtures(elastic, kibana, index):
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


def _load_data(elastic, index):
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


def _initialize_data(version: str, pybana_index: str):
    kibana = Kibana(index=pybana_index, using=version)
    elastic = ELASTIC_CLIENTS[version]
    elastic.indices.delete(f"{pybana_index}*")
    elastic.indices.create(f"{pybana_index}_1")
    _load_fixtures(elastic, kibana, pybana_index)
    _load_data(elastic, "pybana")
    kibana.init_config()
    kibana.init_config()
    assert kibana.config()


def _get_clients(version, start_cache, pybana_postfix):
    pybana_index = f".kibana_pybana_t_{pybana_postfix}".lower()
    _initialize_data(version, pybana_index)
    kibana = Kibana(index=pybana_index, using=version, start_cache=start_cache)
    elastic = ELASTIC_CLIENTS[version]
    assert kibana.config()
    return elastic, kibana


class TestPybana:

    @parameterized.expand(get_versions_and_cache())
    def test_client(self, version, start_cache):
        elastic, kibana = _get_clients(
            version, start_cache, f"client_{'c' if start_cache else 's'}"
        )
        assert len(list(kibana.index_patterns())) == 1
        index_pattern = kibana.index_pattern("6c172f80-fb13-11e9-84e4-078763638bf3")
        index_pattern.fields
        index_pattern.fieldFormatMap
        kibana.update_or_create_default_index_pattern(index_pattern)
        kibana.update_or_create_default_index_pattern(index_pattern)
        kibana.update_or_create_default_index_pattern(index_pattern)
        visualizations = list(kibana.visualizations().scan())
        assert len(visualizations) == 34
        visu2 = kibana.all_visualizations()
        assert len(visu2) == 34
        assert isinstance(visu2[0], Visualization)
        assert isinstance(visualizations[0], Visualization)
        visualization = kibana.visualization("6eab7cb0-fb18-11e9-84e4-078763638bf3")
        visualization.visState
        visualization.uiStateJSON
        assert visualization.index(using=elastic).meta.id == index_pattern.meta.id
        dashboards = list(kibana.dashboards())
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
        # No index in searchSourceJSON
        visualization = kibana.visualization("4a23d096-541b-4638-bb4a-441fd9ed5ef4")
        with pytest.raises(ValueError):
            visualization.index(using=elastic).meta.id

    @parameterized.expand(get_versions_and_cache())
    def test_translators(self, version, start_cache):
        _initialize_data(
            version,
            PYBANA_INDEX,  # f".kibana_pybana_translators_{'c' if start_cache else 's'}"
        )
        kibana = Kibana(
            index=PYBANA_INDEX,
            using=elasticsearch_dsl.connections.get_connection(version),
            start_cache=start_cache,
        )
        elastic = ELASTIC_CLIENTS[version]
        _load_fixtures(elastic, kibana, PYBANA_INDEX)
        _load_data(elastic, "pybana")
        assert isinstance(elastic, ESClientInterface)

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
            if visualization.visState["type"] in (
                "histogram",
                "metric",
                "pie",
                "line",
                "vega",
                "table",
            ):
                search = translator.translate(visualization, scope)
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
                    response = search.execute()
                    VegaTranslator(using=elastic).translate(
                        visualization, response, scope
                    )
                if visualization_id in (
                    "c8ce3da6-d048-4431-9ad4-dc2e74005214",
                    "420034ac-6f20-4ac9-9aa1-375703e4c768",
                    "b305f5a5-b20e-4f2c-b246-ffe9117ae32a",
                ):
                    # Elasticsearch 6 cannot run these: rate (7.11) and rare_terms (7.3).
                    if version == "v6" and visualization_id in (
                        "420034ac-6f20-4ac9-9aa1-375703e4c768",
                        "b305f5a5-b20e-4f2c-b246-ffe9117ae32a",
                    ):
                        continue
                    response = search.execute()
                    agg_dict = response.aggregations.to_dict()
                    assert "1" in agg_dict
                    assert "1-bucket" in agg_dict
                    assert "buckets" in agg_dict["1-bucket"]
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
                # Test Filters
                if visualization_id in ("2ba3534a-2d98-4510-82a5-d1ff5e19c08b",):
                    response = search.execute()
                    #
                    state = json.loads(visualization.visualization.visState)
                    assert "buckets" in response.aggregations["3"]["buckets"][0]["2"]
                    assert (
                        "f > 1"
                        in response.aggregations["3"]["buckets"][0]["2"]["buckets"]
                    )
                    assert (
                        "5-bucket"
                        in response.aggregations["3"]["buckets"][0]["2"]["buckets"][
                            "f > 1"
                        ]
                    )
                    assert (
                        "buckets"
                        in response.aggregations["3"]["buckets"][0]["2"]["buckets"][
                            "f > 1"
                        ]["5-bucket"]
                    )
                    assert (
                        's:"d" '
                        in response.aggregations["3"]["buckets"][0]["2"]["buckets"][
                            "f > 1"
                        ]["5-bucket"]["buckets"]
                    )
                    assert (
                        response.aggregations["3"]["buckets"][0]["2"]["buckets"][
                            "f > 1"
                        ]["5-bucket"]["buckets"]['s:"d" ']["5-metric"]["value"]
                        == 0
                    )

    @parameterized.expand(get_versions_and_cache())
    def test_vega_visualization(self, version: str, start_cache):
        pybana_index = f".kibana_pybana_vega_visu_{'c' if start_cache else 's'}"
        kibana = Kibana(index=pybana_index, using=version)
        elastic = ELASTIC_CLIENTS[version]

        _load_fixtures(elastic, kibana, pybana_index)
        _load_data(elastic, "pybana")
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

    @parameterized.expand(get_versions_and_cache())
    def vega_renderer_momentFormat(self, version: str, start_cache):
        pybana_postfix = (
            f".kibana_pybana_vega_render_momentfmt_{'c' if start_cache else 's'}"
        )
        _, kibana = _get_clients(
            version, start_cache=start_cache, pybana_postfix=pybana_postfix
        )

        # Test renderer MomentFormat
        visualization = kibana.visualization("33788540-f67f-11ee-ba56-1101ab5c82ed")

        spec = json.loads(visualization.visState["params"]["spec"])

        renderer = VegaRenderer("fr", "utc")
        renderer.to_svg(spec)

    def test_elastic_translator_helpers(self):
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

    def test_vega_renderer(self):
        renderer = VegaRenderer("fr", "utc")
        renderer.to_svg({"$schema": "https://vega.github.io/schema/vega/v5.json"})

    def test_datasweet(self):
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
            ds.datasweet_eval(
                "avg(agg1, agg2) + 1", {"1": {"value": 0}, "2": {"value": 1}}
            )
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

    def test_datetime(self):
        import pybana.helpers.datetime as dt

        assert dt.convert("") == ""
        assert dt.convert("Y") == "Y"
        assert dt.convert("w") is None
        assert dt.convert("llll") == "LLLL"
        assert dt.convert("[coucou]") == "[coucou]"
        assert dt.convert("Y [coucou]") == "Y [coucou]"
        assert dt.convert("[coucou] Y"), "[coucou] Y"
        assert dt.convert("[a [coucou]]"), "[a [coucou]]"

    @parameterized.expand(get_allowed_versions(versions=["v8"]) or ["ignore"])
    def test_get_es_wraps_unextended_clients(self, version):
        if version == "ignore":  # due to issue on skip on empty
            return
        assert version == "v8"
        es = ELASTIC_ORIGINS["v8"]
        kibana = Kibana(using=es, index=PYBANA_INDEX)
        odd = object()
        elasticsearch_dsl.connections.add_connection("pybana-odd", odd)
        elasticsearch_dsl.connections.add_connection(
            "pybana-raw", ELASTIC_ORIGINS["v6"]
        )
        try:
            assert kibana.get_es("pybana-odd") is odd
            wrapped_direct = kibana.get_es(es)
            assert isinstance(wrapped_direct, ElasticsearchExtClient)
            assert wrapped_direct.es is es
            wrapped_named = kibana.get_es("pybana-raw")
            assert isinstance(wrapped_named, ElasticsearchExtClient)
            assert kibana.get_es("pybana-raw") is wrapped_named
        finally:
            elasticsearch_dsl.connections.remove_connection("pybana-odd")
            elasticsearch_dsl.connections.remove_connection("pybana-raw")

    @parameterized.expand([False, True])
    def test_is_v8_and_visualization_mapping_repair(self, start_cache):
        elastic = MockElasticsearchPolluxClient()
        kibana = Kibana(using=elastic, index=".kibana", start_cache=start_cache)
        assert kibana.using is elastic
        assert kibana.is_v8() is True
        assert kibana.visualization_mapping_needs_update() is False
        assert kibana.ensure_visualization_mapping() is False

        v6 = MockElasticsearchPolluxClient()
        v6._version = "6.7.1"
        v6._version_major = 6
        assert Kibana(using=v6, index=".kibana").is_v8() is False

        elastic.memory.add_index(
            ".kibana_1",
            aliases=[".kibana"],
            mappings={
                "properties": {
                    "visualization": {"properties": {"title": {"type": "text"}}}
                }
            },
        )
        mapping = next(iter(elastic.indices.get_mapping(index=".kibana").values()))
        assert kibana._visualization_properties_from_mapping(mapping) == {
            "title": {"type": "text"}
        }
        assert kibana.visualization_mapping_needs_update() is True
        assert kibana.ensure_visualization_mapping(reindex=False) is True
        assert kibana.visualization_mapping_needs_update() is False
        kibana.init_index()
        assert list(elastic.memory.indices) == [".kibana_1"]

        typed = MockElasticsearchPolluxClient()
        typed.memory.add_index(
            "typed",
            mappings={
                "doc": {
                    "properties": {
                        "visualization": {"properties": {"title": {"type": "keyword"}}}
                    }
                }
            },
        )
        assert Kibana(using=typed, index="typed").visualization_mapping_needs_update()

        bumped = MockElasticsearchPolluxClient()
        bumped.memory.add_index("plain_1")
        Kibana(using=bumped, index="plain").init_index()
        assert "plain_2" in bumped.memory.indices
        assert bumped.indices.exists_alias(name="plain")

        incomplete = MockElasticsearchPolluxClient()
        incomplete.memory.add_index(
            ".kibana", mappings={"properties": {"type": {"type": "keyword"}}}
        )
        incomplete.index(
            ".kibana",
            "doc",
            {"type": "visualization", "visualization": {"title": "t"}},
            id="visualization:1",
        )
        repaired = Kibana(using=incomplete, index=".kibana")
        assert repaired.ensure_visualization_mapping(reindex=True) is True
        assert incomplete.get(".kibana", "doc", "visualization:1")["found"] is True
        assert repaired.visualization_mapping_needs_update() is False

    @parameterized.expand([False, True])
    def test_config_falls_back_to_same_major_version(self, start_cache):
        elastic = MockElasticsearchPolluxClient()
        elastic.memory.add_index(".kibana")
        kibana = Kibana(using=elastic, index=".kibana", start_cache=start_cache)
        with pytest.raises(NotFoundError):
            kibana.config()

        Config(config={"defaultIndex": "old"}, meta={"id": "config:6.7.1"}).save(
            index=".kibana", using=elastic
        )
        with pytest.raises(NotFoundError):
            kibana.config()

        Config(config={"defaultIndex": "abc"}, meta={"id": "config:8.0.0"}).save(
            index=".kibana", using=elastic
        )
        found = kibana.config()
        assert found.meta.id == "config:8.0.0"
        assert found.config.to_dict()["defaultIndex"] == "abc"
