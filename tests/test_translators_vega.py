# -*- coding: utf-8 -*-

import datetime
import json
import os
import sys

BASE_DIRECTORY = os.path.join(os.path.dirname(__file__), "..")  # NOQA
sys.path.insert(0, BASE_DIRECTORY)  # NOQA

import unittest  # noqa: E402
from unittest.mock import patch  # noqa: E402

import pytest  # noqa: E402

from pybana.translators.vega import colormaps  # noqa: E402
from pybana.translators.vega.colormaps import (  # noqa: E402
    enforce_bounds,
    get_heatmap_color,
    get_interval_color,
    interpolate_linearly,
)
from pybana.translators.vega.constants import (  # noqa: E402
    DEFAULT_GAUGE_WIDTH,
    DEFAULT_HEIGHT,
    DEFAULT_PADDING,
    DEFAULT_PIE_WIDTH,
    DEFAULT_WIDTH,
    KIBANA_SEED_COLORS,
)
from pybana.translators.vega.vega import VegaTranslator  # noqa: E402
from pybana.translators.vega.visualization import ContextVisualization  # noqa: E402

TS = 1546300800000
DURATION_FORMATS = {"latency": {"id": "number", "params": {"pattern": "00:00:00"}}}


class DictView(dict):
    def to_dict(self):
        return self


class IndexPattern:
    def __init__(self, formats):
        self.fieldFormatMap = DictView(formats)


class Visualization:
    def __init__(self, state, formats=None, ui=None):
        self.visState = DictView(state)
        self.uiStateJSON = DictView(ui or {})
        self.formats = formats or {}

    def index(self, using=None):
        return IndexPattern(self.formats)


class KibanaConfig:
    def __init__(self, values=None):
        payload = {
            "visualization:colorMapping": "{}",
            "dateFormat": "YYYY-MM-DD",
            "dateFormat:scaled": "[]",
        }
        payload.update(values or {})
        self.config = DictView(payload)


class Scope:
    def __init__(self, config):
        self.config = config
        self.beg = datetime.datetime(2019, 1, 1, tzinfo=datetime.timezone.utc)
        self.end = datetime.datetime(2019, 1, 3, tzinfo=datetime.timezone.utc)
        self.locale = "en"


class Response:
    def __init__(self, aggregations, hits_total=4):
        self.aggregations = DictView(aggregations)
        self.hits = {"total": hits_total}

    def to_dict(self):
        return {"hits": self.hits, "aggregations": self.aggregations}

    def __getitem__(self, key):
        if key == "hits":
            return self.hits
        if key == "aggregations":
            return self.aggregations
        raise KeyError(key)


class Payload:
    def __init__(self, data):
        self.data = data

    def to_dict(self):
        return self.data


def metric_agg(agg_id, agg_type, field=None, hidden=False, params=None):
    body = {
        "id": agg_id,
        "type": agg_type,
        "schema": "metric",
        "params": dict(params or {}),
    }
    if field:
        body["params"]["field"] = field
    if hidden:
        body["hidden"] = True
    return body


def bucket_agg(agg_id, agg_type, schema, field=None, params=None):
    body = {
        "id": agg_id,
        "type": agg_type,
        "schema": schema,
        "params": dict(params or {}),
    }
    if field:
        body["params"]["field"] = field
    return body


def series_param(
    agg_id, axis, label, series_type="line", mode="normal", line_width=2, **extra
):
    param = {
        "data": {"id": agg_id, "label": label},
        "valueAxis": axis,
        "type": series_type,
        "mode": mode,
        "showCircles": True,
        "drawLinesBetweenPoints": True,
        "lineWidth": line_width,
    }
    param.update(extra)
    return param


def value_axis(
    axis_id, mode="normal", show=True, position="left", title="Y", scale_extra=None
):
    scale = {"mode": mode}
    scale.update(scale_extra or {})
    return {
        "id": axis_id,
        "show": show,
        "position": position,
        "title": {"text": title},
        "scale": scale,
    }


def category_axis(show=True, rotate=0, position="bottom"):
    return {"show": show, "position": position, "labels": {"rotate": rotate}}


def make(vis_type, aggs, params, formats=None, ui=None, colors=None):
    values = {}
    if colors is not None:
        values["visualization:colorMapping"] = json.dumps(colors)
    config = KibanaConfig(values)
    visualization = Visualization(
        {"type": vis_type, "aggs": aggs, "params": params}, formats, ui
    )
    state = ContextVisualization(visualization, config, using="unit")
    return state, Scope(config), VegaTranslator(using="unit")


def pie_params(donut=False, tooltip=True, labels=True, position="right"):
    return {
        "legendPosition": position,
        "isDonut": donut,
        "addTooltip": tooltip,
        "labels": {"show": labels},
    }


class TestVegaTranslator(unittest.TestCase):
    def test_conf_width_follows_chart_type(self):
        pie, _, translator = make("pie", [], pie_params())
        gauge, _, _ = make("gauge", [], {})
        goal, _, _ = make("goal", [], {})
        histogram, _, _ = make("histogram", [], {})
        self.assertEqual(translator.conf(pie)["width"], DEFAULT_PIE_WIDTH)
        self.assertEqual(translator.conf(gauge)["width"], DEFAULT_GAUGE_WIDTH)
        self.assertEqual(translator.conf(goal)["width"], DEFAULT_GAUGE_WIDTH)
        self.assertEqual(translator.conf(histogram)["width"], DEFAULT_WIDTH)
        self.assertEqual(translator.conf(histogram)["height"], DEFAULT_HEIGHT)
        self.assertEqual(translator.conf(histogram)["padding"], DEFAULT_PADDING)

    def test_pie_singleton(self):
        state, scope, translator = make(
            "pie", [metric_agg("1", "count")], pie_params(donut=True)
        )
        spec = translator.translate(state_visualization(state), Response({}), scope)
        self.assertEqual(spec["width"], DEFAULT_PIE_WIDTH)
        self.assertEqual(
            spec["data"][0]["values"], [{"y": 4, "metric": "Count", "group": "all"}]
        )
        self.assertEqual(spec["scales"][-1]["domain"], ["all"])
        self.assertEqual(spec["legends"][0]["fill"], "groupcolor")
        self.assertEqual(spec["legends"][0]["orient"], "right")
        self.assertEqual(
            spec["marks"][0]["encode"]["enter"]["innerRadius"],
            {"signal": "width * .35"},
        )
        self.assertIn("tooltip", spec["marks"][0]["encode"]["enter"])
        self.assertEqual(spec["marks"][1]["type"], "text")
        self.assertNotIn("axes", spec)

    def test_pie_buckets_duration_and_plain(self):
        duration_state, scope, translator = make(
            "pie",
            [
                metric_agg("1", "avg", field="latency"),
                bucket_agg("2", "terms", "segment", field="host"),
            ],
            pie_params(tooltip=True, labels=True),
            formats=DURATION_FORMATS,
        )
        response = Response(
            {
                "2": {
                    "buckets": [
                        {"key": "a", "1": {"value": 3600}},
                        {"key": "b", "1": {"value": 7200}},
                    ]
                }
            }
        )
        spec = translator.translate(
            state_visualization(duration_state), response, scope
        )
        values = spec["data"][0]["values"]
        self.assertEqual(values[0]["tooltip"]["a"], "1:0:0 (33.33%)")
        self.assertEqual(values[1]["label"], "b (66.67%)")
        self.assertEqual(values[0]["segment"], 0)
        self.assertIn("tooltip", spec["marks"][0]["encode"]["enter"])
        self.assertEqual(
            spec["marks"][0]["encode"]["enter"]["innerRadius"], {"value": 0}
        )

        plain_state, plain_scope, _ = make(
            "pie",
            [
                metric_agg("1", "count"),
                bucket_agg("2", "terms", "segment", field="host"),
            ],
            pie_params(tooltip=False, labels=False),
        )
        plain = translator.data(
            {},
            plain_state,
            Response(
                {
                    "2": {
                        "buckets": [
                            {"key": "a", "doc_count": 1},
                            {"key": "b", "doc_count": 3},
                        ]
                    }
                }
            ),
            plain_scope,
        )
        self.assertEqual(plain["data"][0]["values"][0]["tooltip"]["a"], "1/4 (25.00%)")
        self.assertEqual(plain["data"][0]["values"][0]["label"], "a (25.00%)")

        zero = translator.data_pie(
            {},
            plain_state,
            Response({"2": {"buckets": [{"key": "a", "doc_count": 0}]}}),
        )
        self.assertEqual(zero["data"][0]["values"][0]["tooltip"]["a"], "0/0 (0.00%)")
        marks = translator.marks(
            translator.data(
                {},
                plain_state,
                Response({"2": {"buckets": [{"key": "a", "doc_count": 1}]}}),
                plain_scope,
            ),
            plain_state,
            None,
        )
        self.assertEqual(len(marks["marks"]), 1)

    def test_line_date_groups_and_duration(self):
        aggs = [
            metric_agg("avg", "avg", field="latency"),
            metric_agg("cnt", "count", field="latency"),
            bucket_agg(
                "seg",
                "date_histogram",
                "segment",
                field="@timestamp",
                params={"interval": "auto", "customLabel": "Time"},
            ),
            bucket_agg("grp", "terms", "group", field="region"),
        ]
        params = {
            "addTooltip": True,
            "legendPosition": "left",
            "seriesParams": [
                series_param(
                    "avg", "ValueAxis-1", "Latency", line_width=3, showCircles=False
                ),
                series_param(
                    "cnt",
                    "ValueAxis-1",
                    "It's",
                    drawLinesBetweenPoints=False,
                    line_width=0,
                ),
            ],
            "valueAxes": [value_axis("ValueAxis-1", title="Latency")],
            "categoryAxes": [category_axis(rotate=20)],
        }
        state, scope, translator = make(
            "line",
            aggs,
            params,
            formats=DURATION_FORMATS,
            colors={"Latency": "#123456"},
        )
        response = Response(
            {
                "seg": {
                    "buckets": [
                        {
                            "key": TS,
                            "grp": {
                                "buckets": [
                                    {
                                        "key": "east",
                                        "doc_count": 4,
                                        "avg": {"value": 3661},
                                    },
                                    {"key": 0, "doc_count": 1, "avg": {"value": 60}},
                                ]
                            },
                        }
                    ]
                }
            }
        )
        spec = translator.translate(state_visualization(state), response, scope)
        values = spec["data"][0]["values"]
        self.assertEqual(len(values), 4)
        east = [
            row
            for row in values
            if row["group"] == "east" and row["metric"] == "Latency"
        ][0]
        blank = [
            row for row in values if row["group"] == "" and row["metric"] == "It's"
        ][0]
        self.assertEqual(east["x"], "2019-01-01")
        self.assertEqual(east["key"], TS)
        self.assertEqual(east["x_label"], "Time")
        self.assertEqual(east["tooltip"]["Latency"], "1:1:1")
        self.assertEqual(east["tooltip"]["group"], "east")
        self.assertEqual(east["y1"], 3661)
        self.assertNotIn("group", blank["tooltip"])
        self.assertEqual(blank["tooltip"]["It's"], 1)
        self.assertEqual(spec["scales"][0]["type"], "point")
        self.assertEqual(spec["scales"][0]["domain"]["sort"]["field"], "key")
        metric_scale = [
            scale for scale in spec["scales"] if scale["name"] == "metriccolor"
        ][0]
        self.assertEqual(metric_scale["domain"], ["Latency", "It's"])
        self.assertEqual(metric_scale["range"][0], "#123456")
        self.assertEqual(metric_scale["range"][1], KIBANA_SEED_COLORS[1])
        group_scale = [
            scale for scale in spec["scales"] if scale["name"] == "groupcolor"
        ][0]
        self.assertEqual(group_scale["domain"][0], "east")
        self.assertEqual(group_scale["range"][0], KIBANA_SEED_COLORS[0])
        self.assertEqual(spec["axes"][0]["labelAngle"], 340)
        self.assertIn("encode", spec["axes"][1])
        self.assertEqual(spec["legends"][0]["fill"], "groupcolor")
        self.assertEqual(spec["legends"][0]["columns"], 1)
        self.assertFalse(spec["marks"][0]["clip"])
        symbol = spec["marks"][0]["marks"][1]
        self.assertIn("tooltip", symbol["encode"]["enter"])
        tests = symbol["encode"]["enter"]["size"]
        self.assertTrue(any(item.get("test") == "false" for item in tests))
        self.assertIn({"value": 0}, tests)
        self.assertTrue(any("It\\'" in item.get("test", "") for item in tests))

    def test_histogram_side_by_side(self):
        aggs = [
            metric_agg("cnt", "count"),
            metric_agg("sum", "sum", field="bytes"),
            bucket_agg("seg", "terms", "segment", field="host"),
        ]
        params = {
            "addTooltip": False,
            "legendPosition": "bottom",
            "seriesParams": [
                series_param("cnt", "ValueAxis-1", "Count", series_type="histogram"),
                series_param("sum", "ValueAxis-1", "Bytes", series_type="histogram"),
            ],
            "valueAxes": [value_axis("ValueAxis-1")],
            "categoryAxes": [category_axis(rotate=0)],
        }
        state, scope, translator = make("histogram", aggs, params)
        spec = translator.translate(
            state_visualization(state),
            Response(
                {
                    "seg": {
                        "buckets": [
                            {
                                "key": "web",
                                "key_as_string": "web-1",
                                "doc_count": 2,
                                "sum": {"value": 5},
                            }
                        ]
                    }
                }
            ),
            scope,
        )
        self.assertEqual(spec["scales"][0]["type"], "band")
        self.assertNotIn("sort", spec["scales"][0]["domain"])
        self.assertEqual(spec["data"][0]["values"][0]["x"], "web-1")
        self.assertEqual(spec["legends"][0]["fill"], "metriccolor")
        self.assertEqual(spec["legends"][0]["columns"], 10)
        self.assertNotIn("labelAngle", spec["axes"][0])
        self.assertNotIn("format", spec["axes"][1])
        self.assertNotIn("encode", spec["axes"][1])
        rect = spec["marks"][0]["marks"][0]
        self.assertEqual(rect["encode"]["enter"]["x"]["field"], "metric")
        self.assertEqual(rect["encode"]["enter"]["fill"]["scale"], "metriccolor")
        self.assertEqual(
            rect["encode"]["enter"]["y2"], {"scale": "ValueAxis-1", "value": 0}
        )
        self.assertNotIn("tooltip", rect["encode"]["enter"])

    def test_stacked_axes_hidden_metrics_and_scale_bounds(self):
        aggs = [
            metric_agg("hidden", "avg", field="latency", hidden=True),
            metric_agg("top", "top_hits", params={"field": "name", "aggregate": "min"}),
            metric_agg("zero", "sum", field="bytes"),
            metric_agg("total", "sum", field="latency"),
            metric_agg("other", "cardinality", field="host"),
            bucket_agg("seg", "terms", "segment", params={}),
        ]
        params = {
            "addTooltip": True,
            "legendPosition": "right",
            "seriesParams": [
                series_param(
                    "hidden",
                    "ValueAxis-1",
                    "Hidden",
                    series_type="histogram",
                    mode="stacked",
                ),
                series_param(
                    "top", "ValueAxis-1", "Top", series_type="histogram", mode="stacked"
                ),
                series_param(
                    "zero",
                    "ValueAxis-1",
                    "Zero",
                    series_type="histogram",
                    mode="stacked",
                ),
                series_param(
                    "total",
                    "ValueAxis-1",
                    "Total",
                    series_type="histogram",
                    mode="stacked",
                ),
                series_param("other", "ValueAxis-2", "Other", series_type="line"),
            ],
            "valueAxes": [
                value_axis("ValueAxis-1", mode="percentage", title="Share"),
                value_axis(
                    "ValueAxis-2",
                    show=False,
                    position="right",
                    scale_extra={"min": 0, "max": 10},
                ),
            ],
            "categoryAxes": [category_axis(show=False)],
        }
        state, scope, translator = make(
            "histogram",
            aggs,
            params,
            formats=DURATION_FORMATS,
            colors={"Total": "#abcdef"},
        )
        bucket = {
            "key": "only",
            "doc_count": 3,
            "zero": {"value": 0},
            "total": {"value": 3661},
            "other": {"value": 1},
            "top": {"hits": {"hits": []}},
        }
        response = Response({"seg": {"buckets": [bucket]}})
        spec = translator.data({}, state, response, scope)
        self.assertEqual(spec["data"][0]["transform"][0]["offset"], "normalize")
        self.assertEqual(bucket["hidden"], {"value": None})
        rows = spec["data"][0]["values"]
        self.assertFalse(any(row["metric"] == "Top" for row in rows))
        self.assertTrue(
            any(row["metric"] == "Other" and row.get("y1") == 0 for row in rows)
        )
        self.assertTrue(
            any(row["metric"] == "Total" and row["y1"] == 3661 for row in rows)
        )
        self.assertEqual(rows[0]["x_label"], "x")
        scaled = translator.scales(spec, state)
        metric_scale = [
            item for item in scaled["scales"] if item["name"] == "metriccolor"
        ][0]
        self.assertIn("Total", metric_scale["domain"])
        self.assertNotIn("Zero", metric_scale["domain"])
        self.assertNotIn("Hidden", metric_scale["domain"])
        self.assertIn("#abcdef", metric_scale["range"])
        y_scales = [
            item for item in scaled["scales"] if item["name"].startswith("ValueAxis")
        ]
        self.assertEqual(y_scales[0]["domain"]["field"], "y1|1")
        self.assertFalse(y_scales[1]["nice"])
        self.assertEqual(y_scales[1]["domain"], [0, 10])
        axes = translator.axes({"axes": []}, state)
        self.assertEqual(axes["axes"][0]["format"], ".0%")
        self.assertTrue(axes["axes"])
        legends = translator.legends({}, state)
        self.assertEqual(legends["legends"][0]["columns"], 1)
        marks = translator.marks(scaled, state, response)
        self.assertEqual(
            marks["marks"][0]["marks"][0]["encode"]["enter"]["x"]["field"], "axis"
        )
        self.assertTrue(marks["marks"][-1]["clip"])
        self.assertIn("tooltip", marks["marks"][-1]["marks"][1]["encode"]["enter"])

    def test_groups_stacked_and_metric_stack_offsets(self):
        group_aggs = [
            metric_agg("cnt", "count"),
            bucket_agg("seg", "date_range", "segment", field="t"),
            bucket_agg("grp", "terms", "group", field="region"),
        ]
        percentage_state, scope, translator = make(
            "histogram",
            group_aggs,
            {
                "addTooltip": False,
                "legendPosition": "top",
                "seriesParams": [
                    series_param(
                        "cnt",
                        "ValueAxis-1",
                        "Count",
                        series_type="histogram",
                        mode="stacked",
                    )
                ],
                "valueAxes": [value_axis("ValueAxis-1", mode="percentage")],
                "categoryAxes": [category_axis()],
            },
        )
        response = Response(
            {
                "seg": {
                    "buckets": [
                        {
                            "key": 1,
                            "grp": {"buckets": [{"key": "east", "doc_count": 2}]},
                        }
                    ]
                }
            }
        )
        grouped = translator.data_line_bar({}, percentage_state, response, scope)
        self.assertEqual(grouped["data"][0]["transform"][0]["offset"], "normalize")
        self.assertNotIn("sort", grouped["data"][0]["transform"][0])
        zero_state, _, _ = make(
            "histogram",
            group_aggs,
            {
                "seriesParams": [
                    series_param(
                        "cnt",
                        "ValueAxis-1",
                        "Count",
                        series_type="histogram",
                        mode="stacked",
                    )
                ],
                "valueAxes": [value_axis("ValueAxis-1", mode="normal")],
            },
        )
        zeroed = translator.data_line_bar({}, zero_state, response, scope)
        self.assertEqual(zeroed["data"][0]["transform"][0]["offset"], "zero")
        legends = translator.legends_line_bar({}, percentage_state)
        self.assertEqual(legends["legends"][0]["columns"], 10)
        self.assertEqual(legends["legends"][0]["orient"], "top")
        scale = translator._scale_x(percentage_state)
        self.assertEqual(scale["domain"]["sort"]["field"], "key")

        metric_state, _, _ = make(
            "line",
            [metric_agg("a", "count"), metric_agg("b", "sum", field="bytes")],
            {
                "seriesParams": [
                    series_param("a", "ValueAxis-1", "A", mode="stacked"),
                    series_param("b", "ValueAxis-1", "B", mode="normal"),
                ],
                "valueAxes": [value_axis("ValueAxis-1", mode="normal")],
            },
        )
        stacked = translator.data_line_bar(
            {},
            metric_state,
            Response({"b": {"value": 1}}),
            scope,
        )
        self.assertEqual(
            stacked["data"][0]["transform"][0]["sort"], {"field": "metric"}
        )
        self.assertEqual(stacked["data"][0]["transform"][0]["offset"], "zero")
        no_date, _, _ = make(
            "line",
            [
                metric_agg("a", "count"),
                bucket_agg("seg", "terms", "segment", field="host"),
                bucket_agg("extra", "filters", "segment", field="host"),
            ],
            {"seriesParams": [series_param("a", "ValueAxis-1", "A")], "valueAxes": []},
        )
        plain = translator.data_line_bar(
            {},
            no_date,
            Response(
                {
                    "seg": {
                        "buckets": [
                            {"key": "h", "doc_count": 1, "extra": {"buckets": []}}
                        ]
                    }
                }
            ),
            scope,
        )
        self.assertIsNone(plain["data"][0].get("transform"))
        self.assertNotIn("sort", translator._scale_x(no_date)["domain"])

    def test_bucket_keys_empty_label_and_missing_series(self):
        state, scope, translator = make(
            "metric",
            [
                metric_agg("cnt", "count"),
                bucket_agg("seg", "terms", "segment", field="host"),
            ],
            {},
        )
        keyed = translator.data_line_bar(
            {},
            state,
            Response(
                {
                    "seg": {
                        "buckets": [
                            {"key": 5, "key_as_string": "", "doc_count": 2},
                            {"key": 6, "key_as_string": "six", "doc_count": 1},
                        ]
                    }
                }
            ),
            scope,
        )
        xs = [row["x"] for row in keyed["data"][0]["values"]]
        self.assertEqual(xs, [5, "six"])
        self.assertEqual(keyed["data"][0]["values"][0]["x_label"], "host")
        self.assertNotIn("axis", keyed["data"][0]["values"][0])
        bare, _, _ = make(
            "metric",
            [metric_agg("cnt", "count")],
            {},
        )
        solo = translator.data_line_bar({}, bare, Response({}), scope)
        self.assertEqual(solo["data"][0]["values"][0]["x"], "all")
        self.assertEqual(solo["data"][0]["values"][0]["tooltip"]["x"], "all")
        self.assertEqual(
            translator._get_node_key(
                {"key": TS}, {"type": "date_histogram"}, "YYYY-MM-DD", "en"
            ),
            "2019-01-01",
        )
        self.assertEqual(translator.axes({}, bare), {})
        self.assertIsNone(
            translator.legends({}, make("gauge", [], {})[0]).get("legends")
        )

    def test_histogram_mark_variants(self):
        translator = VegaTranslator(using="unit")
        grouped, _, _ = make(
            "histogram",
            [
                metric_agg("cnt", "count"),
                bucket_agg("seg", "terms", "segment", field="host"),
                bucket_agg("grp", "terms", "group", field="region"),
            ],
            {
                "addTooltip": False,
                "seriesParams": [
                    series_param("cnt", "ValueAxis-1", "Count", series_type="histogram")
                ],
                "valueAxes": [value_axis("ValueAxis-1")],
            },
        )
        marks = translator._marks_histogram(grouped, grouped.valueaxes()[0])
        self.assertEqual(marks[0]["scales"][0]["domain"]["field"], "group")
        self.assertEqual(
            marks[0]["marks"][0]["encode"]["enter"]["fill"]["field"], "group"
        )

        side, _, _ = make(
            "histogram",
            [metric_agg("a", "count"), metric_agg("b", "sum", field="bytes")],
            {
                "addTooltip": True,
                "seriesParams": [
                    series_param("a", "ValueAxis-1", "A", series_type="histogram"),
                    series_param("b", "ValueAxis-1", "B", series_type="histogram"),
                ],
                "valueAxes": [value_axis("ValueAxis-1")],
            },
        )
        side_marks = translator._marks_histogram(side, side.valueaxes()[0])
        self.assertEqual(side_marks[0]["scales"][0]["domain"]["field"], "metric")
        self.assertIn("tooltip", side_marks[0]["marks"][0]["encode"]["enter"])

        single, _, _ = make(
            "histogram",
            [metric_agg("a", "count")],
            {
                "addTooltip": False,
                "seriesParams": [
                    series_param(
                        "a", "ValueAxis-1", "A", series_type="histogram", mode="stacked"
                    )
                ],
                "valueAxes": [value_axis("ValueAxis-1")],
            },
        )
        single_marks = translator._marks_histogram(single, single.valueaxes()[0])
        enter = single_marks[0]["marks"][0]["encode"]["enter"]
        self.assertEqual(enter["x"]["field"], "x")
        self.assertEqual(enter["y"]["field"], "y1")
        self.assertEqual(enter["y2"], {"scale": "ValueAxis-1", "value": 0})

    def test_line_mark_variants(self):
        translator = VegaTranslator(using="unit")
        state, _, _ = make(
            "line",
            [
                metric_agg("a", "avg", field="latency"),
                metric_agg("b", "sum", field="bytes"),
                bucket_agg("grp", "terms", "group", field="region"),
            ],
            {
                "addTooltip": False,
                "seriesParams": [
                    series_param(
                        "a", "ValueAxis-1", "A's", mode="stacked", line_width=None
                    ),
                    series_param(
                        "b",
                        "ValueAxis-2",
                        "B",
                        mode="normal",
                        showCircles=False,
                        drawLinesBetweenPoints=False,
                    ),
                ],
                "valueAxes": [
                    value_axis("ValueAxis-1", scale_extra={"min": 0}),
                    value_axis("ValueAxis-2", scale_extra={"min": 1, "max": 2}),
                ],
            },
            formats=DURATION_FORMATS,
        )
        left = translator._marks_line(state, state.valueaxes()[0])
        self.assertTrue(left[0]["clip"])
        self.assertEqual(left[0]["from"]["facet"]["groupby"], "group")
        line = left[0]["marks"][0]["encode"]["enter"]
        self.assertEqual(line["y"]["field"], "y1|1")
        self.assertEqual(line["stroke"]["scale"], "groupcolor")
        self.assertEqual(line["strokeWidth"][0]["value"], 4)
        self.assertIn("A\\'", line["strokeWidth"][0]["test"])
        symbol = left[0]["marks"][1]["encode"]["enter"]
        self.assertNotIn("tooltip", symbol)
        right = translator._marks_line(state, state.valueaxes()[1])
        self.assertEqual(right[0]["from"]["facet"]["groupby"], "group")
        self.assertEqual(
            right[0]["marks"][0]["encode"]["enter"]["strokeWidth"][0]["test"], "false"
        )
        self.assertEqual(
            right[0]["marks"][1]["encode"]["enter"]["size"][0]["test"], "false"
        )

        plain, _, _ = make(
            "line",
            [metric_agg("a", "count")],
            {
                "addTooltip": True,
                "seriesParams": [series_param("a", "ValueAxis-1", "A")],
                "valueAxes": [value_axis("ValueAxis-1")],
            },
        )
        marks = translator._marks_line(plain, plain.valueaxes()[0])
        self.assertFalse(marks[0]["clip"])
        self.assertEqual(marks[0]["from"]["facet"]["groupby"], "metric")
        self.assertEqual(marks[0]["marks"][0]["encode"]["enter"]["y"]["field"], "y1")
        self.assertEqual(
            marks[0]["marks"][0]["encode"]["enter"]["stroke"]["scale"], "metriccolor"
        )
        self.assertIn("tooltip", marks[0]["marks"][1]["encode"]["enter"])

    def test_metric_error_is_raised_when_visible(self):
        state, scope, translator = make(
            "line",
            [
                metric_agg("avg", "avg", field="latency"),
                bucket_agg("seg", "terms", "segment", field="h"),
            ],
            {
                "seriesParams": [series_param("avg", "ValueAxis-1", "Latency")],
                "valueAxes": [value_axis("ValueAxis-1")],
            },
        )
        with pytest.raises(KeyError):
            translator.data_line_bar(
                {},
                state,
                Response({"seg": {"buckets": [{"key": "a", "doc_count": 1}]}}),
                scope,
            )

    def test_gauge_goal_and_fully_hidden(self):
        gauge_params = {
            "gauge": {
                "colorsRange": [
                    {"from": 0, "to": 50, "color": "green"},
                    {"from": 50, "to": 100},
                ],
                "colorSchema": "Greens",
                "invertColors": True,
                "labels": {"show": True},
                "style": {"subText": "of quota"},
                "percentageMode": True,
                "scale": {"show": True},
            }
        }
        state, scope, translator = make(
            "gauge",
            [
                metric_agg("cnt", "count", hidden=True),
                metric_agg(
                    "total", "sum", field="bytes", params={"numeralFormat": ".0f"}
                ),
            ],
            gauge_params,
        )
        spec = translator.translate(
            state_visualization(state),
            Response({"total": {"value": 25}}, hits_total=9),
            scope,
        )
        self.assertEqual(spec["width"], DEFAULT_GAUGE_WIDTH)
        self.assertEqual(spec_signal(spec, "mainValue"), "25")
        self.assertIn("%", spec["marks"][2]["encode"]["update"]["text"]["signal"])
        self.assertEqual(
            spec["marks"][0]["encode"]["enter"]["fill"]["value"], "rgb(235,235,235)"
        )
        self.assertEqual(spec["marks"][1]["encode"]["update"]["fill"]["value"], "green")
        self.assertTrue(any(mark.get("name") == "legend" for mark in spec["marks"]))
        self.assertTrue(any(mark.get("name") == "sub_text" for mark in spec["marks"]))
        self.assertGreater(len(spec["data"]), 1)
        self.assertNotIn("legends", spec)

        goal_state, _, _ = make(
            "goal",
            [metric_agg("total", "sum", field="bytes")],
            {
                "gauge": {
                    "colorsRange": [{"from": 0, "to": 10, "color": "blue"}],
                    "colorSchema": "Blues",
                    "labels": {"show": False},
                    "style": {},
                }
            },
        )
        goal = translator.translate(
            state_visualization(goal_state),
            Response({"total": {"value": -1}}),
            scope,
        )
        self.assertEqual(
            goal["marks"][0]["encode"]["enter"]["fill"]["value"], "rgb(0,0,0)"
        )
        self.assertEqual(goal["marks"][1]["encode"]["update"]["fill"]["value"], "black")
        self.assertNotIn("%", goal["marks"][2]["encode"]["update"]["text"]["signal"])
        self.assertTrue(
            goal["marks"][2]["encode"]["update"]["text"]["signal"].startswith("'")
        )
        self.assertFalse(any(mark.get("name") == "legend" for mark in goal["marks"]))
        self.assertIn(
            "outerRadius - outerRadius * 0.05",
            spec_signal(goal, "outerBackgroundRadius"),
        )

        hidden_state, _, _ = make(
            "gauge",
            [metric_agg("avg", "avg", field="latency", hidden=True)],
            gauge_params,
            formats=DURATION_FORMATS,
        )
        hidden = translator.translate(
            state_visualization(hidden_state), Response({}), scope
        )
        self.assertNotIn("marks", hidden)
        self.assertNotIn("signals", hidden)

        labeled, _, _ = make(
            "goal",
            [metric_agg("total", "sum", field="bytes")],
            {
                "gauge": {
                    "colorsRange": [{"from": 0, "to": 10, "color": "blue"}],
                    "colorSchema": "Blues",
                    "labels": {"show": True},
                    "style": {},
                }
            },
        )
        labeled_spec = translator.marks_gauge(
            {"data": [{"values": [{"y": 1}]}]}, labeled, None
        )
        self.assertTrue(
            any(mark.get("name") == "legend" for mark in labeled_spec["marks"])
        )
        self.assertFalse(
            any(mark.get("name") == "sub_text" for mark in labeled_spec["marks"])
        )

    def test_translate_vega_and_empty_image_urls(self):
        translator = VegaTranslator(using="unit")
        pixel = "data:image/png;base64,"
        spec = {
            "data": {"name": "table", "url": {"index": "logs"}},
            "width": 12,
            "marks": [
                "skip",
                {"type": "rect"},
                {"type": "image"},
                {"type": "image", "encode": "bad"},
                {
                    "type": "image",
                    "encode": {"enter": ["nope"], "update": {"url": None}},
                },
                {"type": "image", "encode": {"enter": {"url": {"value": None}}}},
                {"type": "image", "encode": {"enter": {"url": {"value": "  "}}}},
                {"type": "image", "encode": {"enter": {"url": {"value": ""}}}},
                {"type": "image", "encode": {"enter": {"url": {"value": 0}}}},
                {"type": "image", "encode": {"enter": {"url": "http://literal"}}},
                {"type": "image", "encode": {"enter": {"url": {"value": "http://ok"}}}},
                {
                    "type": "image",
                    "encode": {"update": {"url": {"field": "icon", "value": "x"}}},
                },
                {
                    "type": "group",
                    "marks": [
                        {"type": "image", "encode": {"enter": {"url": {"value": ""}}}}
                    ],
                },
                {"marks": None},
            ],
        }
        visualization = Visualization(
            {"type": "vega", "params": {"spec": json.dumps(spec)}}
        )
        translated = translator.translate(visualization, Payload({"rows": [1]}), None)
        self.assertEqual(translated["width"], 12)
        self.assertEqual(translated["height"], DEFAULT_HEIGHT)
        self.assertEqual(translated["padding"], DEFAULT_PADDING)
        self.assertNotIn("url", translated["data"])
        self.assertEqual(translated["data"]["values"]["rows"], [1])
        self.assertTrue(
            translated["marks"][5]["encode"]["enter"]["url"]["value"].startswith(pixel)
        )
        self.assertEqual(translated["marks"][8]["encode"]["enter"]["url"]["value"], 0)
        self.assertEqual(
            translated["marks"][9]["encode"]["enter"]["url"], "http://literal"
        )
        self.assertEqual(
            translated["marks"][10]["encode"]["enter"]["url"]["value"], "http://ok"
        )
        self.assertIn(
            "datum['icon']",
            translated["marks"][11]["encode"]["update"]["url"]["signal"],
        )
        self.assertTrue(
            translated["marks"][12]["marks"][0]["encode"]["enter"]["url"][
                "value"
            ].startswith(pixel)
        )

        listed = {
            "data": [{"name": "a", "url": "http://a"}, {"name": "b"}],
            "height": 3,
            "padding": 1,
            "width": 2,
        }
        listed_vis = Visualization(
            {"type": "vega", "params": {"spec": json.dumps(listed)}}
        )
        listed_out = translator.translate_vega(
            listed_vis, [Payload({"i": 0}), Payload({"i": 1})], None
        )
        self.assertEqual(listed_out["data"][0]["values"], {"i": 0})
        self.assertNotIn("url", listed_out["data"][0])
        self.assertEqual(listed_out["data"][1], {"name": "b"})
        self.assertEqual(listed_out["height"], 3)
        translator._fix_empty_image_urls(None)

    def test_duration_helper(self):
        translator = VegaTranslator(using="unit")
        self.assertEqual(translator._format_duration(3661), "1:1:1")
        state, _, _ = make(
            "line",
            [
                metric_agg("cnt", "count", field="latency"),
                metric_agg("avg", "avg", field="latency"),
            ],
            {"seriesParams": [], "valueAxes": []},
            formats=DURATION_FORMATS,
        )

        class Metric:
            aggtype = "count"

        class Average:
            aggtype = "avg"

        self.assertFalse(
            translator._is_duration_bucket(state, state.metric_aggs()[0], Metric())
        )
        self.assertTrue(
            translator._is_duration_bucket(state, state.metric_aggs()[1], Average())
        )
        plain, _, _ = make("line", [metric_agg("avg", "avg", field="bytes")], {})
        self.assertFalse(
            translator._is_duration_bucket(plain, plain.metric_aggs()[0], Average())
        )


class TestColormaps(unittest.TestCase):
    def test_bounds_and_interpolation(self):
        self.assertEqual(enforce_bounds(-0.2), 0)
        self.assertEqual(enforce_bounds(0.4), 0.4)
        self.assertEqual(enforce_bounds(1.2), 1)
        self.assertEqual(
            interpolate_linearly(
                0, [[0, [-0.2, 0.4, 1.2]], [0.5, [0, 0, 0]], [1, [1, 1, 1]]]
            ),
            [0, 0.4, 1],
        )
        self.assertEqual(
            interpolate_linearly(1, [[0, [0, 0, 0]], [1, [1, 0, 0]]]), [1, 0, 0]
        )

    def test_heatmap_and_interval_colors(self):
        self.assertTrue(get_heatmap_color(0, "Blues").startswith("rgb("))
        self.assertTrue(get_heatmap_color("0.5", "Greens").startswith("rgb("))
        self.assertNotEqual(
            get_heatmap_color(0, "Yellow to Red"), get_heatmap_color(1, "Yellow to Red")
        )
        self.assertEqual(
            get_interval_color("Greys", 0, 4), get_heatmap_color(0, "Greys")
        )
        self.assertEqual(
            get_interval_color("Greys", 0, 4, inverted=True),
            get_heatmap_color(1, "Greys"),
        )
        self.assertEqual(
            get_interval_color("Green to Red", 1, 4),
            get_heatmap_color(0.25, "Green to Red"),
        )
        with pytest.raises(TypeError):
            get_heatmap_color("nope", "Blues")
        for value in (-0.01, 1.01):
            with pytest.raises(ValueError):
                get_heatmap_color(value, "Blues")
        with patch.dict(colormaps.COLORMAPS, {"Empty": None}):
            with pytest.raises(KeyError):
                get_heatmap_color(0.2, "Empty")


class TestContextVisualization(unittest.TestCase):
    def test_percentile_ranks_expand_and_labels(self):
        ranks = metric_agg(
            "p", "percentile_ranks", field="bytes", params={"values": [50, 95]}
        )
        state, _, _ = make(
            "metric",
            [ranks, bucket_agg("seg", "terms", "bucket", field="host")],
            {},
        )
        expanded = state.metric_aggs()
        self.assertEqual([agg["rank"] for agg in expanded], [50, 95])
        self.assertEqual([agg["iter"] for agg in expanded], [0, 1])
        self.assertEqual(state.metric_label(expanded[1]), "Percentile rank 95 - bytes")
        self.assertEqual(state.bucket_aggs(), [state.get_agg("seg")])
        self.assertEqual(state.segment_aggs(), [])
        self.assertTrue(state.singleton())
        self.assertTrue(make("metric", [metric_agg("1", "count")], {})[0].singleton())

    def test_pie_gauge_and_table_metric_labels(self):
        labeled = metric_agg(
            "1", "sum", field="bytes", params={"customLabel": "Revenue"}
        )
        state, _, _ = make("pie", [labeled], {})
        self.assertEqual(state.metric_label(labeled), "Revenue")

        expectations = (
            ("cardinality", "Unique count of host"),
            ("max", "Maximum of host"),
            ("min", "Minimum of host"),
            ("avg", "Average of host"),
        )
        for agg_type, expected in expectations:
            agg = metric_agg("1", agg_type, field="host")
            chart, _, _ = make("goal", [agg], {})
            self.assertEqual(chart.metric_label(agg), expected)

        table, _, _ = make(
            "table",
            [
                metric_agg("s", "sum", field="bytes"),
                metric_agg("f", "datasweet_formula", params={"formula": "a+b"}),
                metric_agg(
                    "b",
                    "sum_bucket",
                    params={
                        "customMetric": {
                            "id": "inner",
                            "type": "count",
                            "schema": "metric",
                            "params": {},
                        },
                        "customBucket": {"type": "date_histogram"},
                    },
                ),
            ],
            {},
        )
        self.assertEqual(table.metric_label(table.get_agg("s")), "sum - bytes")
        self.assertEqual(
            table.metric_label(table.get_agg("f")), "datasweet_formula - Formula f"
        )
        self.assertEqual(
            table.metric_label(table.get_agg("b")),
            "sum_bucket (b) - [Count] of date_histogram",
        )


def state_visualization(state):
    return state._index_pattern and _visualization_of(state)


def _visualization_of(state):
    # ContextVisualization keeps the parsed state. Rebuild the document facade
    # translate() and translate_legacy() expect.
    visualization = Visualization(
        {
            "type": state.type(),
            "aggs": state._state["aggs"],
            "params": state._state["params"],
        }
    )
    visualization.formats = state._index_pattern.fieldFormatMap
    visualization.index = lambda using=None: state._index_pattern
    visualization.uiStateJSON = DictView({})
    return visualization


def spec_signal(spec, name):
    return [item["update"] for item in spec["signals"] if item["name"] == name][0]
