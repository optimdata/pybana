# -*- coding: utf-8 -*-

import hjson

from pybana.helpers import format_timestamp, get_scaled_date_format, percentage
from pybana.translators.elastic.buckets import (
    compute_auto_interval,
    duration_from_interval,
)

from .constants import (
    KIBANA_SEED_COLORS,
    DEFAULT_WIDTH,
    DEFAULT_PIE_WIDTH,
    DEFAULT_HEIGHT,
    DEFAULT_PADDING,
)
from .colormaps import get_interval_color
from .metrics import VEGA_METRICS
from .visualization import ContextVisualization

__all__ = ("VegaTranslator",)


class VegaTranslator:
    def conf(self, state):
        return {
            "$schema": "https://vega.github.io/schema/vega/v5.json",
            "width": DEFAULT_PIE_WIDTH
            if state.type() in ["pie", "gauge"]
            else DEFAULT_WIDTH,
            "height": DEFAULT_HEIGHT,
            "padding": DEFAULT_PADDING,
        }

    def data(self, conf, state, response, scope):
        if state.type() in ["pie", "gauge"]:
            conf = self.data_single_metric(conf, state, response)
        else:
            conf = self.data_line_bar(conf, state, response, scope)
        return conf

    def _is_duration_bucket(self, state, agg, metric):
        return state.is_duration_agg(agg) and metric.aggtype in [
            "avg",
            "median",
            "min",
            "max",
            "sum",
        ]

    def _format_duration(self, duration):
        return f"{(duration // 3600):.0f}:{(duration % 3600) // 60:.0f}:{duration % 3600 % 60:.0f}"

    def data_single_metric(self, conf, state, response):
        conf["data"] = [{"name": "table", "values": []}]
        if state.type() == "pie":
            conf["data"][0]["transform"] = ([{"type": "pie", "field": "y"}],)
        # In case of a pie or gauge, there is only one metric agg
        metric_agg = state.metric_aggs()[0]
        metric = VEGA_METRICS[metric_agg["type"]]()
        if state.singleton():
            conf["data"][0]["values"].append(
                {
                    "y": metric.contribute(metric_agg, None, response),
                    "metric": state.metric_label(metric_agg),
                    "group": "all",
                }
            )
        else:

            def rec(segment_it):
                """
                TODO: Implement recursion.
                """
                segment_agg = state.segment_aggs()[segment_it]
                buckets = response.aggregations.to_dict()[segment_agg["id"]]["buckets"]
                sumbuckets = sum(
                    [
                        metric.contribute(metric_agg, bucket, response)
                        for bucket in buckets
                    ]
                )
                for bucket in buckets:
                    y = metric.contribute(metric_agg, bucket, response)
                    label = state.metric_label(metric_agg)
                    group = bucket["key"]
                    ratio = percentage(y, sumbuckets)
                    tooltip_display_value = f"{y}/{sumbuckets}"
                    if self._is_duration_bucket(state, metric_agg, metric):
                        tooltip_display_value = self._format_duration(y)

                    conf["data"][0]["values"].append(
                        {
                            "y": y,
                            "metric": label,
                            "segment": segment_it,
                            "group": group,
                            "tooltip": {
                                group: f"{tooltip_display_value} ({ratio:.2f}%)"
                            },
                            "label": "%s (%.2f%%)" % (group, ratio),
                        }
                    )

            rec(0)
        return conf

    def _get_node_key(self, node, agg, scaled_date_format, locale):
        if agg["type"] == "date_histogram":
            key = format_timestamp(node.get("key"), scaled_date_format, locale)
        else:
            key = node.get("key_as_string") or node.get("key")
        return key

    def _iter_response(
        self,
        node,
        bucket_aggs,
        it,
        point,
        state,
        response,
        scaled_date_format=None,
        locale=None,
    ):
        """
        Iterate through response and yield each point.

        A point is the value of a metric at a given bucketing. It has the following
        properties:
        - x: It's the value of the segment agg (there's should be only one)
        - group: A string which concatenate all the group keys
        - y: The value of the metric
        - metric: The name of the metric
        - axis: The axis on which the point should be displayed

        :param node dict: The node of the response. Should start at `response.aggregations`.
        :param bucket_aggs list: List of bucket aggs
        :param it: Iterator on bucket_aggs.
        :param point: The point currently being filled.
        :param state: Visualization state.
        :param response: Elasticsearch response.
        :param scaled_date_format: Date format for date histograms
        :param locale: Locale for date formatting
        """
        if it == len(bucket_aggs):
            point["group"] = " - ".join(
                map(str, filter(bool, point.setdefault("groups", [])))
            )
            for m, metric_agg in enumerate(state.metric_aggs()):
                if metric_agg.get("hidden"):
                    continue
                metric = VEGA_METRICS[metric_agg["type"]]()
                y = metric.contribute(metric_agg, node, response)
                if y is None:
                    continue
                childpoint = point.copy()
                # handling case where no bucket aggs
                childpoint.setdefault("x", "all")
                childpoint.pop("groups")
                childpoint.update(
                    {"y": y, "m": m, "metric": state.metric_label(metric_agg)}
                )
                if "seriesParams" in state._state["params"]:
                    series_params = state.series_params(metric_agg)
                    ax = state.valueax(series_params["valueAxis"])
                    childpoint.update(
                        {state.y(ax): y, "axis": series_params["valueAxis"]}
                    )
                tooltip = {
                    childpoint["x_label"]: childpoint["x"],
                    childpoint["metric"]: self._format_duration(y)
                    if self._is_duration_bucket(state, metric_agg, metric)
                    else y,
                }
                if childpoint["group"]:
                    tooltip["group"] = childpoint["group"]
                childpoint["tooltip"] = tooltip
                yield childpoint
            return
        agg = bucket_aggs[it]
        aggnode = node[agg["id"]]
        for child in aggnode["buckets"]:
            childpoint = point.copy()
            key = self._get_node_key(child, agg, scaled_date_format, locale)
            if agg["schema"] == "segment":
                childpoint["x"] = key
                childpoint["key"] = child.get("key")
                agg_params = agg.get("params", {})
                childpoint["x_label"] = (
                    agg_params.get("customLabel") or agg_params.get("field") or "x"
                )
            else:
                childpoint.setdefault("groups", []).append(key)
            for obj in self._iter_response(
                child,
                bucket_aggs,
                it + 1,
                childpoint,
                state,
                response,
                scaled_date_format,
                locale,
            ):
                yield obj

    def data_line_bar(self, conf, state, response, scope):
        data = {"name": "table", "values": []}
        scaled_date_format = None
        segment_aggs = state.segment_aggs()
        if segment_aggs:
            for segment_agg in segment_aggs:
                if segment_agg["type"] == "date_histogram":
                    scaled_date_format = get_scaled_date_format(
                        scope.config,
                        duration_from_interval(
                            compute_auto_interval(
                                segment_agg.get("interval", "auto"),
                                scope.beg,
                                scope.end,
                            )
                        ),
                    )
                    break
        for ax in state.valueaxes():
            if state.groups_stacked(ax):
                data["transform"] = [
                    {
                        "type": "stack",
                        "groupby": ["x"],
                        "field": state.y(ax),
                        "as": [state.y(ax) + "|0", state.y(ax) + "|1"],
                        "offset": "normalize"
                        if ax["scale"]["mode"] == "percentage"
                        else "zero",
                    }
                ]
            elif state.metrics_stacked(ax):
                data["transform"] = [
                    {
                        "type": "stack",
                        "groupby": ["x"],
                        "field": state.y(ax),
                        "as": [state.y(ax) + "|0", state.y(ax) + "|1"],
                        "sort": {"field": "metric"},
                        "offset": "normalize"
                        if ax["scale"]["mode"] == "percentage"
                        else "zero",
                    }
                ]

        for item in self._iter_response(
            response.aggregations.to_dict(),
            state.bucket_aggs(),
            0,
            {},
            state,
            response,
            scaled_date_format,
            scope.locale,
        ):
            data["values"].append(item)

        for ax in state.valueaxes():
            if state.stacked_applied(ax):
                for row in data["values"]:
                    if row.get(state.y(ax)) is None:
                        row[state.y(ax)] = 0

        conf["data"] = [data]
        return conf

    def _scale_x(self, state):
        domain = {"data": "table", "field": "x"}
        segment_aggs = state.segment_aggs()
        if segment_aggs and segment_aggs[0]["type"] in ["date_histogram", "date_range"]:
            domain["sort"] = {"field": "key", "op": "values"}
        return {
            "name": "xscale",
            "type": "band" if state.type() == "histogram" else "point",
            "domain": domain,
            "range": "width",
            "padding": 0.05,
            "round": True,
        }

    def _scale_axis(self, state):
        return {
            "name": "axiscolor",
            "type": "band",
            "domain": {"data": "table", "field": "axis"},
            "range": "category",
        }

    def _scales_metric(self, state, conf):
        if state.type() in ["pie", "gauge"]:
            return
        scheme = []
        domain = []
        for a, agg in enumerate(state.metric_aggs()):
            label = state.series_params(agg)["data"]["label"]
            ax = state.valueax(state.series_params(agg)["valueAxis"])
            # Let's hide legend for values that are always equal to 0 when they
            # are stacked. This hack is usefull when there are many metrics and
            # only few of them with data.
            if state.metrics_stacked(ax):
                label = state.metric_label(agg)
                if all(
                    [
                        row["y"] == 0
                        for row in conf["data"][0]["values"]
                        if row["metric"] == label
                    ]
                ):
                    continue

            domain.append(label)
            scheme.append(
                state.ui_colors.get(
                    label, KIBANA_SEED_COLORS[a % len(KIBANA_SEED_COLORS)]
                )
            )

        yield {
            "name": "metriccolor",
            "type": "ordinal",
            "range": scheme,
            "domain": domain,
        }

    def _scale_group(self, state, data):
        scheme = []
        domain = []
        groups = set()
        for point in data[0]["values"]:
            group = point["group"]
            if group not in groups:
                domain.append(group)
                color = state.ui_colors.get(
                    group, KIBANA_SEED_COLORS[len(groups) % len(KIBANA_SEED_COLORS)]
                )
                scheme.append(color)
                groups.add(group)

        return {
            "name": "groupcolor",
            "type": "ordinal",
            "range": scheme,
            "domain": domain,
        }

    def _scales_y(self, state):
        for ax in state.valueaxes():
            if "min" in ax["scale"] and "max" in ax["scale"]:
                domain = [ax["scale"]["min"], ax["scale"]["max"]]
                nice = False
                zero = False
            else:
                domain = {
                    "data": "table",
                    "field": state.y(ax) + "|1"
                    if state.stacked_applied(ax)
                    else state.y(ax),
                }
                nice = True
                zero = True
            yield (
                {
                    "name": ax["id"],
                    "domain": domain,
                    "nice": nice,
                    "range": "height",
                    "zero": zero,
                }
            )

    def scales(self, conf, state):
        conf["scales"] = [
            self._scale_x(state),
            *self._scales_y(state),
            self._scale_axis(state),
            *self._scales_metric(state, conf),
            self._scale_group(state, conf["data"]),
        ]

        return conf

    def axes(self, conf, state):
        if state.type() in ("line", "histogram"):
            # TODO: handle more that 1 axe
            conf["axes"] = []
            categoryax = state._state["params"]["categoryAxes"][0]

            if categoryax["show"]:
                ax = {
                    "orient": categoryax["position"],
                    "scale": "xscale",
                    "labelOverlap": True,
                }
                if categoryax["labels"].get("rotate", 0) != 0:
                    ax.update(
                        {
                            "labelAngle": 360 - categoryax["labels"]["rotate"],
                            "labelBaseline": "middle",
                            "labelAlign": "right",
                            "labelLimit": 1000,
                        }
                    )
                conf["axes"].append(ax)
            for ax in state.valueaxes():
                if ax["show"]:
                    axconf = {
                        "orient": ax["position"],
                        "scale": ax["id"],
                        "title": ax["title"]["text"],
                    }
                    if ax["scale"]["mode"] == "percentage":
                        axconf["format"] = ".0%"

                    else:
                        # If the serie corresponding to this axis is of type duration, we use a special encoding
                        serie = state.valueaxserie(ax)
                        agg = state.get_agg(aggid=serie["data"]["id"])
                        if state.is_duration_agg(agg):
                            axconf["encode"] = {
                                "labels": {
                                    "update": {
                                        "text": {
                                            "signal": "format(datum.value / 3600, '02d') + ':' + format((datum.value % 3600) / 60, '02d') + ':' + format(datum.value % 60, '02d')"
                                        }
                                    }
                                }
                            }
                    conf["axes"].append(axconf)
        return conf

    def legends(self, conf, state):
        if state.type() == "pie":
            conf = self.legends_pie(conf, state)
        elif state.type() == "gauge":
            pass
        else:
            conf = self.legends_line_bar(conf, state)
        return conf

    def legends_pie(self, conf, state):
        conf["legends"] = [
            {
                "fill": "groupcolor",
                "title": "",
                "orient": state._state["params"]["legendPosition"],
                # TODO: offset should be dynamic given the data to prevent label/legend overlapping
                "offset": 150,
            }
        ]
        return conf

    def legends_line_bar(self, conf, state):
        if len(state.group_aggs()):
            conf["legends"] = [
                {
                    "fill": "groupcolor",
                    "title": "",
                    "columns": 1
                    if state._state["params"]["legendPosition"] in ("left", "right")
                    else 10,
                    "orient": state._state["params"]["legendPosition"],
                }
            ]
        else:
            conf["legends"] = [
                {
                    "fill": "metriccolor",
                    "title": "",
                    "columns": 1
                    if state._state["params"]["legendPosition"] in ("left", "right")
                    else 10,
                    "orient": state._state["params"]["legendPosition"],
                }
            ]
        return conf

    def marks(self, conf, state, response):
        if state.type() == "pie":
            conf = self.marks_pie(conf, state, response)
        elif state.type() == "gauge":
            conf = self.marks_gauge(conf, state, response)
        else:
            conf = self.marks_bar(conf, state, response)
        return conf

    def marks_pie(self, conf, state, response):
        donut = state._state["params"].get("isDonut")
        conf["marks"] = [
            {
                "type": "arc",
                "from": {"data": "table"},
                "encode": {
                    "enter": {
                        "fill": {"scale": "groupcolor", "field": "group"},
                        "x": {"signal": "width / 2"},
                        "y": {"signal": "height / 2"},
                        "startAngle": {"field": "startAngle"},
                        "endAngle": {"field": "endAngle"},
                        "innerRadius": {"signal": "width * .35"}
                        if donut
                        else {"value": 0},
                        "outerRadius": {"signal": "width / 2"},
                        **(
                            {"tooltip": {"field": "tooltip"}}
                            if state._state["params"]["addTooltip"]
                            else {}
                        ),
                    }
                },
            }
        ]
        if state._state["params"]["labels"]["show"]:
            conf["marks"].append(
                {
                    "type": "text",
                    "from": {"data": "table"},
                    "encode": {
                        "enter": {
                            "x": {"field": {"group": "width"}, "mult": 0.5},
                            "y": {"field": {"group": "height"}, "mult": 0.5},
                            "radius": {"signal": "width / 2", "offset": 8},
                            "theta": {
                                "signal": "(datum.startAngle + datum.endAngle)/2"
                            },
                            "fill": {"value": "#000"},
                            # Hide labels for small angles
                            "fillOpacity": {
                                "signal": "datum.endAngle - datum.startAngle < .3 ? 0 : 1"
                            },
                            "align": {
                                "signal": "(datum.startAngle + datum.endAngle)/2 < 3.14 ? 'left' : 'right'"
                            },
                            "baseline": {"value": "middle"},
                            "text": {"field": "label"},
                        }
                    },
                }
            )
        return conf

    def marks_gauge(self, conf, state, response):
        colors_range = sorted(
            state._state["params"]["gauge"]["colorsRange"],
            key=lambda x: (x["from"], x["to"]),
        )
        color_schema_name = state._state["params"]["gauge"]["colorSchema"]
        invert_colors = state._state["params"]["gauge"].get("invertColors", False)
        show_labels = state._state["params"]["gauge"]["labels"]["show"]
        sub_text = state._state["params"]["gauge"]["style"].get("subText", None)
        for i, color_range in enumerate(colors_range):
            # Set up range colors
            range_color = color_range.get(
                "color",
                get_interval_color(
                    color_schema_name, i, len(colors_range) - 1, invert_colors
                ),
            )
            color_range["color"] = range_color

        max_value = max(r["to"] for r in colors_range)
        min_value = min(r["from"] for r in colors_range)
        main_value = conf["data"][0]["values"][0].get("y")
        percentage_mode = state._state["params"]["gauge"].get("percentageMode", False)

        def get_color(value):
            color = "black"
            for color_range in colors_range:
                if value >= color_range["from"]:
                    color = color_range["color"]
                if value < color_range["to"]:
                    break
            return color

        fill_color = get_color(main_value)
        ticks = []
        tick_step = (max_value - min_value) / 10
        for tick_index in range(11):
            tick_value = tick_index * tick_step + min_value
            ticks.append(
                {"value": tick_index * tick_step, "color": get_color(tick_value)}
            )
        conf["signals"] = [
            {"name": "centerX", "update": "width/2"},
            {
                "name": "centerY",
                "update": "height/2 + height/2*sin(PI/10)/(1-sin(PI/10)))",
            },
            {
                "name": "outerRadius",
                "update": "radiusRef",
            },
            {"name": "radiusRef", "update": "min(centerX, centerY)"},
            {"name": "innerRadius", "update": "outerRadius - outerRadius * 0.2"},
            {"name": "maxValue", "update": f"{max_value}"},
            {"name": "minValue", "update": f"{min_value}"},
            {"name": "mainValue", "update": f"{main_value}"},
            {"name": "usedValue", "update": "min(max(minValue, mainValue), maxValue)"},
            {"name": "fontFactor", "update": "(radiusRef/5)/25"},
        ]
        conf["data"].append(
            {
                "name": "ticks",
                "values": ticks,
                "transform": [
                    {
                        "type": "formula",
                        "expr": "datum.value + minValue",
                        "as": "value_2",
                    },
                    {
                        "type": "formula",
                        "as": "radianRef",
                        "expr": "6*PI/5 * (datum.value/(maxValue - minValue)) - PI/10",
                    },
                    {
                        "type": "formula",
                        "as": "x",
                        "expr": "centerX - (innerRadius * cos(datum.radianRef))",
                    },
                    {
                        "type": "formula",
                        "as": "y",
                        "expr": "centerY - (innerRadius * sin(datum.radianRef))",
                    },
                ],
            }
        )
        conf["scales"] = [
            {
                "name": "gaugeScale",
                "type": "linear",
                "domain": {"data": "ticks", "field": "value_2"},
                "zero": False,
                "range": {"signal": "[-3*PI/5, 3*PI/5]"},
            },
            {
                "name": "tickScale",
                "type": "linear",
                "domain": {"data": "ticks", "field": "value"},
                "range": {"signal": "[-3*PI/5, 3*PI/5]"},
            },
        ]
        conf["marks"] = [
            {
                "type": "arc",
                "name": "gauge",
                "encode": {
                    "enter": {
                        "x": {"signal": "centerX"},
                        "y": {"signal": "centerY"},
                        "startAngle": {"signal": "-3*PI/5"},
                        "endAngle": {"signal": "3*PI/5"},
                        "outerRadius": {"signal": "outerRadius"},
                        "innerRadius": {"signal": "innerRadius"},
                        "fill": {"value": "rgb(235,235,235)"},
                    }
                },
            },
            {
                "type": "arc",
                "encode": {
                    "enter": {"startAngle": {"signal": "-3*PI/5"}},
                    "update": {
                        "x": {"signal": "centerX"},
                        "y": {"signal": "centerY"},
                        "innerRadius": {"signal": "innerRadius"},
                        "outerRadius": {"signal": "outerRadius"},
                        "endAngle": {"scale": "gaugeScale", "signal": "usedValue"},
                        "fill": {"value": f"{fill_color}"},
                    },
                },
            },
            {
                "type": "text",
                "name": "gaugeValue",
                "encode": {
                    "enter": {
                        "x": {"signal": "centerX"},
                        "baseline": {"value": "top"},
                        "align": {"value": "center"},
                    },
                    "update": {
                        "text": {
                            "signal": "format(mainValue*100/(maxValue - minValue), '.0f') + '%'"
                            if percentage_mode
                            else "format(mainValue, '.2f')"
                        },
                        "y": {
                            "signal": "centerY - 14*fontFactor",
                        },
                        "fontSize": {"signal": "fontFactor*18"},
                    },
                },
            },
            {
                "type": "arc",
                "from": {"data": "ticks"},
                "encode": {
                    "enter": {
                        "x": {"signal": "centerX"},
                        "y": {"signal": "centerY"},
                        "outerRadius": {"signal": "innerRadius-5"},
                        "innerRadius": {"signal": "innerRadius - 5 -(radiusRef*0.02)"},
                        "startAngle": {"scale": "tickScale", "field": "value"},
                        "endAngle": {"scale": "tickScale", "field": "value"},
                        "stroke": {"signal": "datum.color"},
                    }
                },
            },
        ]

        for i, color_range in enumerate(
            sorted(colors_range, key=lambda x: (x["from"], x["to"]))
        ):
            start_angle_factor = color_range["from"] / (max_value - min_value)
            end_angle_factor = color_range["to"] / (max_value - min_value)
            conf["marks"].append(
                {
                    "type": "arc",
                    "encode": {
                        "enter": {
                            "startAngle": {
                                "signal": f"-3*PI/5 + {start_angle_factor}*2*3*PI/5"
                            }
                        },
                        "update": {
                            "x": {"signal": "centerX"},
                            "y": {"signal": "centerY"},
                            "innerRadius": {"signal": "innerRadius-5"},
                            "outerRadius": {"signal": "innerRadius"},
                            "endAngle": {
                                "signal": f"-3*PI/5 + {end_angle_factor}*2*3*PI/5",
                            },
                            "fill": {"value": f"{color_range['color']}"},
                        },
                    },
                },
            )
        if show_labels:
            conf["marks"].append(
                {
                    "type": "text",
                    "name": "legend",
                    "encode": {
                        "enter": {
                            "x": {"signal": "centerX"},
                            "baseline": {"value": "top"},
                            "align": {"value": "center"},
                        },
                        "update": {
                            "text": {
                                "value": state.metric_label(state.metric_aggs()[0])
                            },
                            "y": {
                                "signal": "centerY - 2*14*fontFactor",
                            },
                            "fontSize": {"signal": "fontFactor*10"},
                        },
                    },
                },
            )
            if sub_text:
                conf["marks"].append(
                    {
                        "type": "text",
                        "name": "sub_text",
                        "encode": {
                            "enter": {
                                "x": {"signal": "centerX"},
                                "baseline": {"value": "top"},
                                "align": {"value": "center"},
                            },
                            "update": {
                                "text": {"value": sub_text},
                                "y": {
                                    "signal": "centerY + 6*fontFactor",
                                },
                                "fontSize": {"signal": "fontFactor*10"},
                            },
                        },
                    },
                )
        return conf

    def _marks_histogram(self, state, ax):
        ret = []
        x2 = (
            "axis"
            if state.multi_axis()
            else "group"
            if state.groups_side_by_side(ax)
            else "metric"
            if state.metrics_side_by_side(ax)
            else "x"
        )
        ret = [
            {
                "type": "group",
                "from": {"facet": {"data": "table", "name": "facet", "groupby": "x"}},
                "signals": [{"name": "width", "update": "bandwidth('xscale')"}],
                "encode": {"enter": {"x": {"scale": "xscale", "field": "x"}}},
                "scales": [
                    {
                        "name": "xscale2",
                        "type": "band",
                        "range": "width",
                        "domain": {"data": "facet", "field": x2},
                    }
                ],
                "marks": [],
            }
        ]
        marks = ret[0]["marks"]

        fill = (
            {"scale": "groupcolor", "field": "group"}
            if state.group_aggs()
            else {"scale": "metriccolor", "field": "metric"}
        )
        stacked = state.stacked_applied(ax)
        marks.append(
            {
                "type": "rect",
                "from": {"data": "facet"},
                "encode": {
                    "enter": {
                        "x": {"scale": "xscale2", "field": x2},
                        "width": {"scale": "xscale2", "band": 1},
                        "y": {
                            "scale": ax["id"],
                            "field": state.y(ax) + "|0" if stacked else state.y(ax),
                        },
                        "y2": {
                            "scale": ax["id"],
                            **(
                                {"field": state.y(ax) + "|1"}
                                if stacked
                                else {"value": 0}
                            ),
                        },
                        "fill": fill,
                        "fillOpacity": {"value": 0.8},
                        **(
                            {"tooltip": {"field": "tooltip"}}
                            if state._state["params"]["addTooltip"]
                            else {}
                        ),
                    }
                },
            }
        )

        return ret

    def _marks_line(self, state, ax):
        circlesizes = []
        strokesizes = []
        marks = []
        for agg in state.metric_aggs():
            params = state.series_params(agg)
            if params["valueAxis"] != ax["id"]:
                continue
            label = params["data"]["label"].replace("'", "\\'")
            size = params.get("lineWidth") or 4
            circletest = (
                f"datum.metric == '{label}'" if params["showCircles"] else "false"
            )
            circlesizes.append({"test": circletest, "value": (size * 2) ** 2})

            stroketest = (
                f"datum.metric == '{label}'"
                if params["drawLinesBetweenPoints"]
                else "false"
            )
            strokesizes.append({"test": stroketest, "value": size})
        circlesizes.append({"value": 0})
        strokesizes.append({"value": 0})

        stackgroupfield = "group" if state.group_aggs() else "metric"
        stylescale = "groupcolor" if state.group_aggs() else "metriccolor"

        marks.append(
            {
                "type": "line",
                "from": {"data": "facet"},
                "encode": {
                    "enter": {
                        "x": {"scale": "xscale", "field": "x"},
                        "y": {
                            "scale": ax["id"],
                            "field": state.y(ax) + "|1"
                            if state.stacked_applied(ax)
                            else state.y(ax),
                        },
                        "stroke": {"scale": stylescale, "field": stackgroupfield},
                        "strokeWidth": strokesizes,
                    }
                },
            }
        )
        marks.append(
            {
                "type": "symbol",
                "from": {"data": "facet"},
                "encode": {
                    "enter": {
                        "x": {"scale": "xscale", "field": "x"},
                        "y": {
                            "scale": ax["id"],
                            "field": state.y(ax) + "|1"
                            if state.stacked_applied(ax)
                            else state.y(ax),
                        },
                        "fill": {"scale": stylescale, "field": stackgroupfield},
                        "size": circlesizes,
                        **(
                            {"tooltip": {"field": "tooltip"}}
                            if state._state["params"]["addTooltip"]
                            else {}
                        ),
                    }
                },
            }
        )
        marks = [
            {
                "type": "group",
                "clip": any(
                    "min" in ax["scale"] and "max" in ax["scale"]
                    for ax in state.valueaxes()
                ),
                "from": {
                    "facet": {
                        "data": "table",
                        "name": "facet",
                        "groupby": stackgroupfield,
                    }
                },
                "marks": marks,
            }
        ]
        return marks

    def marks_bar(self, conf, state, response):
        conf["marks"] = []
        ax = state.valueaxes()[0]
        marks = []

        for ax in state.valueaxes():
            handler = {"histogram": self._marks_histogram, "line": self._marks_line}[
                state.valueaxtype(ax)
            ]
            marks.extend(handler(state, ax))

        conf["marks"] = marks
        return conf

    def translate_legacy(self, visualization, response, scope):
        state = ContextVisualization(visualization=visualization, config=scope.config)

        ret = self.conf(state)
        ret = self.data(ret, state, response, scope)
        ret = self.scales(ret, state)
        ret = self.axes(ret, state)
        ret = self.legends(ret, state)
        ret = self.marks(ret, state, response)
        return ret

    def translate_vega(self, visualization, response, scope):
        ret = hjson.loads(visualization.visState["params"]["spec"])

        def translate_data_item(data, response):
            if "url" in data:
                data.pop("url")
                data["values"] = response.to_dict()

        data = ret["data"]
        if isinstance(data, dict):
            translate_data_item(data, response)
        else:
            for index, data_item in enumerate(data):
                translate_data_item(data_item, response[index])
        ret.setdefault("width", DEFAULT_WIDTH)
        ret.setdefault("height", DEFAULT_HEIGHT)
        ret.setdefault("padding", DEFAULT_PADDING)
        return ret

    def translate(self, visualization, response, scope):
        """
        Transform a kibana visualization object and an elasticsearch_dsl response into a vega object.

        :param elasticsearch_dsl.Document visualization: Visualization fetched from a kibana index.
        :param elasticsearch_dsl.response.Response visualization: Visualization fetched from a kibana index.
        :param Scope scope: The scope associated for data fetching.
        """
        if visualization.visState["type"] == "vega":
            return self.translate_vega(visualization, response, scope)
        else:
            return self.translate_legacy(visualization, response, scope)
