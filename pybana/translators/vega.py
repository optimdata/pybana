# -*- coding: utf-8 -*-


import json


__all__ = ("VegaTranslator",)

CATEGORY20 = [
    "#1f77b4",
    "#aec7e8",
    "#ff7f0e",
    "#ffbb78",
    "#2ca02c",
    "#98df8a",
    "#d62728",
    "#ff9896",
    "#9467bd",
    "#c5b0d5",
    "#8c564b",
    "#c49c94",
    "#e377c2",
    "#f7b6d2",
    "#7f7f7f",
    "#c7c7c7",
    "#bcbd22",
    "#dbdb8d",
    "#17becf",
    "#9edae5",
]

KIBANA_SEED_COLORS = [
    "#57c17b",
    "#6f87d8",
    "#663db8",
    "#bc52bc",
    "#9e3533",
    "#daa05d",
    "#00a69b",
]


def percentage(num, den):
    return 100 * num / den if den else 0


class BaseMetric:
    def label(self, agg):
        return "metric" + agg["id"]

    def contribute(self, agg, bucket, response):
        return (bucket or response["aggregations"])[agg["id"]]["value"]


class CountMetric(BaseMetric):
    aggtype = "count"

    def contribute(self, agg, bucket, response):
        return (
            bucket["doc_count"]
            if bucket and "doc_count" in bucket
            else response["hits"]["total"]
        )


class AverageMetric(BaseMetric):
    aggtype = "avg"


class MinMetric(BaseMetric):
    aggtype = "min"


class MaxMetric(BaseMetric):
    aggtype = "max"


class SumMetric(BaseMetric):
    aggtype = "sum"


class CardinalityMetric(BaseMetric):
    aggtype = "cardinality"


METRICS = {
    metric.aggtype: metric
    for metric in [
        AverageMetric,
        CountMetric,
        MaxMetric,
        MinMetric,
        SumMetric,
        CardinalityMetric,
    ]
}


class State:
    def __init__(self, state):
        self._state = state

    def singleton(self):
        return all(map(lambda agg: agg["schema"] != "segment", self._state["aggs"]))

    def _aggs_by_type(self, typ):
        return [agg for agg in self._state["aggs"] if agg["schema"] == typ]

    def type(self):
        return self._state["type"]

    def agg(self, aggid):
        return [agg for agg in self._state["aggs"] if agg["id"] == aggid][0]

    def series_params(self, agg):
        return [
            param
            for param in self._state["params"]["seriesParams"]
            if param["data"]["id"] == agg["id"]
        ][0]

    def segment_aggs(self):
        return self._aggs_by_type("segment")

    def metric_aggs(self):
        return self._aggs_by_type("metric")

    def metric_label(self, agg):
        if self.type() == "pie":
            if agg["type"] == "count":
                return "Count"
            elif agg["type"] == "sum":
                return "Sum of %(field)s" % agg["params"]
            elif agg["type"] == "cardinality":
                return "Unique count of %(field)s" % agg["params"]
            raise NotImplementedError(
                "%(type)s for pie chart is not implemented" % agg
            )  # pragma: no cover
        return self.series_params(agg)["data"]["label"]

    def group_aggs(self):
        return self._aggs_by_type("group")

    def groups_side_by_side(self, ax):
        return len(self.group_aggs()) > 0 and not self.groups_stacked(ax)

    def groups_stacked(self, ax):
        return (
            any(
                [
                    param.get("mode") == "stacked"
                    for param in self._state["params"]["seriesParams"]
                    if param["valueAxis"] == ax["id"]
                ]
            )
            and len(self.group_aggs()) > 0
        )

    def metrics_side_by_side(self, ax):
        return len(
            [
                agg
                for agg in self.metric_aggs()
                if self.series_params(agg)["valueAxis"] == ax["id"]
            ]
        ) > 1 and not self.metrics_stacked(ax)

    def metrics_stacked(self, ax):
        aggs = [
            (agg, self.series_params(agg))
            for agg in self.metric_aggs()
            if self.series_params(agg)["valueAxis"] == ax["id"]
        ]

        return (
            any([param.get("mode") == "stacked" for agg, param in aggs])
            and len(aggs) > 1
        )

    def multi_axis(self):
        aggs = self.metric_aggs()
        axis = set([self.series_params(agg)["valueAxis"] for agg in aggs])
        return len(axis) > 1

    def stacked_applied(self, ax):
        return self.metrics_stacked(ax) or self.groups_stacked(ax)

    def faceted(self):
        return False

    def valueax(self, axid):
        return [ax for ax in self.valueaxes() if ax["id"] == axid][0]

    def valueaxtype(self, ax):
        return [
            serie
            for serie in self._state["params"]["seriesParams"]
            if serie["valueAxis"] == ax["id"]
        ][0]["type"]

    def valueaxes(self):
        return self._state["params"].get("valueAxes", [])

    def y(self, ax):
        axid = ax["id"].split("-")[-1]
        return f"y{axid}"

    def ui_colors(self):
        return self._state.get("vis", {}).get("colors", {})


class VegaTranslator:
    def _mark_encode_enter(self, typ, field):
        return {
            "histogram": {"fill": {"scale": "color", "field": field}},
            "line": {"stroke": {"scale": "color", "field": field}},
        }[typ]

    def conf(self, state):
        return {
            "$schema": "https://vega.github.io/schema/vega/v5.json",
            "width": 200 if state.type() == "pie" else 800,
            "height": 200,
            "padding": 5,
        }

    def data(self, conf, state, response):
        if state.type() == "pie":
            conf = self.data_pie(conf, state, response)
        else:
            conf = self.data_line_bar(conf, state, response)
        return conf

    def data_pie(self, conf, state, response):
        conf["data"] = [
            {
                "name": "table",
                "values": [],
                "transform": [{"type": "pie", "field": "y"}],
            }
        ]
        # In case of a pie, there is only one metric agg
        metric_agg = state.metric_aggs()[0]
        metric = METRICS[metric_agg["type"]]()
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
                    conf["data"][0]["values"].append(
                        {
                            "y": y,
                            "metric": label,
                            "segment": segment_it,
                            "group": group,
                            "tooltip": {group: f"{y}/{sumbuckets} ({ratio:.2f}%)"},
                            "label": "%s (%.2f%%)" % (group, ratio),
                        }
                    )

            rec(0)
        return conf

    def data_line_bar(self, conf, state, response):
        data = {"name": "table", "values": []}
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
                    }
                ]

        def iterate_segment():
            if state.singleton():
                yield "all", response.aggregations.to_dict()
            else:
                # For now only first segment is handled
                agg = state.segment_aggs()[0]
                buckets = response.aggregations.to_dict()[agg["id"]]["buckets"]
                for bucket in buckets:
                    x = bucket.get("key_as_string") or bucket.get("key")
                    yield x, bucket

        def rec(bucket, it, groups, baseitem):
            if it == len(group_aggs):
                groups = groups + [bucket.get("key_as_string") or bucket.get("key")]
                group = " - ".join(filter(bool, groups[1:]))
                y = metric.contribute(agg, bucket, response)
                tooltip = {"x": baseitem["x"], baseitem["metric"]: y}
                if group:
                    tooltip["group"] = group
                yield (
                    {
                        "y": y,
                        state.y(ax): y,
                        "group": group,
                        "axis": series_params["valueAxis"],
                        "tooltip": tooltip,
                        **baseitem,
                    }
                )

            if it < len(group_aggs):
                groups = groups + [bucket.get("key_as_string") or bucket.get("key")]
                buckets = bucket[group_aggs[it]["id"]]["buckets"]
                for bucket in buckets:
                    for item in rec(bucket, it + 1, groups, baseitem):
                        yield item

        for x, bucket in iterate_segment():
            for m, agg in enumerate(state.metric_aggs()):
                metric = METRICS[agg["type"]]()
                label = state.metric_label(agg)
                series_params = state.series_params(agg)
                group_aggs = state.group_aggs()
                ax = state.valueax(series_params["valueAxis"])
                baseitem = {"x": x, "metric": label, "m": m}
                for item in rec(bucket, 0, [], baseitem):
                    data["values"].append(item)

        for ax in state.valueaxes():
            if state.stacked_applied(ax):
                for row in data["values"]:
                    if row.get(state.y(ax)) is None:
                        row[state.y(ax)] = 0
        conf["data"] = [data]
        return conf

    def _scale_x(self, state):
        return {
            "name": "xscale",
            "type": "band" if state.type() == "histogram" else "point",
            "domain": {"data": "table", "field": "x"},
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

    def _scales_metric(self, state):
        if state.type() == "pie":
            return
        scheme = []
        domain = []
        for a, agg in enumerate(state.metric_aggs()):
            label = state.series_params(agg)["data"]["label"]
            domain.append(label)
            scheme.append(state.ui_colors().get(label, KIBANA_SEED_COLORS[a]))

        yield {
            "name": "metriccolor",
            "type": "ordinal",
            "range": scheme,
            "domain": domain,
        }

    def _scale_group(self, state, data):
        if state.type() == "pie":
            scheme = []
            domain = []
            for a, row in enumerate(data[0]["values"]):
                label = row["group"]
                domain.append(label)
                scheme.append(state.ui_colors().get(label, KIBANA_SEED_COLORS[a]))

            return {
                "name": "groupcolor",
                "type": "ordinal",
                "range": scheme,
                "domain": domain,
            }
        else:
            return {
                "name": "groupcolor",
                "type": "ordinal",
                "range": "category",
                "domain": {"data": "table", "field": "group"},
            }

    def _scales_y(self, state):
        for ax in state.valueaxes():
            yield (
                {
                    "name": ax["id"],
                    "domain": {
                        "data": "table",
                        "field": state.y(ax) + "|1"
                        if state.stacked_applied(ax)
                        else state.y(ax),
                    },
                    "nice": True,
                    "range": "height",
                }
            )

    def scales(self, conf, state):
        conf["scales"] = [
            self._scale_x(state),
            *self._scales_y(state),
            self._scale_axis(state),
            *self._scales_metric(state),
            self._scale_group(state, conf["data"]),
        ]

        return conf

    def axes(self, conf, state):
        if state.type() in ("line", "histogram"):
            # TODO: handle more that 1 axe
            conf["axes"] = []
            categoryax = state._state["params"]["categoryAxes"][0]
            if categoryax["show"]:
                conf["axes"].append(
                    {
                        "orient": categoryax["position"],
                        "scale": "xscale",
                        "labelOverlap": True,
                    }
                )
            for ax in state.valueaxes():
                if ax["show"]:
                    axconf = {"orient": ax["position"], "scale": ax["id"]}
                    if ax["scale"]["mode"] == "percentage":
                        axconf["format"] = ".0%"
                    conf["axes"].append(axconf)
        return conf

    def legends(self, conf, state):
        if state.type() == "pie":
            conf = self.legends_pie(conf, state)
        else:
            conf = self.legends_line_bar(conf, state)
        return conf

    def legends_pie(self, conf, state):
        conf["legends"] = [
            {
                "fill": "groupcolor",
                "title": "Series",
                "orient": state._state["params"]["legendPosition"],
            }
        ]
        return conf

    def legends_line_bar(self, conf, state):
        if len(state.group_aggs()):
            conf["legends"] = [
                {
                    "fill": "groupcolor",
                    "title": "Series",
                    "orient": state._state["params"]["legendPosition"],
                }
            ]
        else:
            conf["legends"] = [
                {
                    "fill": "metriccolor",
                    "title": "Series",
                    "orient": state._state["params"]["legendPosition"],
                }
            ]
        return conf

    def marks(self, conf, state, response):
        if state.type() == "pie":
            conf = self.marks_pie(conf, state, response)
        elif state.type() == "histogram":
            conf = self.marks_bar(conf, state, response)
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
            label = params["data"]["label"]
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
            if state.valueaxtype(ax) == "histogram":
                marks.extend(self._marks_histogram(state, ax))

            elif state.valueaxtype(ax) == "line":
                marks.extend(self._marks_line(state, ax))

        conf["marks"] = marks
        return conf

    def translate(self, visualization, response, scope):
        """
        Transform a kibana visualization object and an elasticsearch_dsl response into a vega object.

        :param elasticsearch_dsl.Document visualization: Visualization fetched from a kibana index.
        :param elasticsearch_dsl.response.Response visualization: Visualization fetched from a kibana index.
        :param scope Scope: A scope is a object with beg (datetime), end (datetime) and tzinfo (pytz.timezone).
        """
        state = State(
            {
                **json.loads(visualization.visualization["visState"]),
                **json.loads(
                    visualization.visualization.to_dict().get("uiStateJSON", "{}")
                ),
            }
        )

        ret = self.conf(state)
        ret = self.data(ret, state, response)
        ret = self.scales(ret, state)
        ret = self.axes(ret, state)
        ret = self.legends(ret, state)
        ret = self.marks(ret, state, response)

        return ret
