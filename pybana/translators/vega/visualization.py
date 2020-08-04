# -*- coding: utf-8 -*-

import json


__all__ = ("ContextVisualization",)


class ContextVisualization:
    """
    Represent a visualization with a context.

    :param Visualization visualization: Visualization deserialized.
    :param pybana.Config config: Config of the kibana instance.
    """

    def __init__(self, visualization, config):
        self._index_pattern = visualization.index()
        self._state = visualization.visState.to_dict()
        self._ui_state = visualization.uiStateJSON.to_dict()
        self._config = config
        self.ui_colors = {
            **json.loads(
                self._config.config.to_dict().get("visualization:colorMapping", "{}")
            ),
            **self._ui_state.get("vis", {}).get("colors", {}),
        }

    def singleton(self):
        return all(map(lambda agg: agg["schema"] != "segment", self._state["aggs"]))

    def _aggs_by_type(self, typ):
        return [agg for agg in self._state["aggs"] if agg["schema"] == typ]

    def type(self):
        return self._state["type"]

    def get_agg(self, aggid):
        """
        Returns the agg corresponding to the agg identifier.
        """
        aggs = self._state["aggs"]
        return [agg for agg in aggs if agg["id"] == aggid][0]

    def is_duration_agg(self, agg):
        """
        Indicates if an aggregation is of type duration.

        A field is considered as a duration if it's a number and if the format is something like '00:00:00'
        """
        params = agg["params"]
        field = params.get("field")
        field_formats = self._index_pattern.fieldFormatMap
        fmt = field_formats.to_dict().get(field) if field and field_formats else None
        return (
            fmt
            and fmt["id"] == "number"
            and ":" in fmt.get("params", {}).get("pattern")
        )

    def series_params(self, agg):
        return [
            param
            for param in self._state["params"]["seriesParams"]
            if param["data"]["id"] == agg["id"]
        ][0]

    def bucket_aggs(self):
        """
        Return all the aggregations that generate bucketing.
        """
        return [
            agg
            for agg in self._state["aggs"]
            if agg["schema"] in ("segment", "group", "bucket")
        ]

    def segment_aggs(self):
        return self._aggs_by_type("segment")

    def metric_aggs(self):
        return self._aggs_by_type("metric")

    def metric_label(self, agg):
        if agg["params"].get("customLabel"):
            return agg["params"]["customLabel"]
        if self.type() in ("pie",):
            if agg["type"] == "count":
                return "Count"
            elif agg["type"] == "sum":
                return "Sum of %(field)s" % agg["params"]
            elif agg["type"] == "cardinality":
                return "Unique count of %(field)s" % agg["params"]
            raise NotImplementedError(
                "%(type)s for pie chart is not implemented" % agg
            )  # pragma: no cover
        elif self.type() in ("table", "metric"):
            if agg["type"] == "count":
                return "Count"
            return "%s - %s" % (agg["type"], agg["params"]["field"])
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

    def valueax(self, axid):
        return [ax for ax in self.valueaxes() if ax["id"] == axid][0]

    def valueaxserie(self, ax):
        """
        Returns first serie of the given axe.
        """
        series = self._state["params"]["seriesParams"]
        return [serie for serie in series if serie["valueAxis"] == ax["id"]][0]

    def valueaxtype(self, ax):
        return self.valueaxserie(ax)["type"]

    def valueaxes(self):
        return self._state["params"].get("valueAxes", [])

    def y(self, ax):
        axid = ax["id"].split("-")[-1]
        return f"y{axid}"
