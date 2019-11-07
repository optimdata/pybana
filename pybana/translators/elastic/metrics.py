# -*- coding: utf-8 -*-

import json

"""
Provide translators which translate metric aggregations defined using kibana syntax to elasticsearch syntax.

Not supported:
- Top Hit
- Sibling pipeline aggregations
- Parent pipeline aggregations
"""


class BaseMetric:
    def params(self, agg):
        return json.loads(agg["params"].get("json") or "{}")

    def translate(self, proxy, agg, state):
        proxy.metric(agg["id"], agg["type"], **self.params(agg))


class AvgMetric(BaseMetric):
    aggtype = "avg"

    def params(self, agg):
        return {"field": agg["params"]["field"], **super().params(agg)}


class CardinalityMetric(BaseMetric):
    aggtype = "cardinality"

    def params(self, agg):
        return {"field": agg["params"]["field"], **super().params(agg)}


class CountMetric(BaseMetric):
    aggtype = "count"

    def translate(self, proxy, agg, state):
        pass


class MaxMetric(BaseMetric):
    aggtype = "max"

    def params(self, agg):
        return {"field": agg["params"]["field"], **super().params(agg)}


class MedianMetric(BaseMetric):
    aggtype = "median"

    def params(self, agg):
        return {
            "field": agg["params"]["field"],
            "percents": [50],
            **super().params(agg),
        }

    def translate(self, proxy, agg, state):
        proxy.metric(agg["id"], "percentiles", **self.params(agg))


class MinMetric(BaseMetric):
    aggtype = "min"

    def params(self, agg):
        return {"field": agg["params"]["field"], **super().params(agg)}


class PercentilesMetric(BaseMetric):
    aggtype = "percentiles"

    def params(self, agg):
        return {
            "field": agg["params"]["field"],
            "percents": agg["params"]["percents"],
            **super().params(agg),
        }


class PercentileRanksMetric(BaseMetric):
    aggtype = "percentile_ranks"

    def params(self, agg):
        return {
            "field": agg["params"]["field"],
            "values": agg["params"]["values"],
            **super().params(agg),
        }


class StdDevMetric(BaseMetric):
    aggtype = "std_dev"

    def params(self, agg):
        return {"field": agg["params"]["field"], **super().params(agg)}

    def translate(self, proxy, agg, state):
        proxy.metric(agg["id"], "extended_stats", **self.params(agg))


class SumMetric(BaseMetric):
    aggtype = "sum"

    def params(self, agg):
        return {"field": agg["params"]["field"], **super().params(agg)}


class DatasweetMetric(BaseMetric):
    aggtype = "datasweet_formula"

    def translate(self, proxy, agg, state):
        pass


TRANSLATORS = {
    translator.aggtype: translator
    for translator in (
        AvgMetric,
        CardinalityMetric,
        CountMetric,
        DatasweetMetric,
        MaxMetric,
        MedianMetric,
        MinMetric,
        PercentileRanksMetric,
        PercentilesMetric,
        StdDevMetric,
        SumMetric,
    )
}


class MetricTranslator:
    def translate(self, proxy, agg, state):
        TRANSLATORS[agg["type"]]().translate(proxy, agg, state)
