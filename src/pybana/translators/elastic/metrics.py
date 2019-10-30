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


class AvgBucket(BaseMetric):
    aggtype = "avg"

    def params(self, agg):
        return {"field": agg["params"]["field"], **super().params(agg)}


class CardinalityBucket(BaseMetric):
    aggtype = "cardinality"

    def params(self, agg):
        return {"field": agg["params"]["field"], **super().params(agg)}


class CountBucket(BaseMetric):
    aggtype = "count"

    def translate(self, proxy, agg, state):
        pass


class MaxBucket(BaseMetric):
    aggtype = "max"

    def params(self, agg):
        return {"field": agg["params"]["field"], **super().params(agg)}


class MedianBucket(BaseMetric):
    aggtype = "median"

    def params(self, agg):
        return {
            "field": agg["params"]["field"],
            "percents": [50],
            **super().params(agg),
        }

    def translate(self, proxy, agg, state):
        proxy.metric(agg["id"], "percentiles", **self.params(agg))


class MinBucket(BaseMetric):
    aggtype = "min"

    def params(self, agg):
        return {"field": agg["params"]["field"], **super().params(agg)}


class PercentilesBucket(BaseMetric):
    aggtype = "percentiles"

    def params(self, agg):
        return {
            "field": agg["params"]["field"],
            "percents": agg["params"]["percents"],
            **super().params(agg),
        }


class PercentileRanksBucket(BaseMetric):
    aggtype = "percentile_ranks"

    def params(self, agg):
        return {
            "field": agg["params"]["field"],
            "values": agg["params"]["values"],
            **super().params(agg),
        }


class StdDevBucket(BaseMetric):
    aggtype = "std_dev"

    def params(self, agg):
        return {"field": agg["params"]["field"], **super().params(agg)}

    def translate(self, proxy, agg, state):
        proxy.metric(agg["id"], "extended_stats", **self.params(agg))


class SumBucket(BaseMetric):
    aggtype = "sum"

    def params(self, agg):
        return {"field": agg["params"]["field"], **super().params(agg)}


TRANSLATORS = {
    translator.aggtype: translator
    for translator in (
        AvgBucket,
        CardinalityBucket,
        CountBucket,
        MaxBucket,
        MedianBucket,
        MinBucket,
        PercentileRanksBucket,
        PercentilesBucket,
        StdDevBucket,
        SumBucket,
    )
}


class MetricTranslator:
    def translate(self, proxy, agg, state):
        TRANSLATORS[agg["type"]]().translate(proxy, agg, state)
