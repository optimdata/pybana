# -*- coding: utf-8 -*-

import json

from .utils import get_field_arg

"""
Provide translators which translate metric aggregations defined using kibana syntax to elasticsearch syntax.

Not supported:
- Sibling pipeline aggregations
- Parent pipeline aggregations
"""


class BaseMetric:
    def params(self, agg, field):
        return json.loads(agg["params"].get("json") or "{}")

    def translate(self, proxy, agg, state, field):
        proxy.metric(agg["id"], agg["type"], **self.params(agg, field))


class AvgMetric(BaseMetric):
    aggtype = "avg"

    def params(self, agg, field):
        return {**get_field_arg(agg, field), **super().params(agg, field)}


class CardinalityMetric(BaseMetric):
    aggtype = "cardinality"

    def params(self, agg, field):
        return {**get_field_arg(agg, field), **super().params(agg, field)}


class CountMetric(BaseMetric):
    aggtype = "count"

    def translate(self, proxy, agg, state, field):
        pass


class MaxMetric(BaseMetric):
    aggtype = "max"

    def params(self, agg, field):
        return {**get_field_arg(agg, field), **super().params(agg, field)}


class MedianMetric(BaseMetric):
    aggtype = "median"

    def params(self, agg, field):
        return {"percents": [50], **get_field_arg(agg, field), **super().params(agg, field)}

    def translate(self, proxy, agg, state, field):
        proxy.metric(agg["id"], "percentiles", **self.params(agg, field))


class MinMetric(BaseMetric):
    aggtype = "min"

    def params(self, agg, field):
        return {**get_field_arg(agg, field), **super().params(agg, field)}


class PercentilesMetric(BaseMetric):
    aggtype = "percentiles"

    def params(self, agg, field):
        return {
            "percents": agg["params"]["percents"],
            **get_field_arg(agg, field),
            **super().params(agg, field),
        }


class PercentileRanksMetric(BaseMetric):
    aggtype = "percentile_ranks"

    def params(self, agg, field):
        return {
            "values": agg["params"]["values"],
            **get_field_arg(agg, field),
            **super().params(agg, field),
        }


class StdDevMetric(BaseMetric):
    aggtype = "std_dev"

    def params(self, agg, field):
        return {**get_field_arg(agg, field), **super().params(agg, field)}

    def translate(self, proxy, agg, state, field):
        proxy.metric(agg["id"], "extended_stats", **self.params(agg, field))


class SumMetric(BaseMetric):
    aggtype = "sum"

    def params(self, agg, field):
        return {**get_field_arg(agg, field), **super().params(agg, field)}


class DatasweetMetric(BaseMetric):
    aggtype = "datasweet_formula"

    def translate(self, proxy, agg, state, field):
        pass


class TopHitsMetric(BaseMetric):
    """
    Translator for top_hits metric.

    Careful, this metric is partially supported:
    - date fields are not handled.
    - scripted fields are not handled.
    """

    aggtype = "top_hits"

    def translate(self, proxy, agg, state, field):
        params = agg["params"]
        proxy.metric(
            agg["id"],
            "top_hits",
            sort={params["sortField"]: {"order": params["sortOrder"]}},
            size=params["size"],
            _source=agg["params"]["field"],
        )


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
        TopHitsMetric,
    )
}


class MetricTranslator:
    def translate(self, proxy, agg, state, field):
        TRANSLATORS[agg["type"]]().translate(proxy, agg, state, field)
