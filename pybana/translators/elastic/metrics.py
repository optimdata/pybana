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

    def translate(self, proxy, agg, state, field, *args):
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

    def translate(self, proxy, agg, state, field, *args):
        pass


class MaxMetric(BaseMetric):
    aggtype = "max"

    def params(self, agg, field):
        return {**get_field_arg(agg, field), **super().params(agg, field)}


class MedianMetric(BaseMetric):
    aggtype = "median"

    def params(self, agg, field):
        return {
            "percents": [50],
            **get_field_arg(agg, field),
            **super().params(agg, field),
        }

    def translate(self, proxy, agg, state, field, *args):
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

    def translate(self, proxy, agg, state, field, *args):
        proxy.metric(agg["id"], "extended_stats", **self.params(agg, field))


class SumMetric(BaseMetric):
    aggtype = "sum"

    def params(self, agg, field):
        return {**get_field_arg(agg, field), **super().params(agg, field)}


class DatasweetMetric(BaseMetric):
    aggtype = "datasweet_formula"

    def translate(self, proxy, agg, state, field, *args):
        pass


class SinglePercentileRankMetric(PercentileRanksMetric):
    aggtype = "single_percentile_rank"

    def translate(self, proxy, agg, state, field, *args):
        agg["params"]["values"] = [0]
        proxy.metric(agg["id"], "percentile_ranks", **self.params(agg, field))
        agg["params"].pop("values")


class RateMetric(BaseMetric):
    aggtype = "rate"

    def translate(self, proxy, agg, state, field, *args):
        pass


class UniqueCountMetric(BaseMetric):
    aggtype = "cardinality"

    def translate(self, proxy, agg, state, field, *args):
        pass


class ValueCountMetric(BaseMetric):
    aggtype = "value_count"

    def translate(self, proxy, agg, state, field, *args):
        pass


class TopHitsMetric(BaseMetric):
    """
    Translator for top_hits metric.

    Careful, this metric is partially supported:
    - date fields are not handled.
    - scripted fields are not handled.
    """

    aggtype = "top_hits"

    def translate(self, proxy, agg, state, field, *args):
        params = agg["params"]
        proxy.metric(
            agg["id"],
            self.aggtype,
            sort={params["sortField"]: {"order": params["sortOrder"]}},
            size=params["size"],
            _source=agg["params"]["field"],
        )


class TopMetricsMetric(BaseMetric):
    """
    Translator for top_metrics metric.

    Careful, this metric is partially supported:
    - date fields are not handled.
    - scripted fields are not handled.
    """

    aggtype = "top_metrics"

    def translate(self, proxy, agg, state, field, *args):
        params = agg["params"]
        proxy.metric(
            agg["id"],
            self.aggtype,
            sort={params["sortField"]: {"order": params["sortOrder"]}},
            size=params["size"],
            metrics={"field": agg["params"]["field"]},
        )


class BaseBucketMetric(BaseMetric):
    def _translate_custom_bucket(self, proxy, params, state, scope, fields):
        from .buckets import BucketTranslator

        custom_bucket_agg = params["customBucket"]
        return BucketTranslator().translate(
            proxy, custom_bucket_agg, state, scope, fields
        )

    def _translate_custom_metric(self, proxy, params, state, scope, fields):
        custom_metric_agg = params["customMetric"]
        metric_field = fields.get(custom_metric_agg.get("params", {}).get("field"))
        MetricTranslator().translate(
            proxy, custom_metric_agg, state, metric_field, scope, fields
        )

    def translate(self, proxy, agg, state, field, scope, fields):
        params = agg["params"]
        new_proxy = self._translate_custom_bucket(proxy, params, state, scope, fields)
        self._translate_custom_metric(new_proxy, params, state, scope, fields)

        bucket_id = params["customBucket"]["id"]
        metric_id = (
            "_count"
            if params["customMetric"]["type"] == "count"
            else params["customMetric"]["id"]
        )

        proxy.metric(
            agg["id"],
            self.aggtype,
            buckets_path=f"{bucket_id}>{metric_id}",
        )


class AverageBucketMetric(BaseBucketMetric):
    aggtype = "avg_bucket"


class MinBucketMetric(BaseBucketMetric):
    aggtype = "min_bucket"


class MaxBucketMetric(BaseBucketMetric):
    aggtype = "max_bucket"


class SumBucketMetric(BaseBucketMetric):
    aggtype = "sum_bucket"


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
        SinglePercentileRankMetric,
        # RateMetric,
        # UniqueCountMetric,
        # ValueCountMetric,
        TopHitsMetric,
        TopMetricsMetric,
        AverageBucketMetric,
        MinBucketMetric,
        MaxBucketMetric,
        SumBucketMetric,
    )
}


class MetricTranslator:
    def translate(self, proxy, agg, state, field, *args):
        TRANSLATORS[agg["type"]]().translate(proxy, agg, state, field, *args)
