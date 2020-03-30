# -*- coding: utf-8 -*-

import math
from pybana.helpers.datasweet import datasweet_eval

__all__ = ("VEGA_METRICS",)


class BaseMetric:
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


class MedianMetric(BaseMetric):
    aggtype = "median"

    def contribute(self, agg, bucket, response):
        return (bucket or response["aggregations"])[agg["id"]]["values"]["50.0"]


class StdDevMetric(BaseMetric):
    aggtype = "std_dev"

    def contribute(self, agg, bucket, response):
        return (bucket or response["aggregations"])[agg["id"]]["std_deviation"]


class MinMetric(BaseMetric):
    aggtype = "min"


class MaxMetric(BaseMetric):
    aggtype = "max"


class SumMetric(BaseMetric):
    aggtype = "sum"


class CardinalityMetric(BaseMetric):
    aggtype = "cardinality"


class DatasweetMetric(BaseMetric):
    aggtype = "datasweet_formula"

    def contribute(self, agg, bucket, response):
        ret = datasweet_eval(agg["params"]["formula"], bucket)
        bucket[agg["id"]] = {
            "value": None if isinstance(ret, float) and math.isnan(ret) else ret
        }
        return ret


VEGA_METRICS = {
    metric.aggtype: metric
    for metric in [
        AverageMetric,
        CardinalityMetric,
        CountMetric,
        DatasweetMetric,
        MaxMetric,
        MedianMetric,
        MinMetric,
        StdDevMetric,
        SumMetric,
    ]
}
