# -*- coding: utf-8 -*-

import json

from .metrics import MetricTranslator

"""
Provide translators which translate bucket aggregations defined using kibana syntax to elasticsearch syntax.

Not supported:
- IPv4 range. Never used.
- Significant terms. Rarely used (maybe never).
- Terms:
  - "Group other values in separate bucket" is not handled. If set, kibana turns a "terms" to a "filters" agg.
  - Order by custom metric
"""


def compute_auto_interval(interval, beg, end):
    """
    Compute the automatic interval
    """
    if interval == "auto":
        delta = end - beg
        if delta.days >= 2 * 365:  # 2years
            return "30d"
        elif delta.days >= 365:
            return "1w"
        elif delta.days >= 48:
            return "1d"
        elif delta.days >= 12:
            return "12h"
        elif delta.days >= 4:
            return "3h"
        elif delta.total_seconds() >= 36 * 3600:
            return "1h"
        elif delta.total_seconds() >= 16 * 3600:
            return "30m"
        elif delta.total_seconds() >= 8 * 3600:
            return "10m"
        elif delta.total_seconds() >= 2 * 3600:
            return "5m"
        elif delta.total_seconds() >= 36 * 60:
            return "1m"
        elif delta.total_seconds() >= 12 * 60:
            return "30s"
        elif delta.total_seconds() >= 6 * 60:
            return "10s"
        elif delta.total_seconds() >= 4 * 60:
            return "5s"
        else:
            return "1s"
    return f"1{interval}"


def format_from_interval(interval):
    if interval.endswith("y"):
        return "yyyy"
    if interval.endswith("q") or interval.endswith("M"):
        return "yyyy-MM"
    if interval.endswith("d") or interval.endswith("w"):
        return "yyyy-MM-dd"
    if interval.endswith("h"):
        return "yyyy-MM-dd'T'HH'h'"
    return "date_time"


class BaseBucket:
    def translate(self, agg, state, context):
        return json.loads(agg["params"].get("json") or "{}")


class DateHistogramBucket(BaseBucket):
    aggtype = "date_histogram"

    def translate(self, agg, state, context):
        interval = compute_auto_interval(
            agg["params"]["interval"], context.beg, context.end
        )

        return {
            "field": agg["params"]["field"],
            "interval": interval,
            "time_zone": str(context.tzinfo),
            "format": format_from_interval(interval),
            **super().translate(agg, state, context),
        }


class DateRangeBucket(BaseBucket):
    aggtype = "date_range"

    def translate(self, agg, state, context):
        return {
            "field": agg["params"]["field"],
            "ranges": agg["params"]["ranges"],
            **super().translate(agg, state, context),
        }


class FiltersBucket(BaseBucket):
    aggtype = "filters"

    def translate(self, agg, state, context):
        filters = {}
        for fltr in agg["params"]["filters"]:
            label = fltr.get("label") or fltr["input"]["query"] or "*"
            filters[label] = (
                {
                    "query_string": {
                        "query": fltr["input"]["query"],
                        "analyze_wildcard": True,
                        "default_field": "*",
                    }
                }
                if fltr["input"]["query"]
                else {"match_all": {}}
            )
        return {"filters": filters, **super().translate(agg, state, context)}


class HistogramBucket(BaseBucket):
    aggtype = "histogram"

    def translate(self, agg, state, context):
        return {
            "field": agg["params"]["field"],
            "interval": agg["params"]["interval"],
            **super().translate(agg, state, context),
        }


class RangeBucket(BaseBucket):
    aggtype = "range"

    def translate(self, agg, state, context):
        return {
            "field": agg["params"]["field"],
            "ranges": agg["params"]["ranges"],
            **super().translate(agg, state, context),
        }


class TermsBucket(BaseBucket):
    aggtype = "terms"

    def translate(self, agg, state, context):
        orderby = agg["params"]["orderBy"]
        aggs = {agg["id"]: agg for agg in state["aggs"]}
        if orderby in aggs and aggs[orderby]["type"] == "count":
            orderby = "_count"
        if orderby == "custom":
            orderby = agg["params"]["orderAgg"]["id"]
        return {
            "field": agg["params"]["field"],
            "size": agg["params"]["size"],
            "order": {orderby: agg["params"]["order"]},
            **super().translate(agg, state, context),
        }


TRANSLATORS = {
    translator.aggtype: translator
    for translator in (
        DateHistogramBucket,
        DateRangeBucket,
        FiltersBucket,
        HistogramBucket,
        RangeBucket,
        TermsBucket,
    )
}


class BucketTranslator:
    def translate(self, proxy, agg, state, context):
        ret = proxy.bucket(
            agg["id"],
            agg["type"],
            **TRANSLATORS[agg["type"]]().translate(agg, state, context),
        )
        for metric_agg in state["aggs"]:
            if metric_agg["id"] == agg["params"].get("orderBy"):
                MetricTranslator().translate(ret, metric_agg, state)
        if "orderAgg" in agg["params"]:
            order_agg = agg["params"]["orderAgg"]
            ret.bucket(order_agg["id"], order_agg["type"], **order_agg["params"])
        return ret
