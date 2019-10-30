import json

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
        return "30m"
    return f"1{interval}"


def format_from_interval(interval):
    if interval.endswith("y"):
        return "yyyy"
    if interval.endswith("q") or interval.endswith("M"):
        return "yyyy-MM"
    if interval.endswith("d"):
        return "yyyy-MM-dd"
    if interval.endswith("h"):
        return "yyyy-MM-dd'T'HH'h'"
    return "date_time"


class BaseBucket:
    def translate(self, agg, state, scope):
        return json.loads(agg["params"].get("json") or "{}")


class DateHistogramBucket(BaseBucket):
    aggtype = "date_histogram"

    def translate(self, agg, state, scope):
        interval = compute_auto_interval(
            agg["params"]["interval"], scope.beg, scope.end
        )

        return {
            "field": agg["params"]["field"],
            "interval": interval,
            "time_zone": str(scope.tzinfo),
            "format": format_from_interval(interval),
            **super().translate(agg, state, scope),
        }


class DateRangeBucket(BaseBucket):
    aggtype = "date_range"

    def translate(self, agg, state, scope):
        return {
            "field": agg["params"]["field"],
            "ranges": agg["params"]["ranges"],
            **super().translate(agg, state, scope),
        }


class FiltersBucket(BaseBucket):
    aggtype = "filters"

    def translate(self, agg, state, scope):
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
        return {"filters": filters, **super().translate(agg, state, scope)}


class HistogramBucket(BaseBucket):
    aggtype = "histogram"

    def translate(self, agg, state, scope):
        return {
            "field": agg["params"]["field"],
            "interval": agg["params"]["interval"],
            **super().translate(agg, state, scope),
        }


class RangeBucket(BaseBucket):
    aggtype = "range"

    def translate(self, agg, state, scope):
        return {
            "field": agg["params"]["field"],
            "ranges": agg["params"]["ranges"],
            **super().translate(agg, state, scope),
        }


class TermsBucket(BaseBucket):
    aggtype = "terms"

    def translate(self, agg, state, scope):
        orderby = agg["params"]["orderBy"]
        aggs = {agg["id"]: agg for agg in state["aggs"]}
        if orderby in aggs and aggs[orderby]["type"] == "count":
            orderby = "_count"
        return {
            "field": agg["params"]["field"],
            "size": agg["params"]["size"],
            "order": {orderby: agg["params"]["order"]},
            **super().translate(agg, state, scope),
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
    def translate(self, proxy, agg, state, scope):
        return proxy.bucket(
            agg["id"],
            agg["type"],
            **TRANSLATORS[agg["type"]]().translate(agg, state, scope),
        )
