# -*- coding: utf-8 -*-

import elasticsearch_dsl
import hjson
import json

from pybana.translators.elastic.buckets import BucketTranslator, compute_auto_interval
from pybana.translators.elastic.metrics import MetricTranslator
from .filter import FilterTranslator

__all__ = ("ElasticTranslator", "FilterTranslator")


class ElasticTranslator:
    def translate_vega(self, visualization, scope):
        # TODO: handle magic keyword %timefilter%
        def replace_autointerval(node):
            if isinstance(node, list):
                return [replace_autointerval(child) for child in node]
            elif isinstance(node, dict):
                ret = {}
                for key, val in node.items():
                    if (
                        key == "interval"
                        and isinstance(val, dict)
                        and val.get("%autointerval%")
                    ):
                        ret[key] = compute_auto_interval("auto", scope.beg, scope.end)
                    else:
                        ret[key] = replace_autointerval(val)
                return ret
            return node

        spec = hjson.loads(visualization.visState["params"]["spec"])
        data = spec["data"] if isinstance(spec["data"], dict) else spec["data"][0]
        if "url" in data:
            index = data["url"]["index"]
            body = replace_autointerval(data["url"]["body"])
            search = elasticsearch_dsl.Search(index=index).from_dict(body)
            if data["url"].get("%timefield%"):
                ts = data["url"]["%timefield%"]
                search = search.filter(
                    "range",
                    **{ts: {"gte": scope.beg.isoformat(), "lte": scope.end.isoformat()}}
                )
        else:
            search = elasticsearch_dsl.Search()[:0]
        return search

    def translate_legacy(self, visualization, scope):
        index_pattern = visualization.index()
        index = index_pattern["index-pattern"]["title"]
        ts = index_pattern["index-pattern"]["timeFieldName"]
        search = elasticsearch_dsl.Search(index=index).filter(
            "range",
            **{ts: {"gte": scope.beg.isoformat(), "lte": scope.end.isoformat()}}
        )
        state = json.loads(visualization.visualization["visState"])
        segment_aggs = [
            agg
            for agg in state["aggs"]
            if agg["schema"] in ("segment", "group", "split", "bucket")
        ]
        metric_aggs = [agg for agg in state["aggs"] if agg["schema"] in ("metric",)]
        proxy = search.aggs
        for agg in segment_aggs:
            proxy = BucketTranslator().translate(proxy, agg, state, scope)
        for agg in metric_aggs:
            MetricTranslator().translate(proxy, agg, state)
        search = search[:0]
        search = search.filter(visualization.filters())
        return search

    def translate(self, visualization, scope):
        """
        Transform a kibana visualization object into an elasticsearch_dsl Search.

        :param elasticsearch_dsl.Document visualization: Visualization fetched from a kibana index.
        :param Scope scope: Scope to use for data fetching.
        """
        if visualization.visState["type"] == "vega":
            return self.translate_vega(visualization, scope)
        else:
            return self.translate_legacy(visualization, scope)
