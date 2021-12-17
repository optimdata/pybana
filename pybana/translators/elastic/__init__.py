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
        def replace_magic_keywords(node):
            if isinstance(node, list):
                return [replace_magic_keywords(child) for child in node]
            elif isinstance(node, dict):
                ret = {}
                for key, val in node.items():
                    if (
                        key == "interval"
                        and isinstance(val, dict)
                        and val.get("%autointerval%")
                    ):
                        ret[key] = compute_auto_interval("auto", scope.beg, scope.end)
                    elif key == "%timefilter%":
                        if val == "min":
                            ret = scope.beg.isoformat()
                        elif val == "max":
                            ret = scope.end.isoformat()
                        elif val is True:
                            # TODO: handle shift and unit
                            ret = {
                                "min": scope.beg.isoformat(),
                                "max": scope.end.isoformat(),
                            }

                    else:
                        ret[key] = replace_magic_keywords(val)
                return ret
            return node

        def translate_data_item(data):
            if "url" in data:
                index = data["url"]["index"]
                body = replace_magic_keywords(data["url"]["body"])
                search = elasticsearch_dsl.Search(index=index).update_from_dict(body)
                if data["url"].get("%timefield%"):
                    ts = data["url"]["%timefield%"]
                    search = search.filter(
                        "range",
                        **{
                            ts: {
                                "gte": scope.beg.isoformat(),
                                "lte": scope.end.isoformat(),
                            }
                        }
                    )
            else:
                search = elasticsearch_dsl.Search()[:0]
            return search

        spec = hjson.loads(visualization.visState["params"]["spec"])
        data = spec["data"]
        return (
            translate_data_item(data)
            if isinstance(data, dict)
            else [translate_data_item(d) for d in data]
        )

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
