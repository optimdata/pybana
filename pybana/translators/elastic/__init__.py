# -*- coding: utf-8 -*-

import elasticsearch_dsl
import json

from pybana.translators.elastic.buckets import BucketTranslator
from pybana.translators.elastic.metrics import MetricTranslator
from .filter import FilterTranslator

__all__ = ("ElasticTranslator", "FilterTranslator")


class ElasticTranslator:
    def translate(self, visualization, scope):
        """
        Transform a kibana visualization object into an elasticsearch_dsl Search.

        :param elasticsearch_dsl.Document visualization: Visualization fetched from a kibana index.
        :param Scope scope: Scope to use for data fetching.
        """
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
