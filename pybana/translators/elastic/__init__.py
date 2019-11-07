# -*- coding: utf-8 -*-

import elasticsearch_dsl
import json

from pybana.translators.elastic.buckets import BucketTranslator
from pybana.translators.elastic.metrics import MetricTranslator
from .filter import FilterTranslator

__all__ = ("ElasticTranslator", "Context", "FilterTranslator")


class Context:
    """
    Context associated to the visualization.

    :param beg datetime: Begin date of the period on which data should be fetched.
    :param end datetime: End date of the period on which data should be fetched.
    :param tzinfo (str, pytz.Timezone): Timezone of the request.
    :param config pybana.Config: Config of the kibana instance.
    """

    def __init__(self, beg, end, tzinfo, config):
        self.beg = beg
        self.end = end
        self.tzinfo = tzinfo
        self.config = config


class ElasticTranslator:
    def translate(self, visualization, context):
        """
        Transform a kibana visualization object into an elasticsearch_dsl Search.

        :param elasticsearch_dsl.Document visualization: Visualization fetched from a kibana index.
        :param context Context: A context is a object with beg (datetime), end (datetime) and tzinfo (pytz.timezone).
        """
        index_pattern = visualization.index()
        index = index_pattern["index-pattern"]["title"]
        ts = index_pattern["index-pattern"]["timeFieldName"]
        search = elasticsearch_dsl.Search(index=index).filter(
            "range",
            **{ts: {"gte": context.beg.isoformat(), "lte": context.end.isoformat()}}
        )
        state = json.loads(visualization.visualization["visState"])
        segment_aggs = [
            agg
            for agg in state["aggs"]
            if agg["schema"] in ("segment", "group", "split")
        ]
        metric_aggs = [agg for agg in state["aggs"] if agg["schema"] in ("metric",)]
        proxy = search.aggs
        for agg in segment_aggs:
            proxy = BucketTranslator().translate(proxy, agg, state, context)
        for agg in metric_aggs:
            MetricTranslator().translate(proxy, agg, state)
        search = search[:0]
        search = search.filter(visualization.filters())
        return search
