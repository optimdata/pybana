import collections
import elasticsearch_dsl
import json

from pybana.translators.elastic.buckets import BucketTranslator
from pybana.translators.elastic.metrics import MetricTranslator


__all__ = ("ElasticTranslator", "Scope")


Scope = collections.namedtuple("Scope", ["beg", "end", "tzinfo"])


class ElasticTranslator:
    def translate(self, visualization, scope):
        """
        Transform a kibana visualization object into an elasticsearch_dsl Search.

        :param elasticsearch_dsl.Document visualization: Visualization fetched from a kibana index.
        :param scope Scope: A scope is a object with beg (datetime), end (datetime) and tzinfo (pytz.timezone).
        """
        index = visualization.index()["index-pattern"]["title"]
        search = elasticsearch_dsl.Search(index=index).filter(
            "range", ts_beg={"gte": scope.beg.isoformat(), "lte": scope.end.isoformat()}
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
            proxy = BucketTranslator().translate(proxy, agg, state, scope)
        for agg in metric_aggs:
            MetricTranslator().translate(proxy, agg, state)
        search = search[:0]
        return search
