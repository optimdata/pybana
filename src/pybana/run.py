import datetime
import elasticsearch
import elasticsearch_dsl
import json
import os
import pytz
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))  # NOQA
from kibana import Kibana, ElasticTranslator, Scope, VegaTranslator


def jsonprint(obj):
    print(json.dumps(obj, indent=2))


client = elasticsearch.Elasticsearch()
elasticsearch_dsl.connections.add_connection("default", client)
kibana = Kibana(".kibana")

beg = datetime.datetime(2019, 10, 14, tzinfo=pytz.utc)
end = beg + datetime.timedelta(days=1, milliseconds=-1)
scope = Scope(beg, end, pytz.UTC)
visualization = kibana.visualization("66530ee0-eec8-11e9-b65e-17a13148974c")
jsonprint(json.loads(visualization.visualization["visState"])["aggs"])

elastic_translator = ElasticTranslator()
search = elastic_translator.translate(visualization, scope)
# jsonprint(search.to_dict())
response = search.execute()
jsonprint(response.aggregations.to_dict())
vega_translator = VegaTranslator()
jsonprint(vega_translator.translate(visualization, response, scope))
