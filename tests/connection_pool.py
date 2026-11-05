import os
import sys
from typing import Dict, List
import elasticsearch
import elasticsearch_dsl

BASE_DIRECTORY = os.path.join(os.path.dirname(__file__), "..")  # NOQA
sys.path.insert(0, BASE_DIRECTORY)  # NOQA

from pybana.elastic.mock_elastic_client import (  # noqa: E402
    MockElasticsearchPolluxClient,
)
from pybana.elastic.elastic_client_interface import ESClientInterface  # noqa: E402
from pybana.elastic.elastic_client import ElasticsearchExtClient  # noqa: E402

def _allow_es():
    return os.environ.get("SKIP_ES", "").upper() != "TRUE"


ELASTIC_ORIGINS: Dict[str, elasticsearch.Elasticsearch] = {}

ELASTIC_CLIENTS: Dict[str, ESClientInterface] = {
    "mock": MockElasticsearchPolluxClient()
}
if _allow_es():
    es_v6 = elasticsearch.Elasticsearch()
    es_v8 = elasticsearch.Elasticsearch(["http://localhost:9201"])
    ELASTIC_ORIGINS.update({"v6": es_v6, "v8": es_v8})
    cl_v6 = ElasticsearchExtClient()
    cl_v8 = ElasticsearchExtClient(es_v8)
    ELASTIC_CLIENTS.update({"default": cl_v8, "v6x": cl_v6, "v8": cl_v8})
else:
    ELASTIC_CLIENTS["default"] = MockElasticsearchPolluxClient()


for k, v in ELASTIC_CLIENTS.items():
    elasticsearch_dsl.connections.add_connection(k, v)


def get_allowed_versions(versions: List[str] = ["mock", "v6", "v8"]) -> List[str]:
    return [version for version in versions if version in ELASTIC_CLIENTS]
