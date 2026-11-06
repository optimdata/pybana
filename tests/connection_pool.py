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


def allow_es():
    return os.environ.get("SKIP_ES", "").upper() != "TRUE"


ELASTIC_ORIGINS: Dict[str, elasticsearch.Elasticsearch] = {}

ELASTIC_CLIENTS: Dict[str, ESClientInterface] = {
    "mock": MockElasticsearchPolluxClient()
}
if allow_es():
    ELASTIC_ORIGINS["v8"] = elasticsearch.Elasticsearch(
        ["http://localhost:9201"], timeout=30
    )
    ELASTIC_CLIENTS["v8"] = ElasticsearchExtClient(ELASTIC_ORIGINS["v8"])
    ELASTIC_CLIENTS["default"] = ELASTIC_CLIENTS["v8"]
    ELASTIC_ORIGINS["v6"] = elasticsearch.Elasticsearch(timeout=10)
    ELASTIC_CLIENTS["v6"] = ElasticsearchExtClient(ELASTIC_ORIGINS["v6"])
else:
    ELASTIC_CLIENTS["default"] = MockElasticsearchPolluxClient()


for k, v in ELASTIC_CLIENTS.items():
    elasticsearch_dsl.connections.add_connection(k, v)


def get_allowed_versions(versions: List[str] = ["mock", "v6", "v8"]) -> List[str]:
    return [version for version in versions if version in ELASTIC_CLIENTS]
