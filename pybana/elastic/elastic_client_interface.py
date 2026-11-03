from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Dict, Optional
from elasticsearch.client.utils import query_params
from elasticsearch.client import TasksClient, Transport
from typing import Protocol, runtime_checkable

if TYPE_CHECKING:  # pragma: no cover
    from pybana.elastic.elastic_client import ScrollsCache


@runtime_checkable
class ElasticsearchBaseInterface(Protocol):

    @property
    @abstractmethod
    def version_major(self) -> int:
        raise NotImplementedError()  # pragma: no cover


@runtime_checkable
class ElasticsearchIndiceInterface(Protocol):

    @abstractmethod
    def fix_mappings(self, original_mappings: Optional[Dict]) -> Dict:
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def open(self, index: str, **kwargs) -> bool:
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def close(self, index: str, **kwargs) -> bool:
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def create(
        self,
        index,
        body: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def get_settings(self, index: str, **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def refresh(self, index, **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def delete(self, index: Optional[str], **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def delete_alias(self, index, name, **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def delete_template(self, name: str, **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def delete_safe(self, index_or_alias: str) -> int:
        """
        Safe deletion method for indices or aliases
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def exists(self, index: str):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def get_alias(self, index=None, name=None, **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def exists_alias(
        self, index: Optional[str] = None, name: Optional[str] = None, **kwargs
    ):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def exists_template(self, template_key: str):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def get(self, index, feature=None, **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def get_mapping(self, index=None, doc_type=None, **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def get_template(self, name: str, doc_type=None, **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def put_alias(self, index, name, body=None, **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    @query_params(
        "allow_no_indices",
        "expand_wildcards",
        "ignore_unavailable",
        "include_type_name",
        "master_timeout",
        "timeout",
        "update_all_types",
    )
    def put_mapping(self, doc_type, body, index=None, params=None):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    @query_params(
        "active_only", "bytes", "format", "h", "help", "master_timeout", "s", "v"
    )
    def recovery(self, index=None, params=None):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def update_aliases(self, body, **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def put_template(self, name: str, body: Dict[str, Any]):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def rollover(self, alias, new_index=None, body=None, **kwargs):
        raise NotImplementedError()  # pragma: no cover


@runtime_checkable
class ElasticsearchCatInterface(Protocol):
    @abstractmethod
    def indices(self, index, **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def templates(self, name=None, **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def count(self, index=None, **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def repositories(self, **kwargs):
        raise NotImplementedError()  # pragma: no cover


@runtime_checkable
class ElasticsearchIngestInterface(Protocol):

    @abstractmethod
    def put_pipeline(self, id, body, **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def delete_pipeline(self, id, **kwargs):
        raise NotImplementedError()  # pragma: no cover


@runtime_checkable
class ESIndiceInterface(
    ElasticsearchIndiceInterface, ElasticsearchBaseInterface, Protocol
):
    pass


@runtime_checkable
class ESCatInterface(ElasticsearchCatInterface, ElasticsearchBaseInterface, Protocol):
    pass


@runtime_checkable
class ESIngestInterface(
    ElasticsearchIngestInterface, ElasticsearchBaseInterface, Protocol
):
    pass


@runtime_checkable
class ElasticsearchClientInterface(Protocol):
    transport: Transport
    scroll_cache: "ScrollsCache"

    @property
    @abstractmethod
    def indices(self) -> ESIndiceInterface:
        raise NotImplementedError()  # pragma: no cover

    @property
    @abstractmethod
    def cat(self) -> ESCatInterface:
        raise NotImplementedError()  # pragma: no cover

    @property
    @abstractmethod
    def ingest(self) -> ESIngestInterface:
        raise NotImplementedError()  # pragma: no cover

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError()  # pragma: no cover

    @property
    @abstractmethod
    def version(self) -> str:
        raise NotImplementedError()  # pragma: no cover

    @property
    @abstractmethod
    def version_major(self) -> int:
        raise NotImplementedError()  # pragma: no cover

    @property
    @abstractmethod
    def tasks(self) -> TasksClient:
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def bulk(self, body, index=None, doc_type=None, **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def create(self, index, doc_type, id, body, **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def reindex(self, body, **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def delete_by_query(self, index, body, doc_type=None, **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def get(self, index, doc_type, id, **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def update_by_query(self, index, doc_type=None, body=None, **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def count(self, index=None, doc_type=None, body=None, **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def index(self, index, doc_type, body, id=None, **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def search(self, index=None, doc_type=None, body=None, **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def helpers_bulk(self, actions, *args, **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    @query_params("rest_total_hits_as_int")
    def scroll(self, scroll_id=None, body=None, scroll=None, params=None):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def clear_scroll(self, scroll_id: Optional[str] = None, body=None, **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def mget(self, body, index=None, doc_type=None, **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def info(self, **kwargs):
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def delete(self, index, doc_type, id, **kwargs):
        raise NotImplementedError()  # pragma: no cover


@runtime_checkable
class ESClientInterface(
    ElasticsearchClientInterface, ElasticsearchBaseInterface, Protocol
):
    pass
