import logging
import json
from typing import Any, Dict, List, Optional

from elasticsearch import Elasticsearch, helpers
from elasticsearch.client import (
    SKIP_IN_PATH,
    _make_path,
    IndicesClient,
    CatClient,
    IngestClient,
)
from elasticsearch.exceptions import ConflictError


from elasticsearch.client.utils import query_params
from .fixes_for_v8 import v6_to_v8, v8_to_v6
import datetime

logger = logging.getLogger("elasticsearch")


class ElasticsearchBaseClient:
    def _transport_perform_request(
        self, method, url, headers=None, params=None, body=None
    ):
        raise NotImplementedError()  # pragma: no cover

    @property
    def version_major(self) -> int:
        raise NotImplementedError()  # pragma: no cover


class ElasticsearchSubClient:
    def __init__(self, parent: ElasticsearchBaseClient):
        self._parent = parent

    @property
    def version_major(self):
        return self._parent.version_major


class ElasticsearchExtIndice(ElasticsearchSubClient):
    def __init__(self, parent: ElasticsearchBaseClient, indices: IndicesClient):
        super().__init__(parent)
        self._indices = indices

    def fix_mappings(self, original_mappings: Optional[Dict]) -> Dict:
        if self.version_major < 7:
            return original_mappings or {}
        return v6_to_v8.fix_mappings(original_mappings=original_mappings)

    def open(self, index: str, **kwargs) -> bool:
        return self._indices.open(index, **kwargs)

    def close(self, index: str, **kwargs) -> bool:
        return self._indices.close(index, **kwargs)

    def create(
        self,
        index,
        body: Optional[Dict[str, Any]] = None,
        **kwargs,  # normally: only params
    ):
        if body is not None and self.version_major >= 7:
            body = v6_to_v8.fix_template(body)
        self._indices.create(index=index, body=body, **kwargs)
        return 1

    def refresh(self, index, **kwargs):  # normally: only params
        return self._indices.refresh(index=index, **kwargs)

    def delete(self, index: Optional[str], **kwargs):  # normally: only params
        if not index:
            return 0

        nb_deleted = 0
        if self.version_major >= 7 and "*" in index:
            if self._indices.exists_alias(index=index):
                for i in self._indices.get_alias(index=index):
                    self._indices.delete(i)
                    nb_deleted += 1
        else:
            self._indices.delete(index=index, **kwargs)
            nb_deleted += 1
        return nb_deleted

    def delete_alias(self, index, name, **kwargs):  # normally: only params
        return self._indices.delete_alias(index=index, name=name, **kwargs)

    def delete_template(self, name: str, **kwargs):  # normally: only params
        return self._indices.delete_template(name=name, **kwargs)

    def delete_safe(self, index_or_alias: str) -> int:
        """
        Safe deletion method for indices or aliases
        """
        nb_deleted = 0
        if self._indices.exists_alias(name=index_or_alias):
            for i in self._indices.get_alias(name=index_or_alias):
                self._indices.delete(i)
                nb_deleted += 1
        if "*" in index_or_alias:
            # try to remove all indexes in flags
            if self._indices.exists_alias(index=index_or_alias):
                for i in self._indices.get_alias(index=index_or_alias):
                    self._indices.delete(i)
                    nb_deleted += 1
        elif self._indices.exists(index_or_alias):
            self._indices.delete(index_or_alias)
            nb_deleted += 1
        return nb_deleted

    def exists(self, index: str):
        return self._indices.exists(index)

    def get_alias(self, index=None, name=None, **kwargs):  # normally: only params
        return self._indices.get_alias(index=index, name=name, **kwargs)

    def exists_alias(
        self, index: Optional[str] = None, name: Optional[str] = None, **kwargs
    ):
        return self._indices.exists_alias(index=index, name=name, **kwargs)

    def exists_template(self, template_key: str):
        return self._indices.exists_template(template_key)

    def get(self, index, feature=None, **kwargs):  # normally: only params
        return self._indices.get(index=index, feature=feature, **kwargs)

    def get_mapping(self, index=None, doc_type=None, **kwargs):  # normally: only params
        old_type = doc_type
        if self.version_major >= 7:
            doc_type = None
        mapping = self._indices.get_mapping(index=index, doc_type=doc_type, **kwargs)
        if self.version_major >= 7 and old_type:
            v8_to_v6.correct_mappings(mapping, doc_type=old_type)
        return mapping

    def get_template(self, name: str, doc_type=None, **kwargs):
        x = self._indices.get_template(name=name, **kwargs)
        if self.version_major >= 7 and doc_type:
            v8_to_v6.correct_mappings(x, doc_type=doc_type)
        return x

    def put_alias(self, index, name, body=None, **kwargs):  # normally: only params
        return self._indices.put_alias(index=index, name=name, body=body, **kwargs)

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
        for param in (doc_type, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")
        body = self.fix_mappings(body)
        if self.version_major >= 7:
            doc_type = None
        return self._parent._transport_perform_request(
            "PUT", _make_path(index, "_mapping", doc_type), params=params, body=body
        )

    def recovery(self, index=None, **kwargs):  # normally: only params
        return self._indices.recovery(index=index, **kwargs)

    def update_aliases(self, body, **kwargs):  # normally: only params
        return self._indices.update_aliases(body=body, **kwargs)

    def put_template(self, name: str, body: Dict[str, Any]):
        if self.version_major >= 7:
            body = v6_to_v8.fix_template(body)
        return self._indices.put_template(name=name, body=body)

    def rollover(
        self, alias, new_index=None, body=None, **kwargs
    ):  # normally: only params
        return self._indices.rollover(
            alias=alias, new_index=new_index, body=body, **kwargs
        )


class ElasticsearchExtCat(ElasticsearchSubClient):
    def __init__(self, parent: ElasticsearchBaseClient, cat: CatClient):
        super().__init__(parent)
        self._cat = cat

    def indices(self, index, **kwargs):
        return self._cat.indices(index=index, **kwargs)

    def templates(self, name=None, **kwargs):
        return self._cat.templates(name=name, **kwargs)

    def count(self, index=None, **kwargs):
        return self._cat.count(index=index, **kwargs)

    def repositories(self, **kwargs):  # normally: only params
        return self._cat.repositories(**kwargs)


class ElasticsearchExtIngest(ElasticsearchSubClient):
    def __init__(self, parent: ElasticsearchBaseClient, ingest: IngestClient):
        super().__init__(parent)
        self._ingest = ingest

    def put_pipeline(self, id, body, **kwargs):  # normally: only params
        return self._ingest.put_pipeline(id=id, body=body, **kwargs)

    def delete_pipeline(self, id, **kwargs):
        return self._ingest.delete_pipeline(id, **kwargs)


class ScrollContext:
    def __init__(self, doc_type: str) -> None:
        self.doc_type = doc_type
        self.date = datetime.datetime.now()

    def is_old(self):
        return datetime.datetime.now() - self.date > datetime.timedelta(hours=1)


def _get_scroll_ids(scroll_id, body) -> List[str]:
    def id_to_list(ids):
        return ids if isinstance(ids, list) else [ids] if isinstance(ids, str) else []

    if scroll_id:
        return id_to_list(scroll_id)
    if not isinstance(body, dict):
        return []
    return id_to_list(body.get("_scroll_id") or body.get("scroll_id"))


def _get_scroll_id(scroll_id, body) -> str:
    return next(iter(_get_scroll_ids(scroll_id=scroll_id, body=body)), "")


class ScrollsCache:
    def __init__(self) -> None:
        self.cache: Dict[str, ScrollContext] = {}
        self.nb_scrolls_added: int = 0

    def __len__(self):
        return len(self.cache)

    def add_item(self, results: dict, doc_type: Optional[str]):
        if not doc_type:
            return False
        scroll_id = _get_scroll_id(scroll_id=None, body=results)
        if not scroll_id:
            return False
        self.clear_scroll_cache()
        self.cache[scroll_id] = ScrollContext(doc_type=doc_type)
        self.nb_scrolls_added += 1
        return True

    def fix_results(self, results: Optional[dict]) -> Optional[dict]:
        if not results:
            return results
        scroll_id = _get_scroll_id(scroll_id=None, body=results)
        cached = self.cache.get(scroll_id)
        if not cached:
            return results
        return v8_to_v6.correct_search_result(results=results, doc_type=cached.doc_type)

    def remove(self, scroll_id: str):
        self.cache.pop(scroll_id, "")

    def clear_scroll_cache(self):
        keys = [k for k, v in self.cache.items() if v.is_old()]
        for k in keys:
            self.cache.pop(k)
        return len(keys)


class ElasticsearchExtClient(ElasticsearchBaseClient):
    def __init__(self, es: Optional[Elasticsearch] = None):
        self.es = es or Elasticsearch()
        info = self.es.info()
        assert info is not None
        assert isinstance(info, dict)
        version = info.get("version", {}).get("number")
        self.version = version
        self._version_major = int(self.version.split(".")[0])
        self.name = self.version  # temporary
        self.indices = ElasticsearchExtIndice(parent=self, indices=self.es.indices)
        self.cat = ElasticsearchExtCat(parent=self, cat=self.es.cat)
        self.ingest = ElasticsearchExtIngest(parent=self, ingest=self.es.ingest)
        self.transport = self.es.transport
        v6_to_v8.fix_transport_instance(self.transport)
        self.scroll_cache = ScrollsCache()

    @property
    def version_major(self) -> int:
        return self._version_major

    @property
    def tasks(self):
        return self.es.tasks

    @query_params(
        "parent",
        "pipeline",
        "refresh",
        "routing",
        "timeout",
        "timestamp",
        "ttl",
        "version",
        "version_type",
        "wait_for_active_shards",
    )
    def _create(self, index, id, body, params=None):
        for param in (index, id, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")
        return self.transport.perform_request(
            "PUT", _make_path(index, "_create", id), params=params, body=body
        )

    def bulk(self, body, index=None, doc_type=None, **kwargs):
        if self.version_major >= 7:
            if isinstance(body, str):
                actions = [json.loads(v.strip()) for v in body.split("\n") if v.strip()]
                actions = v6_to_v8.fix_actions(actions)
                body = "\n".join(
                    [
                        json.dumps(action) if isinstance(action, dict) else action
                        for action in actions
                    ]
                )
            doc_type = None
        results = self.es.bulk(body, index=index, doc_type=doc_type, **kwargs)
        return results

    def create(self, index, doc_type, id, body, **kwargs):  # normally: only params
        if self.version_major >= 7:
            return self._create(index=index, id=id, body=body, **kwargs)
        return self.es.create(
            index=index, doc_type=doc_type, id=id, body=body, **kwargs
        )

    def reindex(self, body, **kwargs):
        return self.es.reindex(body=body, **kwargs)

    def delete_by_query(self, index, body, doc_type=None, **kwargs):
        if self.version_major >= 7:
            doc_type = None
        return self.es.delete_by_query(
            index=index, body=body, doc_type=doc_type, **kwargs
        )  # type: ignore

    @query_params(
        "_source",
        "_source_exclude",
        "_source_include",
        "_source_excludes",
        "_source_includes",
        "parent",
        "preference",
        "realtime",
        "refresh",
        "routing",
        "stored_fields",
        "version",
        "version_type",
    )
    def _get(self, index, id, params=None):
        for param in (index, id):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")
        return self.transport.perform_request(
            "GET", _make_path(index, "_doc", id), params=params
        )

    def get(self, index, doc_type, id, **kwargs):  # normally: only paramss
        if self.version_major >= 7:
            document = self._get(index=index, id=id, **kwargs)
            if isinstance(document, dict) and doc_type:
                document["_type"] = doc_type
        else:
            document = self.es.get(index=index, doc_type=doc_type, id=id, **kwargs)
        return document

    def update_by_query(
        self, index, doc_type=None, body=None, **kwargs
    ):  # normally: only params
        if self.version_major >= 7:
            doc_type = None
        return self.es.update_by_query(
            index=index, doc_type=doc_type, body=body, **kwargs
        )

    def count(
        self, index=None, doc_type=None, body=None, **kwargs
    ):  # normally: only params
        if self.version_major >= 7:
            doc_type = None
        return self.es.count(index=index, doc_type=doc_type, body=body, **kwargs)

    def index(self, index, doc_type, body, id=None, **kwargs):
        print(f"indexing doc in index={index}, doc_type={doc_type}, id={id}")
        if self.version_major >= 7:
            doc_type = "_doc"
            if "version" in kwargs and "version_type" not in kwargs:
                kwargs["version_type"] = "external"
            try:
                r = self.es.index(
                    index=index, doc_type=doc_type, body=body, id=id, **kwargs
                )
                return r
            except ConflictError:
                # increase the version of 1 for update
                if "version" in kwargs and isinstance(kwargs["version"], int):
                    kwargs["version"] += 1
                else:
                    raise

        return self.es.index(index=index, doc_type=doc_type, body=body, id=id, **kwargs)

    def search(self, index=None, doc_type=None, body=None, **kwargs):
        old_doc_type: str = ""
        if self.version_major >= 7:
            body = v6_to_v8.fix_search_body(body)
            v6_to_v8.fix_search_params(kwargs)
            if (
                isinstance(doc_type, list)
                and len(doc_type) == 1
                and isinstance(doc_type[0], str)
            ):
                old_doc_type = doc_type[0]
            elif isinstance(doc_type, str):
                old_doc_type = doc_type
            doc_type = None
        search_result = self.es.search(
            index=index, doc_type=doc_type, body=body, **kwargs
        )
        if self.version_major >= 7 and isinstance(search_result, dict):
            search_result = v8_to_v6.correct_search_result(
                results=search_result, doc_type=old_doc_type
            )
            self.scroll_cache.add_item(results=search_result, doc_type=old_doc_type)
        return search_result

    def helpers_bulk(self, actions, *args, **kwargs):
        if self.version_major >= 7:
            actions = v6_to_v8.fix_actions(actions)
        return helpers.bulk(client=self.es, actions=actions, *args, **kwargs)

    def _transport_perform_request(
        self, method, url, headers=None, params=None, body=None
    ):
        return self.transport.perform_request(
            method=method, url=url, headers=headers, params=params, body=body
        )

    @query_params("rest_total_hits_as_int")
    def scroll(self, scroll_id=None, body=None, scroll=None, params=None):
        body = body or {}
        if scroll_id:
            body["scroll_id"] = scroll_id
        if scroll:
            body["scroll"] = scroll
        body = {k: v for k, v in body.items() if v is not None}
        r = self.es.transport.perform_request(
            "GET", _make_path("_search", "scroll"), params=params, body=body
        )
        if isinstance(r, dict):
            return self.scroll_cache.fix_results(r)
        return r

    def clear_scroll(self, scroll_id: Optional[str] = None, body=None, **kwargs):
        for id in _get_scroll_ids(scroll_id=scroll_id, body=body):
            self.scroll_cache.remove(id)
        return self.es.clear_scroll(scroll_id=scroll_id, body=body, **kwargs)

    def mget(self, body, index=None, doc_type=None, **kwargs):
        if self.version_major >= 7:
            doc_type = None
        return self.es.mget(body=body, index=index, doc_type=doc_type, **kwargs)

    def info(self, **kwargs):
        return self.es.info(**kwargs)

    def delete(self, index, doc_type, id, **kwargs):
        if self.version_major >= 7:
            doc_type = "_doc"
            version = kwargs.get("version")
            if version:
                # kwargs["if_seq_no"] = kwargs.get("if_seq_no", 0)
                # kwargs["if_primary_term"] = kwargs.get("if_primary_term", 1)
                # 'Validation Failed: 1: internal versioning can not be used for optimistic concurrency control. Please use `if_seq_no` and `if_primary_term` instead;'
                # TODO check if other solution is possible, bug in elasticdsl.document.delete
                if isinstance(version, int):
                    kwargs["version"] = version + 1
                kwargs["version_type"] = kwargs.get("version_type", "external")
                # kwargs["retry_on_conflict"]='5'
        return self.es.delete(index, doc_type, id, **kwargs)


class ElasticsearchExt(ElasticsearchExtClient):
    def __init__(self, *args, **kwargs):
        super().__init__(es=Elasticsearch(*args, **kwargs))
