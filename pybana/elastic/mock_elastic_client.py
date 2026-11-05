"""In-memory Elasticsearch 8 stand-in for tests.

The mock speaks the same Python API as ``ElasticsearchPolluxClient`` (the
elasticsearch-py 6 client talking to an Elasticsearch 8 cluster). Nothing is
sent over the network. Seed documents on ``client.memory`` before exercising
the code under test.

Example::

    client = MockElasticsearchPolluxClient()
    client.memory.add_index("logs-0001", aliases=["logs"])
    client.memory.add_document(
        "logs-0001", "1", {"machine_id": 7, "value": 1.5}, doc_type="span"
    )
    client.search(
        index="logs",
        doc_type="span",
        body={"query": {"term": {"machine_id": 7}}},
    )

``load`` replaces the whole store from a dict. ``term``, ``terms``, ``match``,
``range``, ``bool``, ``ids``, ``exists``, ``wildcard``, ``prefix``, and
``query_string`` are applied to that store. A query clause this mock does not
implement is skipped, so it does not hide seeded documents.

Every method call on a mock client is counted in ``client.stats``. Pass
``stats`` to share one counter across clients; otherwise each client gets its own.
``save`` and ``load`` write or read the whole cluster (indices, documents,
templates, pipelines, repositories) as one ``.json``, ``.yml``, or ``.yaml`` file.
"""

import copy
import fnmatch
import json
import math
import os
import re
import types
import uuid
from datetime import datetime, timedelta, timezone
from functools import cmp_to_key, wraps
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union

from elasticsearch.client import SKIP_IN_PATH, TasksClient
from elasticsearch.client.utils import query_params
from elasticsearch.exceptions import (
    ConflictError,
    NotFoundError,
    RequestError,
    TransportError,
)
from elasticsearch.helpers import BulkIndexError
from elasticsearch.serializer import JSONSerializer
from pybana.elastic.elastic_client import ScrollsCache
from pybana.elastic.elastic_client_interface import (
    ESCatInterface,
    ESIndiceInterface,
    ESIngestInterface,
    ElasticsearchBaseInterface,
    ElasticsearchCatInterface,
    ElasticsearchClientInterface,
    ElasticsearchIndiceInterface,
    ElasticsearchIngestInterface,
)
from pybana.elastic.fixes_for_v8 import HISTOGRAMS_CALENDARS, v6_to_v8, v8_to_v6

JsonDict = Dict[str, Any]
Target = Union[str, Sequence[str], None]

# stats["classes"]["MockElasticsearchPolluxClient"]["functions"]["search"]["parameters"]
_MAX_PARAMETER_KEYS = 500


def new_stats() -> JsonDict:
    """Return an empty call-counter tree."""
    return {"stats": {"count": 0}, "classes": {}}


def reset_stats(stats: JsonDict) -> None:
    """Clear ``stats`` in place."""
    stats["stats"]["count"] = 0
    stats["classes"].clear()


def _format_parameter_value(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, (dict, list, tuple)):
        try:
            return json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))
        except (TypeError, ValueError):
            return str(value)
    return str(value)


def _parameter_key(args, kwargs) -> str:
    """Join positional values and ``name=value`` kwargs with commas.

    ``fn("a", "b", "c")`` becomes ``a,b,c``. ``fn(index="logs")`` becomes
    ``index=logs``.
    """
    parts = [_format_parameter_value(arg) for arg in args]
    for name in sorted(kwargs):
        parts.append("%s=%s" % (name, _format_parameter_value(kwargs[name])))
    return ",".join(parts)


def _bump(bucket: JsonDict) -> None:
    bucket["stats"]["count"] += 1


def _stats_of(instance) -> Optional[JsonDict]:
    stats = getattr(instance, "stats", None)
    if isinstance(stats, dict):
        return stats
    parent = getattr(instance, "_parent", None)
    stats = getattr(parent, "stats", None)
    if isinstance(stats, dict):
        return stats
    return None


def _record_call(
    stats: Optional[JsonDict], class_name: str, function_name: str, args, kwargs
) -> None:
    if not stats:
        return
    _bump(stats)
    classes = stats["classes"]
    class_stats = classes.get(class_name)
    if class_stats is None:
        class_stats = {"stats": {"count": 0}, "functions": {}}
        classes[class_name] = class_stats
    _bump(class_stats)
    functions = class_stats["functions"]
    function_stats = functions.get(function_name)
    if function_stats is None:
        function_stats = {"stats": {"count": 0}, "parameters": {}}
        functions[function_name] = function_stats
    _bump(function_stats)
    parameters = function_stats["parameters"]
    key = _parameter_key(args, kwargs)
    if key not in parameters:
        specific = [name for name in parameters if name != "too_much_parameters"]
        if len(specific) >= _MAX_PARAMETER_KEYS:
            key = "too_much_parameters"
    parameter_stats = parameters.get(key)
    if parameter_stats is None:
        parameter_stats = {"stats": {"count": 0}}
        parameters[key] = parameter_stats
    _bump(parameter_stats)


def _instrument(cls):
    """Count calls of methods defined on ``cls`` (not dunder methods)."""

    def wrap(class_name, function_name, func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            _record_call(_stats_of(self), class_name, function_name, args, kwargs)
            return func(self, *args, **kwargs)

        return wrapper

    for name, attr in list(vars(cls).items()):
        if not isinstance(attr, types.FunctionType):
            continue
        if name.startswith("__") and name.endswith("__"):
            continue
        setattr(cls, name, wrap(cls.__qualname__, name, attr))
    return cls


def _yaml():
    try:
        import yaml
    except ImportError as exc:
        raise ImportError(
            "PyYAML is required to save or load .yaml or .yml mock data"
        ) from exc
    return yaml


def _read_mock_file(path) -> JsonDict:
    path = os.fspath(path)
    suffix = os.path.splitext(path)[1].lower()
    with open(path, "r", encoding="utf-8") as handle:
        text = handle.read()
    if not text.strip():
        return {}
    if suffix == ".json":
        data = json.loads(text)
    elif suffix in (".yaml", ".yml"):
        data = _yaml().safe_load(text) or {}
    else:
        raise ValueError("Mock data file must be .json, .yml, or .yaml: %s" % path)
    if not isinstance(data, dict):
        raise ValueError("Mock data file must contain an object: %s" % path)
    return data


class MemoryDocument:
    def __init__(
        self,
        doc_id: str,
        source: JsonDict,
        doc_type: str = "_doc",
        version: int = 1,
        seq_no: int = 0,
    ):
        self.id = doc_id
        self.source = source
        self.doc_type = doc_type
        self.version = version
        self.seq_no = seq_no
        self.primary_term = 1


class MemoryIndex:
    def __init__(self, name: str):
        self.name = name
        self.settings: JsonDict = {}
        self.mappings: JsonDict = {}
        self.aliases: Dict[str, JsonDict] = {}
        self.documents: Dict[str, MemoryDocument] = {}
        self.closed = False


class ElasticMemory:
    """Cluster state a test can fill, replace, or inspect."""

    VERSION = "8.19.3"

    def __init__(self):
        self.indices: Dict[str, MemoryIndex] = {}
        self.templates: Dict[str, JsonDict] = {}
        self.pipelines: Dict[str, JsonDict] = {}
        self.repositories: List[JsonDict] = []
        self._seq = 0

    def next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def add_index(
        self,
        name: str,
        settings: Optional[JsonDict] = None,
        mappings: Optional[JsonDict] = None,
        aliases: Union[None, Dict[str, JsonDict], Sequence[str]] = None,
        closed: Optional[bool] = None,
    ) -> MemoryIndex:
        """Create ``name`` or update the pieces that are not ``None``."""
        meta = self.indices.get(name)
        if meta is None:
            meta = MemoryIndex(name)
            self.indices[name] = meta
        if settings is not None:
            meta.settings = copy.deepcopy(settings)
        if mappings is not None:
            meta.mappings = copy.deepcopy(mappings)
        if aliases is not None:
            meta.aliases = _alias_dict(aliases)
        if closed is not None:
            meta.closed = closed
        return meta

    def add_document(
        self,
        index: str,
        doc_id: Any,
        source: Optional[JsonDict],
        doc_type: str = "_doc",
        version: Optional[int] = None,
        seq_no: Optional[int] = None,
        primary_term: Optional[int] = None,
    ) -> MemoryDocument:
        """Insert or replace one document. Creates ``index`` when missing."""
        meta = self.add_index(index)
        doc_id = str(doc_id)
        current = meta.documents.get(doc_id)
        if version is None:
            version = current.version + 1 if current else 1
        if seq_no is None:
            seq_no = self.next_seq()
        else:
            seq_no = int(seq_no)
            if seq_no > self._seq:
                self._seq = seq_no
        document = MemoryDocument(
            doc_id=doc_id,
            source=copy.deepcopy(source or {}),
            doc_type=doc_type or "_doc",
            version=version,
            seq_no=seq_no,
        )
        if primary_term is not None:
            document.primary_term = int(primary_term)
        meta.documents[doc_id] = document
        return document

    def add_documents(
        self,
        index: str,
        documents: Union[JsonDict, Sequence[JsonDict]],
        doc_type: str = "_doc",
    ) -> None:
        """Replace nothing: append documents.

        A dict maps id to ``_source`` (or to a ``{"_source", "_id", "_type"}``
        wrapper). A list uses each item's ``_id`` and ``_source``.
        """
        self.add_index(index)
        if isinstance(documents, dict):
            items = documents.items()
            for doc_id, source in items:
                wrapped = (
                    isinstance(source, dict)
                    and "_source" in source
                    and any(key in source for key in ("_id", "_type", "_version"))
                )
                if wrapped:
                    self.add_document(
                        index,
                        source.get("_id", doc_id),
                        source.get("_source") or {},
                        doc_type=source.get("_type", doc_type),
                        version=source.get("_version"),
                        seq_no=source.get("_seq_no"),
                        primary_term=source.get("_primary_term"),
                    )
                else:
                    self.add_document(index, doc_id, source or {}, doc_type=doc_type)
            return
        for position, doc in enumerate(documents or []):
            doc = doc or {}
            source = doc.get("_source", doc)
            self.add_document(
                index,
                doc.get("_id", position),
                source if isinstance(source, dict) else {},
                doc_type=doc.get("_type", doc_type),
                version=doc.get("_version"),
                seq_no=doc.get("_seq_no"),
                primary_term=doc.get("_primary_term"),
            )

    def to_dict(self) -> JsonDict:
        """Snapshot of indices, documents, templates, pipelines, and repositories."""
        indices = {}
        for name, meta in self.indices.items():
            indices[name] = {
                "settings": copy.deepcopy(meta.settings),
                "mappings": copy.deepcopy(meta.mappings),
                "aliases": copy.deepcopy(meta.aliases),
                "closed": meta.closed,
                "documents": [
                    {
                        "_id": document.id,
                        "_source": copy.deepcopy(document.source),
                        "_type": document.doc_type,
                        "_version": document.version,
                        "_seq_no": document.seq_no,
                        "_primary_term": document.primary_term,
                    }
                    for document in meta.documents.values()
                ],
            }
        return {
            "version": self.VERSION,
            "seq": self._seq,
            "indices": indices,
            "templates": copy.deepcopy(self.templates),
            "pipelines": copy.deepcopy(self.pipelines),
            "repositories": copy.deepcopy(self.repositories),
        }

    def save(self, path) -> str:
        """Write ``to_dict()`` to a ``.json``, ``.yml``, or ``.yaml`` file."""
        path = os.fspath(path)
        suffix = os.path.splitext(path)[1].lower()
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        if suffix == ".json":
            text = (
                json.dumps(self.to_dict(), indent=2, sort_keys=True, ensure_ascii=False)
                + "\n"
            )
        elif suffix in (".yaml", ".yml"):
            text = _yaml().safe_dump(self.to_dict(), sort_keys=True, allow_unicode=True)
        else:
            raise ValueError("Mock data file must be .json, .yml, or .yaml: %s" % path)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(text)
        return path

    def load(self, data: Union[None, JsonDict, str, os.PathLike]) -> None:
        """Replace indices, templates, pipelines, and repositories.

        ``data`` is a dict, or a path to a ``.json``, ``.yml``, or ``.yaml`` file.
        ``data["indices"]`` maps an index name to ``settings``, ``mappings``,
        ``aliases``, ``closed``, ``doc_type``, and ``documents``.
        """
        if isinstance(data, (str, os.PathLike)):
            data = _read_mock_file(data)
        data = data or {}
        self.indices.clear()
        self.templates.clear()
        self.pipelines.clear()
        self.repositories = []
        self._seq = 0
        for name, spec in (data.get("indices") or {}).items():
            spec = spec or {}
            self.add_index(
                name,
                settings=spec.get("settings"),
                mappings=spec.get("mappings"),
                aliases=spec.get("aliases"),
                closed=spec.get("closed", False),
            )
            if "documents" in spec:
                self.add_documents(
                    name, spec["documents"], doc_type=spec.get("doc_type", "_doc")
                )
        saved_seq = data.get("seq")
        if saved_seq is not None:
            self._seq = max(self._seq, int(saved_seq))
        self.templates = copy.deepcopy(data.get("templates") or {})
        self.pipelines = copy.deepcopy(data.get("pipelines") or {})
        self.repositories = copy.deepcopy(data.get("repositories") or [])


def _alias_dict(
    aliases: Union[Dict[str, JsonDict], Sequence[str]],
) -> Dict[str, JsonDict]:
    if isinstance(aliases, dict):
        return {
            name: copy.deepcopy(body) if isinstance(body, dict) else {}
            for name, body in aliases.items()
        }
    return {name: {} for name in aliases}


def _verify_params(*args):
    for param in args:
        if param in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument.")


def _ignore_tuple(ignore) -> tuple:
    if ignore is None:
        return ()
    if isinstance(ignore, int):
        return (ignore,)
    return tuple(ignore)


def _error_body(
    error_type: str, reason: str, index: Optional[str], status: int
) -> JsonDict:
    cause = {"type": error_type, "reason": reason}
    if index is not None:
        cause["index"] = index
    error = {
        "root_cause": [dict(cause)],
        "type": error_type,
        "reason": reason,
    }
    if index is not None:
        error["index"] = index
    return {"error": error, "status": status}


def _raise(exc_cls, status: int, error_type: str, reason: str, index=None, ignore=()):
    info = _error_body(error_type, reason, index=index, status=status)
    if status in _ignore_tuple(ignore):
        return info
    raise exc_cls(status, error_type, info)


def _shards() -> JsonDict:
    return {"total": 1, "successful": 1, "skipped": 0, "failed": 0}


def _has_wildcard(pattern: str) -> bool:
    return any(char in pattern for char in "*?")


def _split_targets(target: Target) -> List[str]:
    if target is None:
        return []
    if isinstance(target, (list, tuple)):
        names: List[str] = []
        for item in target:
            names.extend(_split_targets(item))
        return names
    return [part.strip() for part in str(target).split(",") if part.strip()]


def _index_pattern_match(name: str, pattern: str) -> bool:
    if pattern in ("_all", "*"):
        return True
    if _has_wildcard(pattern):
        return fnmatch.fnmatchcase(name, pattern)
    return name == pattern


def _name_match(name: str, pattern: str) -> bool:
    if pattern == "*":
        return True
    if _has_wildcard(pattern):
        return fnmatch.fnmatchcase(name, pattern)
    return name == pattern


def _as_dict(body) -> JsonDict:
    if body is None:
        return {}
    if isinstance(body, str):
        body = body.strip()
        if not body:
            return {}
        parsed = json.loads(body)
        return parsed if isinstance(parsed, dict) else {}
    if isinstance(body, dict):
        return body
    return {}


def _deep_merge(base: JsonDict, overlay: JsonDict) -> JsonDict:
    merged = copy.deepcopy(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _get_path(source: Any, path: str):
    current = source
    for part in str(path).split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return None
    return current


def _path_exists(source: Any, path: str) -> bool:
    current = source
    for part in str(path).split("."):
        if not isinstance(current, dict) or part not in current:
            return False
        current = current[part]
    return True


def _set_path(source: JsonDict, path: str, value: Any) -> None:
    parts = str(path).split(".")
    current = source
    for part in parts[:-1]:
        nxt = current.get(part)
        if not isinstance(nxt, dict):
            nxt = {}
            current[part] = nxt
        current = nxt
    current[parts[-1]] = value


def _as_clauses(value) -> List[JsonDict]:
    if not value:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _clause_matches(doc: MemoryDocument, clause: Any) -> bool:
    if not isinstance(clause, dict) or not clause:
        return True
    matched = True
    seen = False
    for key, value in clause.items():
        if key in ("boost", "_name"):
            continue
        seen = True
        matched = matched and _one_clause(doc, key, value)
    return matched if seen else True


def _one_clause(doc: MemoryDocument, key: str, value: Any) -> bool:
    if key == "match_all":
        return True
    if key == "match_none":
        return False
    if key == "bool":
        return _bool_matches(doc, value or {})
    if key == "term":
        return _term_matches(doc, value or {})
    if key == "terms":
        return _terms_matches(doc, value or {})
    if key in ("match", "match_phrase", "match_phrase_prefix"):
        return _match_matches(doc, value or {})
    if key == "range":
        return _range_matches(doc, value or {})
    if key == "ids":
        values = (value or {}).get("values") or []
        return str(doc.id) in {str(item) for item in values}
    if key == "exists":
        field = (value or {}).get("field")
        if not field or not _path_exists(doc.source, field):
            return False
        return _get_path(doc.source, field) is not None
    if key == "wildcard":
        return _wildcard_matches(doc, value or {})
    if key == "prefix":
        return _prefix_matches(doc, value or {})
    if key == "constant_score":
        return _clause_matches(doc, (value or {}).get("filter") or {})
    if key == "query_string":
        return _query_string_matches(doc, value or {})
    # Unsupported clause: keep the document. Tests can still read seeded data.
    return True


def _bool_matches(doc: MemoryDocument, spec: JsonDict) -> bool:
    must = _as_clauses(spec.get("must"))
    filters = _as_clauses(spec.get("filter"))
    should = _as_clauses(spec.get("should"))
    must_not = _as_clauses(spec.get("must_not"))
    if any(not _clause_matches(doc, clause) for clause in must + filters):
        return False
    if any(_clause_matches(doc, clause) for clause in must_not):
        return False
    if should:
        minimum = spec.get("minimum_should_match")
        if minimum is None:
            minimum = 0 if (must or filters) else 1
        hits = sum(1 for clause in should if _clause_matches(doc, clause))
        if hits < int(minimum):
            return False
    return True


def _expected_term(expected):
    if isinstance(expected, dict) and "value" in expected:
        return expected["value"]
    return expected


def _term_matches(doc: MemoryDocument, spec: JsonDict) -> bool:
    for field, expected in spec.items():
        if field in ("boost", "_name"):
            continue
        expected = _expected_term(expected)
        actual = _get_path(doc.source, field)
        if isinstance(actual, list):
            if expected not in actual:
                return False
        elif actual != expected:
            return False
    return True


def _terms_matches(doc: MemoryDocument, spec: JsonDict) -> bool:
    for field, expected in spec.items():
        if field in ("boost", "_name"):
            continue
        if isinstance(expected, dict):
            expected = expected.get("values") or expected.get("value") or []
        actual = _get_path(doc.source, field)
        if isinstance(actual, list):
            if not any(item in expected for item in actual):
                return False
        elif actual not in expected:
            return False
    return True


def _text_match(actual, expected) -> bool:
    if isinstance(actual, list):
        return any(_text_match(item, expected) for item in actual)
    if actual == expected:
        return True
    if isinstance(actual, str) and isinstance(expected, str):
        return expected.lower() in actual.lower().split()
    return False


def _match_matches(doc: MemoryDocument, spec: JsonDict) -> bool:
    for field, expected in spec.items():
        if field in ("boost", "_name"):
            continue
        if isinstance(expected, dict):
            expected = expected.get("query", expected.get("value"))
        if not _text_match(_get_path(doc.source, field), expected):
            return False
    return True


def _as_pair(actual, bound):
    if (
        isinstance(actual, (int, float))
        and isinstance(bound, (int, float))
        and not isinstance(actual, bool)
        and not isinstance(bound, bool)
    ):
        return actual, bound
    return str(actual), str(bound)


def _in_range(actual, bounds: JsonDict) -> bool:
    if actual is None or not isinstance(bounds, dict):
        return False
    checks = (
        ("gte", lambda left, right: left >= right),
        ("gt", lambda left, right: left > right),
        ("lte", lambda left, right: left <= right),
        ("lt", lambda left, right: left < right),
    )
    for key, compare in checks:
        if key in bounds:
            left, right = _as_pair(actual, bounds[key])
            if not compare(left, right):
                return False
    return True


def _range_matches(doc: MemoryDocument, spec: JsonDict) -> bool:
    for field, bounds in spec.items():
        if field in ("boost", "_name"):
            continue
        actual = _get_path(doc.source, field)
        if isinstance(actual, list):
            if not any(_in_range(item, bounds) for item in actual):
                return False
        elif not _in_range(actual, bounds):
            return False
    return True


def _wildcard_matches(doc: MemoryDocument, spec: JsonDict) -> bool:
    for field, pattern in spec.items():
        if field in ("boost", "_name"):
            continue
        if isinstance(pattern, dict):
            pattern = pattern.get("value", "")
        actual = _get_path(doc.source, field)
        if not isinstance(actual, str) or not fnmatch.fnmatchcase(actual, str(pattern)):
            return False
    return True


def _prefix_matches(doc: MemoryDocument, spec: JsonDict) -> bool:
    for field, prefix in spec.items():
        if field in ("boost", "_name"):
            continue
        if isinstance(prefix, dict):
            prefix = prefix.get("value", "")
        actual = _get_path(doc.source, field)
        if not isinstance(actual, str) or not actual.startswith(str(prefix)):
            return False
    return True


def _flatten_values(value, found: List[Any]) -> None:
    if isinstance(value, dict):
        for item in value.values():
            _flatten_values(item, found)
    elif isinstance(value, list):
        for item in value:
            _flatten_values(item, found)
    else:
        found.append(value)


def _query_string_matches(doc: MemoryDocument, spec: JsonDict) -> bool:
    text = spec.get("query", "") if isinstance(spec, dict) else str(spec)
    terms = [part for part in text.split() if part and part not in ("AND", "OR")]
    if not terms:
        return True
    values: List[Any] = []
    _flatten_values(doc.source, values)
    haystack = " ".join(str(item).lower() for item in values)
    return all(term.lower().strip("*") in haystack for term in terms)


def _query_matches(doc: MemoryDocument, body: JsonDict) -> bool:
    if not _clause_matches(doc, body.get("query") or {}):
        return False
    if body.get("post_filter") and not _clause_matches(doc, body["post_filter"]):
        return False
    return True


def _filter_source(source: JsonDict, includes: Optional[Sequence[str]]) -> JsonDict:
    if includes is None:
        return copy.deepcopy(source)
    result: JsonDict = {}
    for field in includes:
        if _path_exists(source, field):
            _set_path(result, field, copy.deepcopy(_get_path(source, field)))
    return result


def _includes_from(
    kwargs: JsonDict, spec: Optional[JsonDict] = None
) -> Optional[List[str]]:
    params = kwargs.get("params") or {}
    raw = (
        kwargs.get("_source_includes")
        or kwargs.get("_source_include")
        or params.get("_source_includes")
        or params.get("_source_include")
    )
    if spec and isinstance(spec.get("_source"), list):
        raw = spec["_source"]
    if raw is None:
        return None
    if isinstance(raw, str):
        return [part.strip() for part in raw.split(",") if part.strip()]
    return list(raw)


def _metric(kind: str, docs: Sequence[MemoryDocument], field: Optional[str]):
    values = []
    for doc in docs:
        value = _get_path(doc.source, field) if field else None
        if isinstance(value, list):
            values.extend(
                item
                for item in value
                if isinstance(item, (int, float)) and not isinstance(item, bool)
            )
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            values.append(value)
        elif kind in ("value_count", "cardinality") and value is not None:
            values.append(value)
    if kind == "value_count":
        return len(values)
    if kind == "cardinality":
        return len(set(values))
    if kind == "sum":
        return sum(values) if values else 0
    if kind == "min":
        return min(values) if values else None
    if kind == "max":
        return max(values) if values else None
    if kind == "avg":
        return (sum(values) / len(values)) if values else None
    return None


_INTERVAL_RE = re.compile(r"^(\d+)(ms|s|m|h|d|w|M|q|y)$")
_NAMED_INTERVALS = {
    "minute": (1, "m"),
    "hour": (1, "h"),
    "day": (1, "d"),
    "week": (1, "w"),
    "month": (1, "M"),
    "quarter": (1, "q"),
    "year": (1, "y"),
}
_PIPELINE_AGGS = ("avg_bucket", "min_bucket", "max_bucket", "sum_bucket")
_MAX_HISTOGRAM_BUCKETS = 10000


def _field_items(doc: MemoryDocument, field: Optional[str]) -> List[Any]:
    if not field:
        return []
    value = _get_path(doc.source, field)
    if value is None:
        return []
    if isinstance(value, list):
        return [item for item in value if item is not None]
    return [value]


def _parse_date(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        seconds = float(value) / 1000.0 if abs(value) >= 10**11 else float(value)
        parsed = datetime.fromtimestamp(seconds, tz=timezone.utc)
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _zone(name: Any):
    if not name or str(name).upper() in ("UTC", "Z", "ETC/UTC"):
        return timezone.utc
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(str(name))
    except Exception:
        try:
            import pytz

            return pytz.timezone(str(name))
        except Exception:
            return timezone.utc


def _epoch_millis(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(round(value.timestamp() * 1000))


def _parse_interval(interval: Any):
    if isinstance(interval, str) and interval in _NAMED_INTERVALS:
        return _NAMED_INTERVALS[interval]
    if not isinstance(interval, str):
        return None
    match = _INTERVAL_RE.match(interval)
    if not match:
        return None
    return int(match.group(1)), match.group(2)


def _interval_delta(amount: int, unit: str) -> Optional[timedelta]:
    if unit == "ms":
        return timedelta(milliseconds=amount)
    if unit == "s":
        return timedelta(seconds=amount)
    if unit == "m":
        return timedelta(minutes=amount)
    if unit == "h":
        return timedelta(hours=amount)
    if unit == "d":
        return timedelta(days=amount)
    if unit == "w":
        return timedelta(weeks=amount)
    return None


def _floor_calendar(value: datetime, amount: int, unit: str, tz) -> datetime:
    local = value.astimezone(tz)
    if unit == "s":
        floored = local.replace(microsecond=0)
        if amount > 1:
            floored = floored.replace(second=(floored.second // amount) * amount)
        return floored
    if unit == "m":
        floored = local.replace(second=0, microsecond=0)
        if amount > 1:
            floored = floored.replace(minute=(floored.minute // amount) * amount)
        return floored
    if unit == "h":
        floored = local.replace(minute=0, second=0, microsecond=0)
        if amount > 1:
            floored = floored.replace(hour=(floored.hour // amount) * amount)
        return floored
    if unit == "d":
        floored = local.replace(hour=0, minute=0, second=0, microsecond=0)
        if amount > 1:
            epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
            days = (floored.astimezone(timezone.utc) - epoch).days
            floored = epoch + timedelta(days=(days // amount) * amount)
        return floored
    if unit == "w":
        floored = local.replace(hour=0, minute=0, second=0, microsecond=0)
        floored = floored - timedelta(days=floored.weekday())
        if amount > 1:
            anchor = datetime(1970, 1, 5, tzinfo=timezone.utc)
            weeks = (floored.astimezone(timezone.utc) - anchor).days // 7
            floored = anchor + timedelta(weeks=(weeks // amount) * amount)
        return floored
    if unit in ("M", "q"):
        step = amount * (3 if unit == "q" else 1)
        month_index = local.year * 12 + (local.month - 1)
        month_index = (month_index // step) * step
        year, month0 = divmod(month_index, 12)
        return local.replace(
            year=year,
            month=month0 + 1,
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
    year = (local.year // amount) * amount
    return local.replace(
        year=year, month=1, day=1, hour=0, minute=0, second=0, microsecond=0
    )


def _floor_fixed(value: datetime, delta: timedelta) -> datetime:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    step = delta.total_seconds()
    seconds = (value.astimezone(timezone.utc) - epoch).total_seconds()
    return epoch + timedelta(seconds=math.floor(seconds / step) * step)


def _add_interval(value: datetime, amount: int, unit: str) -> datetime:
    if unit == "M":
        month_index = value.year * 12 + (value.month - 1) + amount
        year, month0 = divmod(month_index, 12)
        return value.replace(year=year, month=month0 + 1)
    if unit == "q":
        return _add_interval(value, amount * 3, "M")
    if unit == "y":
        return value.replace(year=value.year + amount)
    delta = _interval_delta(amount, unit)
    return value + (delta or timedelta(0))


def _with_subaggs(
    bucket: JsonDict, docs: Sequence[MemoryDocument], sub: Optional[JsonDict]
) -> JsonDict:
    if sub:
        bucket.update(_eval_aggs(docs, sub))
    return bucket


def _eval_terms_agg(
    docs: Sequence[MemoryDocument], params: JsonDict, sub: Optional[JsonDict]
) -> JsonDict:
    field = params.get("field")
    size = int(params.get("size", 10))
    groups: Dict[Any, List[MemoryDocument]] = {}
    for doc in docs:
        placed = set()
        for item in _field_items(doc, field):
            if item in placed:
                continue
            placed.add(item)
            groups.setdefault(item, []).append(doc)
    ordered = sorted(groups.items(), key=lambda item: (-len(item[1]), str(item[0])))
    buckets = []
    for key, grouped in ordered[:size]:
        buckets.append(
            _with_subaggs({"key": key, "doc_count": len(grouped)}, grouped, sub)
        )
    return {
        "doc_count_error_upper_bound": 0,
        "sum_other_doc_count": sum(len(grouped) for _, grouped in ordered[size:]),
        "buckets": buckets,
    }


def _date_histogram_spec(params: JsonDict):
    if params.get("calendar_interval"):
        return params["calendar_interval"], True
    if params.get("fixed_interval"):
        return params["fixed_interval"], False
    interval = params.get("interval")
    return interval, interval in HISTOGRAMS_CALENDARS


def _bucket_start(value: datetime, amount: int, unit: str, tz, calendar: bool):
    if calendar or unit in ("w", "M", "q", "y"):
        return _floor_calendar(value, amount, unit, tz)
    delta = _interval_delta(amount, unit)
    if delta is None:
        return _floor_calendar(value, amount, unit, tz)
    return _floor_fixed(value, delta)


def _histogram_keys(present: Sequence[int], start: datetime, amount: int, unit: str):
    if not present:
        return []
    end_ms = present[-1]
    keys = []
    cursor = start
    while len(keys) <= _MAX_HISTOGRAM_BUCKETS:
        key = _epoch_millis(cursor)
        keys.append(key)
        if key >= end_ms:
            break
        nxt = _add_interval(cursor, amount, unit)
        if nxt <= cursor:
            break
        cursor = nxt
    if len(keys) > _MAX_HISTOGRAM_BUCKETS:
        return list(present)
    return keys


def _eval_date_histogram(
    docs: Sequence[MemoryDocument], params: JsonDict, sub: Optional[JsonDict]
) -> JsonDict:
    parsed = _parse_interval(_date_histogram_spec(params)[0])
    if parsed is None:
        return {"buckets": []}
    amount, unit = parsed
    _, calendar = _date_histogram_spec(params)
    tz = _zone(params.get("time_zone"))
    field = params.get("field")
    groups: Dict[int, List[MemoryDocument]] = {}
    for doc in docs:
        placed = set()
        for raw in _field_items(doc, field):
            value = _parse_date(raw)
            if value is None:
                continue
            key = _epoch_millis(_bucket_start(value, amount, unit, tz, calendar))
            if key in placed:
                continue
            placed.add(key)
            groups.setdefault(key, []).append(doc)
    present = sorted(groups)
    min_doc_count = int(params.get("min_doc_count", 0) or 0)
    if present and min_doc_count <= 0:
        first = datetime.fromtimestamp(present[0] / 1000.0, tz=timezone.utc)
        keys = _histogram_keys(present, first, amount, unit)
    else:
        keys = present
    buckets = []
    for key in keys:
        grouped = groups.get(key, [])
        if len(grouped) < min_doc_count:
            continue
        bucket = {
            "key": key,
            "key_as_string": datetime.fromtimestamp(
                key / 1000.0, tz=timezone.utc
            ).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "doc_count": len(grouped),
        }
        buckets.append(_with_subaggs(bucket, grouped, sub))
    if params.get("keyed"):
        return {"buckets": {bucket["key_as_string"]: bucket for bucket in buckets}}
    return {"buckets": buckets}


def _eval_histogram(
    docs: Sequence[MemoryDocument], params: JsonDict, sub: Optional[JsonDict]
) -> JsonDict:
    try:
        interval = float(params.get("interval") or params.get("fixed_interval"))
        offset = float(params.get("offset") or 0)
    except (TypeError, ValueError):
        return {"buckets": []}
    if interval <= 0:
        return {"buckets": []}
    groups: Dict[float, List[MemoryDocument]] = {}
    for doc in docs:
        placed = set()
        for raw in _field_items(doc, params.get("field")):
            if not isinstance(raw, (int, float)) or isinstance(raw, bool):
                continue
            key = math.floor((raw - offset) / interval) * interval + offset
            if key in placed:
                continue
            placed.add(key)
            groups.setdefault(key, []).append(doc)
    buckets = []
    for key in sorted(groups):
        grouped = groups[key]
        buckets.append(
            _with_subaggs({"key": key, "doc_count": len(grouped)}, grouped, sub)
        )
    return {"buckets": buckets}


def _range_label(value: Any) -> str:
    if value is None:
        return "*"
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, (int, float)):
        return "%s.0" % int(value) if float(value).is_integer() else str(value)
    return str(value)


def _in_agg_range(value: Any, spec: JsonDict, as_date: bool) -> bool:
    if as_date:
        actual = _parse_date(value)
        if actual is None:
            return False
        start = _parse_date(spec["from"]) if "from" in spec else None
        end = _parse_date(spec["to"]) if "to" in spec else None
        if start is not None and actual < start:
            return False
        if end is not None and actual >= end:
            return False
        return True
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return False
    if "from" in spec and value < spec["from"]:
        return False
    if "to" in spec and value >= spec["to"]:
        return False
    return True


def _eval_range_agg(
    docs: Sequence[MemoryDocument],
    params: JsonDict,
    sub: Optional[JsonDict],
    as_date: bool = False,
) -> JsonDict:
    buckets = []
    for spec in params.get("ranges") or []:
        if not isinstance(spec, dict):
            continue
        matched = []
        for doc in docs:
            if any(
                _in_agg_range(item, spec, as_date)
                for item in _field_items(doc, params.get("field"))
            ):
                matched.append(doc)
        bucket = {
            "key": spec.get("key")
            or "%s-%s" % (_range_label(spec.get("from")), _range_label(spec.get("to"))),
            "doc_count": len(matched),
        }
        if "from" in spec:
            bucket["from"] = spec["from"]
        if "to" in spec:
            bucket["to"] = spec["to"]
        buckets.append(_with_subaggs(bucket, matched, sub))
    return {"buckets": buckets}


def _eval_filters_agg(
    docs: Sequence[MemoryDocument], params: JsonDict, sub: Optional[JsonDict]
) -> JsonDict:
    filters = params.get("filters") or {}
    keyed = params.get("keyed", True)
    if isinstance(filters, dict):
        named = list(filters.items())
    else:
        named = [(str(index), clause) for index, clause in enumerate(filters)]
        keyed = False
    buckets: Dict[str, JsonDict] = {}
    ordered = []
    for name, clause in named:
        matched = [doc for doc in docs if _clause_matches(doc, clause)]
        bucket = _with_subaggs({"doc_count": len(matched)}, matched, sub)
        buckets[str(name)] = bucket
        listed = {"key": str(name), "doc_count": bucket["doc_count"]}
        listed.update(
            {key: value for key, value in bucket.items() if key != "doc_count"}
        )
        ordered.append(listed)
    if keyed:
        return {"buckets": buckets}
    return {"buckets": ordered}


def _numeric_items(docs: Sequence[MemoryDocument], field: Optional[str]) -> List[float]:
    values = []
    for doc in docs:
        for item in _field_items(doc, field):
            if isinstance(item, (int, float)) and not isinstance(item, bool):
                values.append(float(item))
    return values


def _percentile(values: Sequence[float], percent: float):
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (percent / 100.0) * (len(ordered) - 1)
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return ordered[low]
    weight = rank - low
    return ordered[low] * (1 - weight) + ordered[high] * weight


def _percent_key(percent: Any) -> str:
    return "%.1f" % float(percent)


def _eval_percentiles(docs: Sequence[MemoryDocument], params: JsonDict) -> JsonDict:
    values = _numeric_items(docs, params.get("field"))
    percents = params.get("percents") or [1, 5, 25, 50, 75, 95, 99]
    return {
        "values": {
            _percent_key(percent): _percentile(values, float(percent))
            for percent in percents
        }
    }


def _eval_percentile_ranks(
    docs: Sequence[MemoryDocument], params: JsonDict
) -> JsonDict:
    values = sorted(_numeric_items(docs, params.get("field")))
    ranks = {}
    for raw in params.get("values") or []:
        target = float(raw)
        if not values:
            ranks[_percent_key(target)] = None
        else:
            below = sum(1 for item in values if item <= target)
            ranks[_percent_key(target)] = 100.0 * below / len(values)
    return {"values": ranks}


def _eval_extended_stats(docs: Sequence[MemoryDocument], params: JsonDict) -> JsonDict:
    values = _numeric_items(docs, params.get("field"))
    count = len(values)
    if count == 0:
        empty = None
        return {
            "count": 0,
            "min": empty,
            "max": empty,
            "avg": empty,
            "sum": 0,
            "sum_of_squares": empty,
            "variance": empty,
            "variance_population": empty,
            "variance_sampling": empty,
            "std_deviation": empty,
            "std_deviation_population": empty,
            "std_deviation_sampling": empty,
        }
    total = sum(values)
    avg = total / count
    sum_squares = sum(value * value for value in values)
    variance_population = sum((value - avg) ** 2 for value in values) / count
    variance_sampling = (
        sum((value - avg) ** 2 for value in values) / (count - 1) if count > 1 else None
    )
    std_population = math.sqrt(variance_population)
    std_sampling = (
        math.sqrt(variance_sampling) if variance_sampling is not None else None
    )
    return {
        "count": count,
        "min": min(values),
        "max": max(values),
        "avg": avg,
        "sum": total,
        "sum_of_squares": sum_squares,
        "variance": variance_population,
        "variance_population": variance_population,
        "variance_sampling": variance_sampling,
        "std_deviation": std_population,
        "std_deviation_population": std_population,
        "std_deviation_sampling": std_sampling,
    }


def _top_hit(doc: MemoryDocument, source) -> JsonDict:
    hit = {"_id": doc.id, "_score": None}
    if source is False:
        return hit
    if source is None:
        hit["_source"] = copy.deepcopy(doc.source)
        return hit
    if isinstance(source, str):
        includes = [source]
    elif isinstance(source, list):
        includes = source
    elif isinstance(source, dict):
        includes = source.get("includes") or []
    else:
        includes = None
    hit["_source"] = (
        copy.deepcopy(doc.source)
        if includes is None
        else _filter_source(doc.source, includes)
    )
    return hit


def _eval_top_hits(docs: Sequence[MemoryDocument], params: JsonDict) -> JsonDict:
    size = int(params.get("size", 3))
    ordered = _sort_documents(docs, params.get("sort"))
    hits = [_top_hit(doc, params.get("_source", True)) for doc in ordered[:size]]
    return {
        "hits": {
            "total": {"value": len(ordered), "relation": "eq"},
            "max_score": None,
            "hits": hits,
        }
    }


def _eval_top_metrics(docs: Sequence[MemoryDocument], params: JsonDict) -> JsonDict:
    size = int(params.get("size", 1))
    ordered = _sort_documents(docs, params.get("sort"))
    metrics = params.get("metrics") or {}
    field = metrics.get("field") if isinstance(metrics, dict) else None
    top = []
    for doc in ordered[:size]:
        value = _get_path(doc.source, field) if field else None
        top.append({"metrics": {field: value} if field else {}})
    return {"top": top}


def _pipeline_kind(spec: JsonDict) -> Optional[str]:
    for kind in _PIPELINE_AGGS:
        if kind in spec:
            return kind
    return None


def _bucket_nodes(node: JsonDict) -> List[JsonDict]:
    buckets = node.get("buckets")
    if isinstance(buckets, list):
        return [bucket for bucket in buckets if isinstance(bucket, dict)]
    if isinstance(buckets, dict):
        return [bucket for bucket in buckets.values() if isinstance(bucket, dict)]
    if "doc_count" in node:
        return [node]
    return []


def _metric_number(bucket: JsonDict, name: str):
    if name == "_count":
        return bucket.get("doc_count", 0)
    metric = bucket.get(name)
    if not isinstance(metric, dict):
        return None
    value = metric.get("value")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    if isinstance(metric.get("std_deviation"), (int, float)):
        return metric["std_deviation"]
    values = metric.get("values")
    if isinstance(values, dict):
        for item in values.values():
            if isinstance(item, (int, float)) and not isinstance(item, bool):
                return item
    return None


def _buckets_path_values(aggs_result: JsonDict, path: Any) -> List[float]:
    if isinstance(path, dict):
        path = next(iter(path.values()), "")
    if not isinstance(path, str) or not path:
        return []
    parts = [part for part in path.split(">") if part]
    node = aggs_result.get(parts[0])
    if not isinstance(node, dict):
        return []
    buckets = _bucket_nodes(node)
    if len(parts) == 1:
        return [float(bucket.get("doc_count", 0)) for bucket in buckets]
    values = []
    metric_name = parts[1]
    for bucket in buckets:
        number = _metric_number(bucket, metric_name)
        if number is not None:
            values.append(float(number))
    return values


def _eval_pipeline(aggs_result: JsonDict, spec: JsonDict) -> JsonDict:
    kind = _pipeline_kind(spec)
    params = spec.get(kind) or {}
    values = _buckets_path_values(aggs_result, params.get("buckets_path"))
    if not values:
        return {"value": 0 if kind == "sum_bucket" else None}
    if kind == "sum_bucket":
        return {"value": sum(values)}
    if kind == "min_bucket":
        return {"value": min(values)}
    if kind == "max_bucket":
        return {"value": max(values)}
    return {"value": sum(values) / len(values)}


def _eval_aggs(docs: Sequence[MemoryDocument], aggs: JsonDict) -> JsonDict:
    result: JsonDict = {}
    pipelines = []
    for name, spec in (aggs or {}).items():
        spec = spec or {}
        if _pipeline_kind(spec):
            pipelines.append((name, spec))
        else:
            result[name] = _eval_agg(docs, spec)
    for name, spec in pipelines:
        result[name] = _eval_pipeline(result, spec)
    return result


def _eval_agg(docs: Sequence[MemoryDocument], spec: JsonDict) -> JsonDict:
    sub = spec.get("aggs") or spec.get("aggregations")
    if "filter" in spec:
        matched = [doc for doc in docs if _clause_matches(doc, spec["filter"])]
        return _with_subaggs({"doc_count": len(matched)}, matched, sub)
    if "filters" in spec:
        return _eval_filters_agg(docs, spec["filters"] or {}, sub)
    for kind in ("terms", "rare_terms", "significant_terms", "significant_text"):
        if kind in spec:
            return _eval_terms_agg(docs, spec[kind] or {}, sub)
    if "date_histogram" in spec:
        return _eval_date_histogram(docs, spec["date_histogram"] or {}, sub)
    if "histogram" in spec:
        return _eval_histogram(docs, spec["histogram"] or {}, sub)
    if "range" in spec:
        return _eval_range_agg(docs, spec["range"] or {}, sub)
    if "date_range" in spec:
        return _eval_range_agg(docs, spec["date_range"] or {}, sub, as_date=True)
    if "top_hits" in spec:
        return _eval_top_hits(docs, spec["top_hits"] or {})
    if "top_metrics" in spec:
        return _eval_top_metrics(docs, spec["top_metrics"] or {})
    if "percentiles" in spec:
        return _eval_percentiles(docs, spec["percentiles"] or {})
    if "percentile_ranks" in spec:
        return _eval_percentile_ranks(docs, spec["percentile_ranks"] or {})
    if "extended_stats" in spec:
        return _eval_extended_stats(docs, spec["extended_stats"] or {})
    if "rate" in spec:
        return {"value": None}
    for kind in ("sum", "avg", "min", "max", "value_count", "cardinality"):
        if kind in spec:
            return {"value": _metric(kind, docs, (spec[kind] or {}).get("field"))}
    return {}


def _apply_painless(
    source: JsonDict, script, params: Optional[JsonDict] = None
) -> None:
    if not script:
        return
    if isinstance(script, str):
        text = script
        script_params = params or {}
    else:
        text = script.get("source") or ""
        script_params = params if params is not None else script.get("params") or {}
    for statement in text.split(";"):
        statement = statement.strip()
        if not statement.startswith("ctx._source.") or "=" not in statement:
            continue
        left, right = statement.split("=", 1)
        path = left.strip()[len("ctx._source.") :]
        right = right.strip()
        if right.startswith("params."):
            value = script_params.get(right[len("params.") :])
        elif right in ("true", "false"):
            value = right == "true"
        elif right == "null":
            value = None
        elif len(right) >= 2 and right[0] == right[-1] and right[0] in ("'", '"'):
            value = right[1:-1]
        else:
            try:
                value = float(right) if "." in right else int(right)
            except ValueError:
                continue
        if path:
            _set_path(source, path, value)


def _sort_documents(docs: Sequence[MemoryDocument], sorts) -> List[MemoryDocument]:
    if not sorts:
        return list(docs)
    if isinstance(sorts, dict):
        sorts = [sorts]
    if isinstance(sorts, str):
        sorts = [sorts]

    def key(doc: MemoryDocument):
        parts = []
        for spec in sorts:
            reverse = False
            if spec in ("_doc", "_id"):
                parts.append((False, doc.id, False))
                continue
            if isinstance(spec, str):
                field = spec
            else:
                field, raw = next(iter(spec.items()))
                if isinstance(raw, str):
                    reverse = raw == "desc"
                elif isinstance(raw, dict):
                    reverse = raw.get("order") == "desc"
            value = doc.id if field in ("_doc", "_id") else _get_path(doc.source, field)
            empty = value is None
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                comparable = value
            else:
                comparable = "" if value is None else str(value)
            parts.append((empty, comparable, reverse))
        return parts

    def compare(left, right):
        if left is right:
            return 0
        pairs = zip(key(left), key(right))
        for (left_empty, left_value, reverse), (right_empty, right_value, _) in pairs:
            if left_empty != right_empty:
                return 1 if left_empty else -1
            if left_value == right_value:
                continue
            try:
                less = left_value < right_value
            except TypeError:
                less = str(left_value) < str(right_value)
            if reverse:
                less = not less
            return -1 if less else 1
        if left.id == right.id:
            return 0
        return -1 if left.id < right.id else 1

    return sorted(docs, key=cmp_to_key(compare))


def _columns(row: JsonDict, headers) -> JsonDict:
    if not headers:
        return row
    if isinstance(headers, str):
        names = [part.strip() for part in headers.split(",") if part.strip()]
    else:
        names = list(headers)
    return {name: row.get(name, "") for name in names}


class _MockTransport:
    def __init__(self):
        self.serializer = JSONSerializer()

    def perform_request(self, method, url, headers=None, params=None, body=None):
        raise NotImplementedError(
            "MockElasticsearchPolluxClient.transport.perform_request "
            "is not implemented (%s %s). Use the client methods instead."
            % (method, url)
        )


class _MockTasks:
    def list(self, **kwargs):
        return {"nodes": {}}

    def get(self, task_id=None, **kwargs):
        return {"completed": True, "task": {"id": task_id, "status": {}}}


class _MockBaseClient(ElasticsearchBaseInterface):
    @property
    def version_major(self) -> int:
        raise NotImplementedError()  # pragma: no cover


class _MockSubClient(ElasticsearchBaseInterface):
    def __init__(self, parent: ElasticsearchBaseInterface):
        self._parent = parent

    @property
    def version_major(self) -> int:
        return self._parent.version_major


class MockElasticsearchIndicePollux(_MockSubClient, ElasticsearchIndiceInterface):
    def __init__(self, parent: ElasticsearchBaseInterface):
        super().__init__(parent)

    @property
    def memory(self) -> ElasticMemory:
        return self._parent.memory

    def fix_mappings(self, original_mappings: Optional[JsonDict]) -> JsonDict:
        if self.version_major < 7:
            return original_mappings or {}
        return v6_to_v8.fix_mappings(original_mappings)

    def _meta(self, name: str, ignore=()) -> MemoryIndex:
        meta = self.memory.indices.get(name)
        if meta is None:
            _raise(
                NotFoundError,
                404,
                "index_not_found_exception",
                "no such index [%s]" % name,
                index=name,
                ignore=ignore,
            )
        return meta

    def _matching_names(
        self, index: Target, include_aliases: bool = False
    ) -> List[str]:
        patterns = _split_targets(index) or ["*"]
        found: List[str] = []
        for pattern in patterns:
            for name, meta in self.memory.indices.items():
                matched = _index_pattern_match(name, pattern)
                if include_aliases and any(
                    _name_match(alias, pattern) for alias in meta.aliases
                ):
                    matched = True
                if matched and name not in found:
                    found.append(name)
        return found

    def _require_names(
        self, index: Target, ignore=(), include_aliases: bool = False
    ) -> List[str]:
        patterns = _split_targets(index)
        wildcard = any(
            _has_wildcard(pattern) or pattern in ("_all", "*") for pattern in patterns
        )
        names = self._matching_names(index, include_aliases=include_aliases)
        if names or wildcard:
            return names
        if patterns:
            _raise(
                NotFoundError,
                404,
                "index_not_found_exception",
                "no such index [%s]" % patterns[0],
                index=patterns[0],
                ignore=ignore,
            )
        return []

    def open(self, index: str, **kwargs) -> bool:
        for name in self._require_names(index, ignore=kwargs.get("ignore")):
            self.memory.indices[name].closed = False
        return True

    def close(self, index: str, **kwargs) -> bool:
        for name in self._require_names(index, ignore=kwargs.get("ignore")):
            self.memory.indices[name].closed = True
        return True

    def create(self, index, body: Optional[JsonDict] = None, **kwargs):
        if index in self.memory.indices:
            ignored = _raise(
                RequestError,
                400,
                "resource_already_exists_exception",
                "index [%s] already exists" % index,
                index=index,
                ignore=kwargs.get("ignore"),
            )
            return ignored
        settings, mappings, aliases = self._template_defaults(index)
        if body is not None and self.version_major >= 7:
            body = v6_to_v8.fix_template(copy.deepcopy(body))
        if body:
            if body.get("settings"):
                settings = _deep_merge(settings, body["settings"])
            if body.get("mappings"):
                mappings = _deep_merge(mappings, body["mappings"])
            if body.get("aliases"):
                aliases.update(_alias_dict(body["aliases"]))
        self.memory.add_index(
            index, settings=settings, mappings=mappings, aliases=aliases
        )
        return 1

    def _template_defaults(self, index: str):
        settings: JsonDict = {}
        mappings: JsonDict = {}
        aliases: Dict[str, JsonDict] = {}
        matched = []
        for body in self.memory.templates.values():
            patterns = body.get("index_patterns", body.get("template", []))
            if isinstance(patterns, str):
                patterns = [
                    part.strip() for part in patterns.split(",") if part.strip()
                ]
            if any(_index_pattern_match(index, pattern) for pattern in patterns):
                matched.append((body.get("order", 0), body))
        for _, body in sorted(matched, key=lambda item: item[0]):
            if body.get("settings"):
                settings = _deep_merge(settings, body["settings"])
            if body.get("mappings"):
                mappings = _deep_merge(mappings, body["mappings"])
            if body.get("aliases"):
                aliases.update(_alias_dict(body["aliases"]))
        return settings, mappings, aliases

    def get_settings(self, index: str, **kwargs):
        result = {}
        for name in self._require_names(index, ignore=kwargs.get("ignore")):
            result[name] = {
                "settings": copy.deepcopy(self.memory.indices[name].settings)
            }
        return result

    def refresh(self, index, **kwargs):
        self._require_names(index, ignore=kwargs.get("ignore"))
        return {"_shards": _shards()}

    def delete(self, index: Optional[str], **kwargs):
        if not index:
            return 0
        patterns = _split_targets(index)
        wildcard = any(
            _has_wildcard(pattern) or pattern in ("_all", "*") for pattern in patterns
        )
        names = self._matching_names(index, include_aliases=False)
        if not names and not wildcard:
            _raise(
                NotFoundError,
                404,
                "index_not_found_exception",
                "no such index [%s]" % (patterns[0] if patterns else index),
                index=patterns[0] if patterns else index,
                ignore=kwargs.get("ignore"),
            )
            return 0
        for name in names:
            self.memory.indices.pop(name, None)
        return len(names)

    def delete_alias(self, index, name, **kwargs):
        if index in ("_all", "*"):
            targets = list(self.memory.indices)
        else:
            targets = self._require_names(index, ignore=kwargs.get("ignore"))
        removed = 0
        for target in targets:
            if name in self.memory.indices[target].aliases:
                self.memory.indices[target].aliases.pop(name, None)
                removed += 1
        if removed == 0 and index not in ("_all", "*"):
            _raise(
                NotFoundError,
                404,
                "aliases_not_found_exception",
                "alias [%s] missing" % name,
                index=index,
                ignore=kwargs.get("ignore"),
            )
        return {"acknowledged": True}

    def delete_template(self, name: str, **kwargs):
        if name not in self.memory.templates:
            _raise(
                NotFoundError,
                404,
                "index_template_missing_exception",
                "template [%s] missing" % name,
                ignore=kwargs.get("ignore"),
            )
            return {"acknowledged": True}
        self.memory.templates.pop(name, None)
        return {"acknowledged": True}

    def delete_safe(self, index_or_alias: str) -> int:
        deleted = set()

        def drop(names: Iterable[str]) -> None:
            for name in names:
                if name in self.memory.indices and name not in deleted:
                    self.memory.indices.pop(name, None)
                    deleted.add(name)

        if self.exists_alias(name=index_or_alias):
            drop(self.get_alias(name=index_or_alias))
        if "*" in index_or_alias or "?" in index_or_alias:
            drop(self._matching_names(index_or_alias, include_aliases=True))
        elif self.exists(index_or_alias):
            drop([index_or_alias])
        return len(deleted)

    def exists(self, index: str) -> bool:
        if not index:
            return False
        return bool(self._matching_names(index, include_aliases=True))

    def get_alias(self, index=None, name=None, **kwargs):
        result = {}
        if index is None and name is None:
            for index_name, meta in self.memory.indices.items():
                if meta.aliases:
                    result[index_name] = {"aliases": copy.deepcopy(meta.aliases)}
        elif index is not None:
            for index_name in self._matching_names(index, include_aliases=True):
                aliases = self.memory.indices[index_name].aliases
                if name is not None:
                    aliases = {
                        key: value
                        for key, value in aliases.items()
                        if _name_match(key, name)
                    }
                    if not aliases:
                        continue
                result[index_name] = {"aliases": copy.deepcopy(aliases)}
        else:
            for index_name, meta in self.memory.indices.items():
                aliases = {
                    key: value
                    for key, value in meta.aliases.items()
                    if _name_match(key, name)
                }
                if aliases:
                    result[index_name] = {"aliases": copy.deepcopy(aliases)}
        if not result:
            _raise(
                NotFoundError,
                404,
                "aliases_not_found_exception",
                "alias missing",
                index=index,
                ignore=kwargs.get("ignore"),
            )
        return result

    def exists_alias(
        self, index: Optional[str] = None, name: Optional[str] = None, **kwargs
    ) -> bool:
        try:
            found = self.get_alias(index=index, name=name)
        except NotFoundError:
            return False
        return any(payload.get("aliases") for payload in found.values())

    def exists_template(self, template_key: str) -> bool:
        return any(_name_match(name, template_key) for name in self.memory.templates)

    def get(self, index, feature=None, **kwargs):
        features = None
        if feature:
            features = (
                list(feature) if isinstance(feature, (list, tuple)) else [feature]
            )
        ignore_unavailable = kwargs.get("ignore_unavailable", False)
        try:
            names = self._require_names(
                index, ignore=kwargs.get("ignore"), include_aliases=True
            )
        except NotFoundError:
            if ignore_unavailable:
                return {}
            raise
        if ignore_unavailable and not names:
            return {}
        result = {}
        for name in names:
            meta = self.memory.indices[name]
            entry: JsonDict = {}
            if not features or "aliases" in features:
                entry["aliases"] = copy.deepcopy(meta.aliases)
            if not features or "mappings" in features:
                entry["mappings"] = copy.deepcopy(meta.mappings)
            if not features or "settings" in features:
                entry["settings"] = copy.deepcopy(meta.settings)
            result[name] = entry
        return result

    def get_mapping(self, index=None, doc_type=None, **kwargs):
        old_type = doc_type
        if self.version_major >= 7:
            doc_type = None
        mapping = {}
        for name in self._require_names(
            index or "*", ignore=kwargs.get("ignore"), include_aliases=True
        ):
            mapping[name] = {
                "mappings": copy.deepcopy(self.memory.indices[name].mappings)
            }
        if self.version_major >= 7 and old_type:
            v8_to_v6.correct_mappings(mapping, doc_type=old_type)
        return mapping

    def get_template(self, name: str, doc_type=None, **kwargs):
        matched = {
            key: copy.deepcopy(body)
            for key, body in self.memory.templates.items()
            if _name_match(key, name)
        }
        if not matched:
            _raise(
                NotFoundError,
                404,
                "index_template_missing_exception",
                "template [%s] missing" % name,
                ignore=kwargs.get("ignore"),
            )
        if self.version_major >= 7 and doc_type:
            v8_to_v6.correct_mappings(matched, doc_type=doc_type)
        return matched

    def put_alias(self, index, name, body=None, **kwargs):
        for target in self._require_names(index, ignore=kwargs.get("ignore")):
            self.memory.indices[target].aliases[name] = copy.deepcopy(body or {})
        return {"acknowledged": True}

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
        _verify_params(doc_type, body)
        body = self.fix_mappings(copy.deepcopy(body))
        ignore = params.get("ignore") if isinstance(params, dict) else None
        if not index:
            _raise(
                NotFoundError,
                404,
                "index_not_found_exception",
                "no such index",
                ignore=ignore,
            )
        for name in self._require_names(index, ignore=ignore, include_aliases=True):
            current = self.memory.indices[name].mappings
            self.memory.indices[name].mappings = _deep_merge(current, body)
        return {"acknowledged": True}

    @query_params(
        "active_only", "bytes", "format", "h", "help", "master_timeout", "s", "v"
    )
    def recovery(self, index=None, params=None):
        return []

    def update_aliases(self, body, **kwargs):
        for action in _as_dict(body).get("actions") or []:
            if "add" in action:
                spec = action["add"]
                self.put_alias(
                    index=spec.get("index"), name=spec.get("alias"), body=spec
                )
            elif "remove" in action:
                spec = action["remove"]
                self.delete_alias(index=spec.get("index"), name=spec.get("alias"))
            elif "remove_index" in action:
                self.delete(action["remove_index"].get("index"))
        return {"acknowledged": True}

    def put_template(self, name: str, body: JsonDict):
        stored = copy.deepcopy(body or {})
        if stored and self.version_major >= 7:
            stored = v6_to_v8.fix_template(stored)
        stored.setdefault("order", 0)
        stored.setdefault("aliases", {})
        stored.setdefault("mappings", {})
        self.memory.templates[name] = stored
        return {"acknowledged": True}

    def rollover(self, alias, new_index=None, body=None, **kwargs):
        current = [
            name
            for name, meta in self.memory.indices.items()
            if alias in meta.aliases and not meta.closed
        ]
        if not current:
            _raise(
                NotFoundError,
                404,
                "index_not_found_exception",
                "no such index [%s]" % alias,
                index=alias,
            )
        old_index = current[-1]
        if not new_index:
            new_index = "%s-000002" % old_index
        self.create(new_index, body=body)
        write_alias = self.memory.indices[old_index].aliases.pop(alias, {})
        self.memory.indices[new_index].aliases[alias] = write_alias
        return {
            "acknowledged": True,
            "shards_acknowledged": True,
            "old_index": old_index,
            "new_index": new_index,
            "rolled_over": True,
            "dry_run": False,
        }


class MockElasticsearchCatPollux(_MockSubClient, ElasticsearchCatInterface):
    def __init__(self, parent: _MockBaseClient):
        super().__init__(parent)

    @property
    def memory(self) -> ElasticMemory:
        return self._parent.memory

    def indices(self, index, **kwargs):
        rows = []
        names = (
            self._parent.indices._matching_names(index, include_aliases=True)
            if index
            else list(self.memory.indices)
        )
        for name in names:
            meta = self.memory.indices[name]
            row = {
                "health": "green",
                "status": "close" if meta.closed else "open",
                "index": name,
                "uuid": name,
                "pri": "1",
                "rep": "0",
                "docs.count": str(len(meta.documents)),
                "docs.deleted": "0",
                "store.size": "1kb",
                "pri.store.size": "1kb",
            }
            rows.append(_columns(row, kwargs.get("h")))
        return rows

    def templates(self, name=None, **kwargs):
        rows = []
        for template_name, body in self.memory.templates.items():
            if name and not _name_match(template_name, name):
                continue
            patterns = body.get("index_patterns", body.get("template", ""))
            if isinstance(patterns, (list, tuple)):
                patterns = ",".join(patterns)
            row = {
                "name": template_name,
                "index_patterns": patterns,
                "order": str(body.get("order", 0)),
                "version": str(body.get("version", "")),
            }
            rows.append(_columns(row, kwargs.get("h")))
        return rows

    def count(self, index=None, **kwargs):
        parent = self._parent
        names = (
            parent.indices._matching_names(index, include_aliases=True)
            if index
            else list(self.memory.indices)
        )
        rows = []
        for name in names:
            rows.append(
                _columns(
                    {
                        "epoch": "0",
                        "timestamp": "00:00:00",
                        "count": str(len(self.memory.indices[name].documents)),
                    },
                    kwargs.get("h"),
                )
            )
        return rows

    def repositories(self, **kwargs):
        return copy.deepcopy(self.memory.repositories)


class MockElasticsearchIngestPollux(_MockSubClient, ElasticsearchIngestInterface):
    def __init__(self, parent: _MockBaseClient):
        super().__init__(parent)

    @property
    def memory(self) -> ElasticMemory:
        return self._parent.memory

    def put_pipeline(self, id, body, **kwargs):
        self.memory.pipelines[id] = copy.deepcopy(body or {})
        return {"acknowledged": True}

    def delete_pipeline(self, id, **kwargs):
        if id not in self.memory.pipelines:
            _raise(
                NotFoundError,
                404,
                "resource_not_found_exception",
                "pipeline [%s] missing" % id,
                ignore=kwargs.get("ignore"),
            )
            return {"acknowledged": True}
        self.memory.pipelines.pop(id, None)
        return {"acknowledged": True}


class MockElasticsearchPolluxClient(
    _MockBaseClient, ElasticsearchClientInterface, ElasticsearchBaseInterface
):
    """Elasticsearch 8 client whose data lives in ``memory``."""

    def __init__(
        self,
        *args,
        memory: Optional[ElasticMemory] = None,
        documents: Optional[Dict[str, Union[JsonDict, Sequence[JsonDict]]]] = None,
        stats: Optional[JsonDict] = None,
        **kwargs,
    ):
        # ``args`` / ``kwargs`` accept the real client constructor (hosts, timeout, ...)
        # and are ignored: this client never opens a connection.
        self.stats = stats if stats is not None else new_stats()
        self._indices = MockElasticsearchIndicePollux(parent=self)
        self._cat = MockElasticsearchCatPollux(parent=self)
        self._ingest = MockElasticsearchIngestPollux(parent=self)
        self.transport = _MockTransport()
        self._tasks = _MockTasks()
        self._scrolls: Dict[str, JsonDict] = {}
        self._scroll_seq = 0
        self.scroll_cache = ScrollsCache()
        self.memory = memory or ElasticMemory()
        if documents:
            for index_name, docs in documents.items():
                self.memory.add_documents(index_name, docs)
        self._version = ElasticMemory.VERSION
        self._version_major = int(self.version.split(".")[0])
        self._name = self.version

    @property
    def indices(self) -> ESIndiceInterface:
        return self._indices

    @property
    def cat(self) -> ESCatInterface:
        return self._cat

    @property
    def ingest(self) -> ESIngestInterface:
        return self._ingest

    @property
    def version(self) -> str:
        return self._version

    @property
    def memory(self) -> ElasticMemory:
        return self._memory

    @memory.setter
    def memory(self, value: ElasticMemory):
        self._memory = value
        self._scrolls.clear()
        self.scroll_cache = ScrollsCache()

    def save(self, path) -> str:
        """Write the cluster to a ``.json``, ``.yml``, or ``.yaml`` file."""
        return self.memory.save(path)

    def load(self, data: Union[None, JsonDict, str, os.PathLike]) -> ElasticMemory:
        """Replace in-memory cluster data from a dict or a ``.json`` / ``.yaml`` file."""
        self.memory.load(data)
        self._scrolls.clear()
        self.scroll_cache = ScrollsCache()
        return self.memory

    @property
    def version_major(self) -> int:
        return self._version_major

    @property
    def tasks(self) -> TasksClient:
        return self._tasks

    @property
    def name(self) -> str:
        return self._name

    def info(self, **kwargs):
        return {
            "name": "mock-elasticsearch",
            "cluster_name": "mock",
            "cluster_uuid": "mock",
            "version": {
                "number": self.version,
                "build_flavor": "default",
                "build_type": "docker",
                "lucene_version": "9.12.0",
            },
            "tagline": "You Know, for Search",
        }

    def _conflict(self, index: str, doc_id: str):
        _raise(
            ConflictError,
            409,
            "version_conflict_engine_exception",
            "[%s][%s]: version conflict" % (index, doc_id),
            index=index,
        )

    def _check_version(
        self, current: Optional[MemoryDocument], version, version_type: Optional[str]
    ) -> Optional[int]:
        if version is None:
            return None
        version = int(version)
        exists = current is not None
        current_version = current.version if current else 0
        if version_type in ("external", "external_gt"):
            if version <= current_version:
                return -1
            return version
        if version_type == "external_gte":
            if version < current_version:
                return -1
            return version
        if exists and current_version != version:
            return -1
        if not exists:
            return -1
        return version

    def _store(
        self,
        index: str,
        doc_id: str,
        source: JsonDict,
        doc_type: str,
        version: Optional[int],
        version_type: Optional[str],
        create_only: bool,
    ) -> JsonDict:
        if index not in self.memory.indices:
            self.memory.add_index(index)
        meta = self.memory.indices[index]
        current = meta.documents.get(doc_id)
        if create_only and current is not None:
            self._conflict(index, doc_id)
        assigned = self._check_version(current, version, version_type)
        if assigned == -1:
            self._conflict(index, doc_id)
        if assigned is None:
            assigned = 1 if current is None else current.version + 1
        document = MemoryDocument(
            doc_id=doc_id,
            source=copy.deepcopy(source or {}),
            doc_type=doc_type or "_doc",
            version=assigned,
            seq_no=self.memory.next_seq(),
        )
        meta.documents[doc_id] = document
        return {
            "_index": index,
            "_type": doc_type or "_doc",
            "_id": doc_id,
            "_version": document.version,
            "result": "created" if current is None else "updated",
            "_shards": {"total": 1, "successful": 1, "failed": 0},
            "_seq_no": document.seq_no,
            "_primary_term": document.primary_term,
        }

    def _create(self, index, id, body, params=None, doc_type="_doc", **kwargs):
        _verify_params(index, id, body)
        return self._store(
            index, str(id), body, doc_type or "_doc", None, None, create_only=True
        )

    def create(self, index, doc_type, id, body, **kwargs):
        if self.version_major >= 7:
            return self._create(
                index=index, id=id, body=body, doc_type=doc_type, **kwargs
            )
        return self._store(
            index,
            str(id),
            body,
            doc_type,
            kwargs.get("version"),
            kwargs.get("version_type"),
            True,
        )

    def index(self, index, doc_type, body, id=None, **kwargs):
        kwargs = dict(kwargs)
        if self.version_major >= 7:
            doc_type = doc_type or "_doc"
            if "version" in kwargs and "version_type" not in kwargs:
                kwargs["version_type"] = "external"
        if id is None:
            id = uuid.uuid4().hex
        try:
            return self._store(
                index,
                str(id),
                body or {},
                doc_type,
                kwargs.get("version"),
                kwargs.get("version_type"),
                create_only=False,
            )
        except ConflictError:
            if "version" in kwargs and isinstance(kwargs["version"], int):
                kwargs["version"] += 1
            else:
                raise
        return self._store(
            index,
            str(id),
            body or {},
            doc_type,
            kwargs.get("version"),
            kwargs.get("version_type"),
            create_only=False,
        )

    def get(self, index, doc_type, id, **kwargs):
        _verify_params(index, id)
        if index not in self.memory.indices and not self.indices.exists(index):
            return _raise(
                NotFoundError,
                404,
                "index_not_found_exception",
                "no such index [%s]" % index,
                index=index,
                ignore=kwargs.get("ignore"),
            )
        names = self.indices._matching_names(index, include_aliases=True)
        doc_id = str(id)
        for name in names:
            document = self.memory.indices[name].documents.get(doc_id)
            if document is None:
                continue
            found = {
                "_index": name,
                "_id": document.id,
                "_version": document.version,
                "_seq_no": document.seq_no,
                "_primary_term": document.primary_term,
                "found": True,
                "_source": _filter_source(document.source, _includes_from(kwargs)),
            }
            if doc_type:
                found["_type"] = doc_type
            return found
        return _raise(
            NotFoundError,
            404,
            "document_missing_exception" if names else "index_not_found_exception",
            "document missing [%s]" % doc_id,
            index=names[0] if names else index,
            ignore=kwargs.get("ignore"),
        )

    def delete(self, index, doc_type, id, **kwargs):
        kwargs = dict(kwargs)
        if self.version_major >= 7:
            version = kwargs.get("version")
            if version and isinstance(version, int):
                kwargs["version"] = version + 1
                kwargs["version_type"] = kwargs.get("version_type", "external")
        if index not in self.memory.indices and not self.indices.exists(index):
            return _raise(
                NotFoundError,
                404,
                "index_not_found_exception",
                "no such index [%s]" % index,
                index=index,
                ignore=kwargs.get("ignore"),
            )
        names = self.indices._matching_names(index, include_aliases=True)
        doc_id = str(id)
        for name in names:
            document = self.memory.indices[name].documents.get(doc_id)
            if document is None:
                continue
            assigned = self._check_version(
                document, kwargs.get("version"), kwargs.get("version_type")
            )
            if assigned == -1:
                self._conflict(name, doc_id)
            self.memory.indices[name].documents.pop(doc_id, None)
            return {
                "_index": name,
                "_type": doc_type or "_doc",
                "_id": doc_id,
                "_version": assigned or document.version,
                "result": "deleted",
                "_shards": {"total": 1, "successful": 1, "failed": 0},
            }
        return _raise(
            NotFoundError,
            404,
            "document_missing_exception",
            "document missing [%s]" % doc_id,
            index=names[0] if names else index,
            ignore=kwargs.get("ignore"),
        )

    def _selected_documents(
        self, index: Target, body: JsonDict, open_only: bool = True
    ) -> List[tuple]:
        if index:
            names = self.indices._matching_names(index, include_aliases=True)
            patterns = _split_targets(index)
            wildcard = any(
                _has_wildcard(pattern) or pattern in ("_all", "*")
                for pattern in patterns
            )
            known = set(self.memory.indices)
            concrete_missing = [
                pattern
                for pattern in patterns
                if pattern not in known
                and not _has_wildcard(pattern)
                and pattern not in ("_all", "*")
                and not any(
                    pattern in meta.aliases for meta in self.memory.indices.values()
                )
            ]
            if concrete_missing and not wildcard:
                _raise(
                    NotFoundError,
                    404,
                    "index_not_found_exception",
                    "no such index [%s]" % concrete_missing[0],
                    index=concrete_missing[0],
                )
        else:
            names = list(self.memory.indices)
        selected = []
        for name in names:
            meta = self.memory.indices[name]
            if open_only and meta.closed:
                continue
            for document in meta.documents.values():
                if _query_matches(document, body):
                    selected.append((name, document))
        return selected

    def _hit(
        self, index: str, document: MemoryDocument, doc_type: str, includes
    ) -> JsonDict:
        hit = {
            "_index": index,
            "_id": document.id,
            "_score": 1.0,
        }
        if doc_type:
            hit["_type"] = doc_type
        hit["_source"] = _filter_source(document.source, includes)
        return hit

    def _search_response(
        self,
        hits: List[JsonDict],
        total: int,
        scroll_id: Optional[str] = None,
        aggs=None,
    ) -> JsonDict:
        response = {
            "took": 1,
            "timed_out": False,
            "_shards": _shards(),
            "hits": {
                "total": total,
                "total_v8": {"value": total, "relation": "eq"},
                "max_score": 1.0 if hits else None,
                "hits": copy.deepcopy(hits),
            },
        }
        if aggs is not None:
            response["aggregations"] = aggs
        if scroll_id:
            response["_scroll_id"] = scroll_id
        return response

    def search(self, index=None, doc_type=None, body=None, **kwargs):
        kwargs = v6_to_v8.fix_search_params(dict(kwargs))
        fixed = v6_to_v8.fix_search_body(body)
        query = _as_dict(fixed)
        old_doc_type = ""
        if (
            isinstance(doc_type, list)
            and len(doc_type) == 1
            and isinstance(doc_type[0], str)
        ):
            old_doc_type = doc_type[0]
        elif isinstance(doc_type, str):
            old_doc_type = doc_type
        matched = self._selected_documents(index, query)
        sorts = kwargs.get("sort", query.get("sort"))
        ordered = _sort_documents([document for _, document in matched], sorts)
        order_index = {id(document): index_name for index_name, document in matched}
        ordered_pairs = [(order_index[id(document)], document) for document in ordered]
        start = int(kwargs.get("from_", kwargs.get("from", query.get("from", 0))) or 0)
        size = kwargs.get("size", query.get("size", 10))
        size = 10 if size is None else int(size)
        includes = _includes_from(kwargs)
        if query.get("_source") is False:
            includes = []
        elif isinstance(query.get("_source"), list):
            includes = query["_source"]
        elif isinstance(query.get("_source"), dict) and query["_source"].get(
            "includes"
        ):
            includes = query["_source"]["includes"]
        page = [
            self._hit(index_name, document, old_doc_type, includes)
            for index_name, document in ordered_pairs[start : start + size]
        ]
        scroll_id = None
        if kwargs.get("scroll"):
            self._scroll_seq += 1
            scroll_id = "mock-scroll-%s" % self._scroll_seq
            self._scrolls[scroll_id] = {
                "hits": [
                    self._hit(index_name, document, old_doc_type, includes)
                    for index_name, document in ordered_pairs
                ],
                "offset": start + len(page),
                "size": size,
                "total": len(ordered_pairs),
            }
        aggs = None
        if query.get("aggs") or query.get("aggregations"):
            aggs = _eval_aggs(
                [document for _, document in ordered_pairs],
                query.get("aggs") or query.get("aggregations"),
            )
        response = self._search_response(
            page, len(ordered_pairs), scroll_id=scroll_id, aggs=aggs
        )
        if scroll_id and old_doc_type:
            self.scroll_cache.add_item(results=response, doc_type=old_doc_type)
        return response

    @query_params("rest_total_hits_as_int")
    def scroll(self, scroll_id=None, body=None, scroll=None, params=None):
        body = _as_dict(body)
        if not scroll_id:
            scroll_id = body.get("scroll_id") or body.get("_scroll_id")
        state = self._scrolls.get(scroll_id)
        if state is None:
            ignore = params.get("ignore") if isinstance(params, dict) else None
            return _raise(
                NotFoundError,
                404,
                "search_context_missing_exception",
                "No search context found for id [%s]" % scroll_id,
                ignore=ignore,
            )
        start = state["offset"]
        size = state["size"]
        page = state["hits"][start : start + size]
        state["offset"] = start + size
        response = self._search_response(page, state["total"], scroll_id=scroll_id)
        fixed = self.scroll_cache.fix_results(response)
        return fixed if fixed is not None else response

    def clear_scroll(self, scroll_id: Optional[str] = None, body=None, **kwargs):
        ids: List[str] = []
        if scroll_id:
            ids.extend(scroll_id if isinstance(scroll_id, list) else [scroll_id])
        if isinstance(body, dict):
            raw = body.get("scroll_id") or body.get("_scroll_id")
            if isinstance(raw, list):
                ids.extend(raw)
            elif isinstance(raw, str):
                ids.append(raw)
        for item in ids:
            self._scrolls.pop(item, None)
            self.scroll_cache.remove(item)
        return {"succeeded": True, "num_freed": len(ids)}

    def count(self, index=None, doc_type=None, body=None, **kwargs):
        matched = self._selected_documents(index, _as_dict(body))
        return {"count": len(matched), "_shards": _shards()}

    def mget(self, body, index=None, doc_type=None, **kwargs):
        payload = _as_dict(body)
        if "docs" in payload:
            specs = payload["docs"]
        else:
            specs = [
                {"_index": index, "_id": doc_id} for doc_id in payload.get("ids") or []
            ]
        docs = []
        for spec in specs:
            target = spec.get("_index") or index
            doc_id = spec.get("_id")
            includes = _includes_from(kwargs, spec)
            try:
                found = self.get(
                    target,
                    doc_type or spec.get("_type"),
                    doc_id,
                    _source_includes=includes,
                )
                if (
                    includes is not None
                    and isinstance(found, dict)
                    and found.get("found")
                ):
                    found["_source"] = _filter_source(
                        found.get("_source") or {}, includes
                    )
                docs.append(found if isinstance(found, dict) else {"found": False})
            except NotFoundError:
                docs.append(
                    {
                        "_index": target,
                        "_id": None if doc_id is None else str(doc_id),
                        "found": False,
                    }
                )
        return {"docs": docs}

    def bulk(self, body, index=None, doc_type=None, **kwargs):
        items = []
        errors = False
        for op, meta, source in _bulk_operations(body):
            if index and "_index" not in meta:
                meta["_index"] = index
            if doc_type and "_type" not in meta:
                meta["_type"] = doc_type
            item = self._apply_bulk_operation(op, meta, source)
            status = next(iter(item.values())).get("status", 500)
            if status >= 300:
                errors = True
            items.append(item)
        return {"took": 1, "errors": errors, "items": items}

    def helpers_bulk(self, actions, *args, **kwargs):
        if self.version_major >= 7:
            actions = v6_to_v8.fix_actions(actions)
        success = 0
        errors = []
        for action in actions or []:
            item = self._apply_helper_action(action)
            info = next(iter(item.values()))
            if 200 <= info.get("status", 500) < 300:
                success += 1
            else:
                errors.append(item)
        if errors and kwargs.get("raise_on_error", True):
            raise BulkIndexError(
                "%i document(s) failed to index." % len(errors), errors
            )
        if kwargs.get("stats_only"):
            return success, len(errors)
        return success, errors

    def _apply_helper_action(self, action) -> JsonDict:
        if isinstance(action, str):
            return self._apply_bulk_operation("index", {}, json.loads(action))
        action = copy.deepcopy(action)
        op = action.pop("_op_type", "index")
        meta = {}
        for key in (
            "_index",
            "_id",
            "_type",
            "_version",
            "_version_type",
            "_routing",
            "_parent",
            "routing",
            "retry_on_conflict",
            "pipeline",
        ):
            if key in action:
                meta[key] = action.pop(key)
        source = action.pop("_source", None)
        if source is None and op != "delete":
            source = action
        return self._apply_bulk_operation(op, meta, source)

    def _apply_bulk_operation(self, op: str, meta: JsonDict, source) -> JsonDict:
        index = meta.get("_index")
        doc_id = meta.get("_id")
        doc_type = meta.get("_type") or "_doc"
        try:
            if op == "delete":
                result = self.delete(
                    index,
                    doc_type,
                    doc_id,
                    version=meta.get("_version"),
                    version_type=meta.get("_version_type"),
                )
                status = 200
            elif op == "create":
                result = self.create(
                    index, doc_type, doc_id, source if isinstance(source, dict) else {}
                )
                status = 201
            elif op == "update":
                result = self._update_existing(
                    index, doc_type, doc_id, source if isinstance(source, dict) else {}
                )
                status = 200
            else:
                result = self.index(
                    index,
                    doc_type,
                    source if isinstance(source, dict) else {},
                    id=doc_id,
                    version=meta.get("_version"),
                    version_type=meta.get("_version_type"),
                )
                status = 201 if result.get("result") == "created" else 200
            if isinstance(result, dict):
                result = dict(result)
                result["status"] = status
                return {op: result}
        except (TransportError, ValueError) as exc:
            status = exc.status_code if isinstance(exc, TransportError) else 400
            error = (
                exc.error
                if isinstance(exc, TransportError)
                else "illegal_argument_exception"
            )
            return {
                op: {
                    "_index": index,
                    "_id": None if doc_id is None else str(doc_id),
                    "status": status,
                    "error": {"type": error, "reason": str(exc)},
                    "result": "error",
                }
            }
        return {op: {"status": 500, "result": "error"}}

    def _update_existing(self, index, doc_type, doc_id, source: JsonDict) -> JsonDict:
        current = self.get(index, doc_type, doc_id)
        if not isinstance(current, dict) or not current.get("found"):
            self._conflict(index, str(doc_id))
        updated = copy.deepcopy(current["_source"])
        if "doc" in source:
            updated = _deep_merge(updated, source["doc"])
        if "script" in source:
            _apply_painless(updated, source["script"], source.get("params"))
        return self.index(index, doc_type, updated, id=doc_id)

    def delete_by_query(self, index, body, doc_type=None, **kwargs):
        matched = self._selected_documents(index, _as_dict(body))
        for index_name, document in matched:
            self.memory.indices[index_name].documents.pop(document.id, None)
        return _by_query_response(total=len(matched), deleted=len(matched))

    def update_by_query(self, index, doc_type=None, body=None, **kwargs):
        payload = _as_dict(body)
        matched = self._selected_documents(index, payload)
        for _, document in matched:
            _apply_painless(
                document.source, payload.get("script"), payload.get("params")
            )
        return _by_query_response(total=len(matched), updated=len(matched))

    def reindex(self, body, **kwargs):
        payload = _as_dict(body)
        source = payload.get("source") or {}
        dest = payload.get("dest") or {}
        matched = self._selected_documents(
            source.get("index"),
            {"query": source.get("query")} if source.get("query") else {},
        )
        target = dest.get("index")
        if target and target not in self.memory.indices:
            self.memory.add_index(target)
        for _, document in matched:
            self.memory.add_document(
                target, document.id, document.source, doc_type=document.doc_type
            )
        return _by_query_response(total=len(matched), created=len(matched))

    def _transport_perform_request(
        self, method, url, headers=None, params=None, body=None
    ):
        return self.transport.perform_request(
            method=method, url=url, headers=headers, params=params, body=body
        )

    def __repr__(self):
        return "<MockElasticsearchPolluxClient version=%s indices=%s>" % (
            self.version,
            list(self.memory.indices),
        )


def _by_query_response(total=0, updated=0, deleted=0, created=0) -> JsonDict:
    return {
        "took": 1,
        "timed_out": False,
        "total": total,
        "updated": updated,
        "created": created,
        "deleted": deleted,
        "batches": 1,
        "version_conflicts": 0,
        "noops": 0,
        "retries": {"bulk": 0, "search": 0},
        "throttled_millis": 0,
        "requests_per_second": -1.0,
        "throttled_until_millis": 0,
        "failures": [],
    }


def _bulk_operations(body):
    if isinstance(body, str):
        lines = [json.loads(line) for line in body.splitlines() if line.strip()]
        lines = list(v6_to_v8.fix_actions(lines))
    elif isinstance(body, list):
        lines = list(v6_to_v8.fix_actions(body))
    else:
        raise RequestError(
            400,
            "illegal_argument_exception",
            {"error": {"reason": "bulk body must be ndjson"}},
        )
    position = 0
    while position < len(lines):
        action = lines[position]
        op, meta = next(iter(action.items()))
        position += 1
        source = None
        if op in ("index", "create", "update"):
            if position >= len(lines):
                raise RequestError(
                    400,
                    "illegal_argument_exception",
                    {
                        "error": {
                            "reason": "bulk action [%s] is missing a source line" % op
                        }
                    },
                )
            source = lines[position]
            position += 1
        yield op, meta or {}, source


_instrument(MockElasticsearchIndicePollux)
_instrument(MockElasticsearchCatPollux)
_instrument(MockElasticsearchIngestPollux)
_instrument(MockElasticsearchPolluxClient)

# Same names as the real client, for tests that import the mock as a drop-in.
# ElasticsearchPolluxClient = MockElasticsearchPolluxClient
# ElasticsearchIndicePollux = MockElasticsearchIndicePollux
# ElasticsearchCatPollux = MockElasticsearchCatPollux
# ElasticsearchIngestPollux = MockElasticsearchIngestPollux
