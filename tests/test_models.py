# -*- coding: utf-8 -*-
"""Unit tests for Kibana saved-object models."""

import json
import os
import sys

BASE_DIRECTORY = os.path.join(os.path.dirname(__file__), "..")  # NOQA
sys.path.insert(0, BASE_DIRECTORY)  # NOQA

from typing import Optional, List  # noqa: E402
from unittest.mock import patch  # noqa: E402

import pytest  # noqa: E402
from elasticsearch import NotFoundError  # noqa: E402

import pybana.models as models  # noqa: E402
from pybana.models import (  # noqa: E402
    Config,
    Dashboard,
    DataView,
    DocumentT,
    IndexPattern,
    KibanaInterface,
    Search,
    Visualization,
    get_index_pattern_or_data_view,
    get_index_pattern_or_data_view_flexible,
)


def _hit(source, index=".kibana", id="doc:1"):
    return {"_index": index, "_id": id, "_type": "doc", "_source": source}


def _not_found():
    return NotFoundError(404, "missing", {})


class _Panel(dict):
    @property
    def type(self):
        return self["type"]


class _Hit(object):
    def __init__(self, id):
        self.meta = type("Meta", (), {"id": id})()


class _Response(object):
    def __init__(self, ids):
        self.hits = [_Hit(item_id) for item_id in ids]


class _Search(object):
    def __init__(self, responses, index=None, using=None, **kwargs):
        self.responses = responses
        self.index = index
        self.using = using
        self.obj_type = None
        self.filters = []

    def filter(self, name, **kwargs):
        if name == "term":
            self.obj_type = kwargs["type"]
        self.filters.append((name, kwargs))
        return self

    def extra(self, **kwargs):
        self.size = kwargs.get("size")
        return self

    def execute(self):
        return _Response(self.responses.get(self.obj_type, []))


class _MyMockKibana(KibanaInterface):
    def get_document(
        self, klass: type[DocumentT], id: str, using=None, raise_on_missing=True
    ) -> Optional[DocumentT]:
        return f"{klass._type}:{id}"

    def all_documents(
        self,
        klass: type[DocumentT],
        ids=None,
        raise_on_missing=True,
    ) -> List[DocumentT]:
        if ids is None:
            return [self.get_document(klass, "0")]
        else:
            return [self.get_document(klass, id) for id in ids]
        raise NotImplementedError()  # pragma: no cover


def test_json_attributes_are_cached_and_default_to_null():
    pattern = IndexPattern.from_es(
        _hit(
            {
                "type": "index-pattern",
                "index-pattern": {"title": "logs-*", "fields": '[{"name": "host"}]'},
            },
            id="index-pattern:1",
        )
    )
    assert pattern.fields == [{"name": "host"}]
    assert pattern.fields == [{"name": "host"}]
    assert pattern.fieldFormatMap is None
    assert pattern.fieldFormatMap is None
    with pytest.raises(AttributeError):
        pattern.not_a_field

    config = Config.from_es(
        _hit({"type": "config", "config": {"buildNum": 1}}, id="config:1")
    )
    assert config.type == "config"
    view = DataView()
    assert view.type == "data-view"


def test_from_es_captures_kibana_references():
    missing = Visualization.from_es({"_index": ".kibana", "_id": "visualization:0"})
    assert missing._kibana_references == []
    empty = Search.from_es(_hit({"type": "search", "references": [], "search": {}}))
    assert empty._kibana_references == []
    refs = [{"name": "index", "type": "index-pattern", "id": "abc"}]
    saved = Search.from_es(
        _hit({"type": "search", "references": refs, "search": {}}, id="search:1")
    )
    assert saved._kibana_references == refs
    assert saved._kibana_references is not refs


def test_get_index_pattern_or_data_view():
    kibana = _MyMockKibana()
    assert (
        get_index_pattern_or_data_view("data-view:1", ".kibana", kibana=kibana)
        == "data-view:1"
    )
    assert (
        get_index_pattern_or_data_view("index-pattern:2", ".kibana", kibana=kibana)
        == "index-pattern:2"
    )

    with patch.object(DataView, "get", return_value="view") as view_get:
        with patch.object(IndexPattern, "get", return_value="pattern") as pattern_get:
            assert (
                get_index_pattern_or_data_view(
                    "data-view:1", ".kibana", using="u", kibana=None
                )
                == "view"
            )
            assert (
                get_index_pattern_or_data_view(
                    "index-pattern:2", ".kibana", using="u", kibana=None
                )
                == "pattern"
            )
    # test without kibana
    view_get.assert_called_once_with(id="data-view:1", index=".kibana", using="u")
    pattern_get.assert_called_once_with(
        id="index-pattern:2", index=".kibana", using="u"
    )


def test_flexible_resolution_tries_ids_then_titles():
    assert get_index_pattern_or_data_view_flexible("", ".kibana") is None
    assert get_index_pattern_or_data_view_flexible(None, ".kibana") is None

    def index_get(id, index=None, using=None):
        if id in (
            "index-pattern:missing",
            "index-pattern:neither",
            "index-pattern:view-title",
            "index-pattern:logs-*",
        ):
            raise _not_found()
        return ("index-pattern", id, index, using)

    def view_get(id, index=None, using=None):
        if id in ("data-view:neither", "data-view:view-title", "data-view:logs-*"):
            raise _not_found()
        return ("data-view", id, index, using)

    with patch.object(IndexPattern, "get", side_effect=index_get):
        with patch.object(DataView, "get", side_effect=view_get):
            with patch.object(
                models,
                "get_index_pattern_or_data_view",
                wraps=get_index_pattern_or_data_view,
            ) as wrapped:
                assert get_index_pattern_or_data_view_flexible(
                    "data-view:direct", ".kibana", using="u", kibana=None
                ) == ("data-view", "data-view:direct", ".kibana", "u")
                assert get_index_pattern_or_data_view_flexible(
                    "index-pattern:direct", ".kibana", using="u", kibana=None
                ) == ("index-pattern", "index-pattern:direct", ".kibana", "u")
                assert wrapped.call_count == 2

            assert get_index_pattern_or_data_view_flexible(
                "found", ".kibana", using="u"
            ) == (
                "index-pattern",
                "index-pattern:found",
                ".kibana",
                "u",
            )
            assert get_index_pattern_or_data_view_flexible(
                "missing", ".kibana", using="u"
            ) == (
                "data-view",
                "data-view:missing",
                ".kibana",
                "u",
            )

            responses = {
                "index-pattern": ["index-pattern:by-title"],
                "data-view": ["data-view:by-title"],
            }
            with patch.object(
                models,
                "EsSearch",
                side_effect=lambda **kwargs: _Search(responses, **kwargs),
            ):
                found = get_index_pattern_or_data_view_flexible(
                    "logs-*", ".kibana", using="u"
                )
            assert found == ("index-pattern", "index-pattern:by-title", ".kibana", "u")

            view_only = {"index-pattern": [], "data-view": ["data-view:from-title"]}
            with patch.object(
                models,
                "EsSearch",
                side_effect=lambda **kwargs: _Search(view_only, **kwargs),
            ):
                found = get_index_pattern_or_data_view_flexible(
                    "view-title", ".kibana", using="u"
                )
            assert found == ("data-view", "data-view:from-title", ".kibana", "u")

            with patch.object(
                models,
                "EsSearch",
                side_effect=lambda **kwargs: _Search({}, **kwargs),
            ):
                assert (
                    get_index_pattern_or_data_view_flexible(
                        "neither", ".kibana", using="u"
                    )
                    is None
                )


def test_search_index_resolution():
    source = json.dumps({"index": "logs"})
    saved = Search.from_es(
        _hit(
            {
                "type": "search",
                "references": [{"type": "index-pattern", "id": "unused"}],
                "search": {"kibanaSavedObjectMeta": {"searchSourceJSON": source}},
            },
            id="search:1",
        )
    )
    with patch.object(
        models, "get_index_pattern_or_data_view", return_value="pattern"
    ) as getter:
        assert saved.index(using="u") == "pattern"
    getter.assert_called_once_with(
        "index-pattern:logs", ".kibana", using="u", kibana=None
    )

    unresolved = Search()
    unresolved.meta.index = ".kibana"
    unresolved.search = {
        "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({"query": {}})}
    }
    with pytest.raises(ValueError):
        unresolved.index(using="u")
    assert unresolved.index(using="u", raise_error=False) is None


def test_visualization_index_filters_and_related_search():
    related = type("Related", (), {})()
    related.calls = []

    def related_index(using, raise_error=True):
        related.calls.append((using, raise_error))
        return "from-search"

    related.index = related_index
    linked = Visualization.from_es(
        _hit(
            {
                "type": "visualization",
                "visualization": {
                    "savedSearchId": "abc",
                    "visState": json.dumps({"type": "histogram"}),
                    "kibanaSavedObjectMeta": {"searchSourceJSON": "{}"},
                },
            },
            id="visualization:linked",
        )
    )
    with patch.object(Search, "get", return_value=related) as search_get:
        assert linked.index(using="u", raise_error=False) == "from-search"
    search_get.assert_called_once_with(id="search:abc", index=".kibana", using="u")
    assert related.calls == [("u", False)]
    with patch.object(Search, "get", return_value="saved-search") as search_get:
        assert linked.related_search(using="conn") == "saved-search"
    search_get.assert_called_once_with(id="search:abc", index=".kibana", using="conn")

    source = json.dumps(
        {"indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index", "filter": []}
    )
    referenced = Visualization.from_es(
        _hit(
            {
                "type": "visualization",
                "references": [
                    {
                        "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                        "type": "data-view",
                        "id": "dv1",
                    }
                ],
                "visualization": {
                    "visState": json.dumps({"type": "histogram"}),
                    "uiStateJSON": json.dumps({"vis": {}}),
                    "kibanaSavedObjectMeta": {"searchSourceJSON": source},
                },
            },
            id="visualization:ref",
        )
    )
    assert referenced.uiStateJSON == {"vis": {}}
    assert referenced.uiStateJSON == {"vis": {}}
    with patch.object(
        models, "get_index_pattern_or_data_view", return_value="view"
    ) as getter:
        assert referenced.index(using="u") == "view"
    getter.assert_called_once_with("data-view:dv1", ".kibana", using="u", kibana=None)

    controls = Visualization()
    controls.meta.index = ".kibana"
    controls.visualization = {
        "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({"query": {}})},
        "visState": json.dumps(
            {
                "type": "input_control_vis",
                "params": {"controls": [{"indexPattern": "logs-*"}]},
            }
        ),
    }
    with patch.object(
        models,
        "get_index_pattern_or_data_view_flexible",
        side_effect=["pattern", None, None],
    ) as flexible:
        assert controls.index(using="u") == "pattern"
        assert controls.index(using="u", raise_error=False) is None
        with pytest.raises(ValueError):
            controls.index(using="u")
    assert flexible.call_count == 3

    plain = Visualization()
    plain.meta.index = ".kibana"
    plain.visualization = {
        "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({"index": "inline"})},
        "visState": json.dumps({"type": "metric"}),
    }
    with patch.object(
        models, "get_index_pattern_or_data_view", return_value="inline"
    ) as getter:
        assert plain.index(using="u") == "inline"
    getter.assert_called_once_with(
        "index-pattern:inline", ".kibana", using="u", kibana=None
    )

    missing = Visualization()
    missing.meta.index = ".kibana"
    missing.visualization = {
        "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({"query": {}})},
        "visState": json.dumps({"type": "metric"}),
    }
    with pytest.raises(ValueError):
        missing.index(using="u")
    assert missing.index(using="u", raise_error=False) is None
    assert missing.uiStateJSON is None

    filtered = Visualization.from_es(
        _hit(
            {
                "type": "visualization",
                "visualization": {
                    "visState": "{}",
                    "kibanaSavedObjectMeta": {
                        "searchSourceJSON": json.dumps(
                            {
                                "filter": [
                                    {
                                        "meta": {
                                            "disabled": False,
                                            "negate": False,
                                            "type": "exists",
                                            "key": "host",
                                        }
                                    }
                                ]
                            }
                        )
                    },
                },
            },
            id="visualization:filters",
        )
    )
    assert "host" in json.dumps(filtered.filters().to_dict())
    unfiltered = Visualization()
    unfiltered.visualization = {
        "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({"query": {}})}
    }
    assert unfiltered.filters().to_dict() == {"match_all": {}}


def test_dashboard_loads_visualizations_and_searches():
    dashboard = Dashboard.from_es(
        _hit(
            {
                "type": "dashboard",
                "references": [
                    {"type": "visualization", "id": "v1"},
                    {"type": "search", "id": "s1"},
                    {"type": "lens", "id": "l1"},
                ],
                "dashboard": {"panelsJSON": "[]", "optionsJSON": "{}"},
            },
            id="dashboard:1",
        )
    )
    assert dashboard.optionsJSON == {}
    assert dashboard.optionsJSON == {}
    with patch.object(Visualization, "mget", return_value=["vis"]) as visualizations:
        assert dashboard.visualizations(using="u") == ["vis"]
    assert dashboard.visualizations(kibana=_MyMockKibana()) == ["visualization:v1"]
    visualizations.assert_called_once_with(
        docs=["visualization:v1"], index=".kibana", missing="skip", using="u"
    )
    with patch.object(Search, "mget", return_value=["search"]) as searches:
        assert dashboard.searches(using="u", missing="raise") == ["search"]
    searches.assert_called_once_with(
        docs=["search:s1"], index=".kibana", missing="raise", using="u"
    )

    empty = Dashboard.from_es(
        _hit(
            {
                "type": "dashboard",
                "references": [],
                "dashboard": {"panelsJSON": "[]"},
            },
            id="dashboard:empty",
        )
    )
    assert empty.visualizations(using="u") == []
    assert empty.searches(using="u") == []
    assert empty.panelsJSON == []

    legacy = Dashboard.from_es(
        _hit(
            {
                "type": "dashboard",
                "dashboard": {
                    "panelsJSON": json.dumps(
                        [
                            {"type": "visualization", "id": "v9"},
                            {"type": "search", "id": "s9"},
                            {"type": "lens", "id": "l9"},
                        ]
                    )
                },
            },
            id="dashboard:legacy",
        )
    )
    original_loads = models.json.loads

    def loads(text, *args, **kwargs):
        data = original_loads(text, *args, **kwargs)
        if isinstance(data, list):
            return [_Panel(item) if isinstance(item, dict) else item for item in data]
        return data

    with patch.object(models.json, "loads", loads):
        with patch.object(
            Visualization, "mget", return_value=["legacy-vis"]
        ) as visualizations:
            assert legacy.visualizations(using="u", missing="none") == ["legacy-vis"]
        with patch.object(Search, "mget", return_value=["legacy-search"]) as searches:
            assert legacy.searches(using="u") == ["legacy-search"]
    visualizations.assert_called_once_with(
        docs=["visualization:v9"], index=".kibana", missing="none", using="u"
    )
    searches.assert_called_once_with(
        docs=["search:s9"], index=".kibana", missing="skip", using="u"
    )
