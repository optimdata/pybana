# -*- coding: utf-8 -*-
"""Unit tests for Kibana 8+ saved-object reference resolution."""

import json
import os
import sys

BASE_DIRECTORY = os.path.join(os.path.dirname(__file__), "..")  # NOQA
sys.path.insert(0, BASE_DIRECTORY)  # NOQA

import pytest  # noqa: E402
from elasticsearch_dsl.utils import AttrDict  # noqa: E402

from pybana.kibana_refs import (  # noqa: E402
    first_input_control_index_pattern_ref,
    kibana_saved_object_data_source_dict,
    reference_as_dict,
    resolve_index_pattern_document_id,
)


def test_resolve_legacy_inline_index():
    s = json.dumps({"index": "my-model", "query": {}, "filter": []})
    assert resolve_index_pattern_document_id(s, []) == "index-pattern:my-model"


def test_resolve_kibana8_index_ref_name():
    s = json.dumps(
        {
            "query": {"query": "", "language": "kuery"},
            "filter": [],
            "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
        }
    )
    refs = [
        {
            "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
            "type": "index-pattern",
            "id": "57fdf160-6637-11f0-a189-1b7fee760a48",
        }
    ]
    assert (
        resolve_index_pattern_document_id(s, refs)
        == "index-pattern:57fdf160-6637-11f0-a189-1b7fee760a48"
    )


def test_resolve_kibana8_data_view_ref():
    s = json.dumps(
        {
            "query": {"query": "", "language": "kuery"},
            "filter": [],
            "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
        }
    )
    refs = [
        {
            "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
            "type": "data-view",
            "id": "abc-def-001",
        }
    ]
    assert resolve_index_pattern_document_id(s, refs) == "data-view:abc-def-001"


def test_resolve_single_reference_fallback_without_index_ref_name():
    s = json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []})
    refs = [{"name": "x", "type": "index-pattern", "id": "only-one"}]
    assert resolve_index_pattern_document_id(s, refs) == "index-pattern:only-one"


def test_resolve_returns_none_when_unresolvable():
    s = json.dumps({"query": {}, "filter": []})
    assert resolve_index_pattern_document_id(s, []) is None
    assert resolve_index_pattern_document_id("", None) is None
    assert resolve_index_pattern_document_id(None, None) is None
    assert resolve_index_pattern_document_id("{", []) is None
    assert resolve_index_pattern_document_id(True, []) is None


def test_reference_as_dict_and_unmatched_refs():
    raw = {"type": "index-pattern", "id": "plain"}
    assert reference_as_dict(raw) is raw

    class Ref(object):
        def to_dict(self):
            return {
                "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                "type": "index-pattern",
                "id": "from-obj",
            }

    pairs = [("name", "other"), ("type", "dashboard"), ("id", "nope")]
    assert reference_as_dict(pairs) == {
        "name": "other",
        "type": "dashboard",
        "id": "nope",
    }
    source = json.dumps(
        {"indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"}
    )
    assert (
        resolve_index_pattern_document_id(source, [pairs, Ref()])
        == "index-pattern:from-obj"
    )
    assert (
        resolve_index_pattern_document_id(
            source,
            [
                {"name": "other", "type": "index-pattern", "id": "skipped"},
                {
                    "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                    "type": "visualization",
                    "id": "wrong-type",
                },
                {
                    "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                    "type": "index-pattern",
                },
            ],
        )
        is None
    )
    assert (
        resolve_index_pattern_document_id(
            json.dumps({"index": "", "query": {}}),
            [{"name": "only", "type": "data-view", "id": "dv-1"}],
        )
        == "data-view:dv-1"
    )
    assert (
        resolve_index_pattern_document_id(
            json.dumps({"query": {}}),
            [
                {"type": "index-pattern", "id": "a"},
                {"type": "data-view", "id": "b"},
            ],
        )
        is None
    )


def test_first_input_control_index_pattern_ref():
    vis = {
        "type": "input_control_vis",
        "params": {
            "controls": [
                {"id": "1", "indexPattern": ""},
                {"id": "2", "indexPattern": "test_ewon_optimdata"},
            ]
        },
    }
    assert first_input_control_index_pattern_ref(vis) == "test_ewon_optimdata"


def test_first_input_control_index_pattern_ref_non_input_vis():
    assert first_input_control_index_pattern_ref({"type": "histogram"}) is None
    assert first_input_control_index_pattern_ref(None) is None
    assert first_input_control_index_pattern_ref({}) is None
    assert (
        first_input_control_index_pattern_ref(
            {
                "type": "input_control_vis",
                "params": {
                    "controls": ["skip", {"indexPattern": None}, {"indexPattern": "  "}]
                },
            }
        )
        is None
    )
    assert (
        first_input_control_index_pattern_ref(
            AttrDict(
                {
                    "type": "input_control_vis",
                    "params": {"controls": [{"indexPattern": "  from-attr  "}]},
                }
            )
        )
        == "from-attr"
    )


def test_kibana_saved_object_data_source_dict_index_pattern():
    class _Doc(object):
        def to_dict(self):
            return {
                "index-pattern": {
                    "title": "t",
                    "timeFieldName": "ts_beg",
                    "fields": "[]",
                }
            }

    d = kibana_saved_object_data_source_dict(_Doc())
    assert d["title"] == "t"
    assert d["timeFieldName"] == "ts_beg"


def test_kibana_saved_object_data_source_dict_data_view():
    class _Doc(object):
        def to_dict(self):
            return {
                "data-view": {
                    "title": "dv",
                    "timeFieldName": "@timestamp",
                    "fields": "[]",
                }
            }

    d = kibana_saved_object_data_source_dict(_Doc())
    assert d["title"] == "dv"

    class _Other(object):
        def to_dict(self):
            return {"visualization": {"title": "nope"}}

    with pytest.raises(KeyError):
        kibana_saved_object_data_source_dict(_Other())
