# -*- coding: utf-8 -*-
"""Unit tests for Kibana 8+ saved-object reference resolution."""

import json
import os
import sys

BASE_DIRECTORY = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, BASE_DIRECTORY)

from pybana.kibana_refs import (
    kibana_saved_object_data_source_dict,
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
