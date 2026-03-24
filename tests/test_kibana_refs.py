# -*- coding: utf-8 -*-
"""Unit tests for Kibana 8+ saved-object reference resolution."""

import json
import os
import sys

BASE_DIRECTORY = os.path.join(os.path.dirname(__file__), "..")  # NOQA
sys.path.insert(0, BASE_DIRECTORY)  # NOQA

from pybana.kibana_refs import (  # noqa: E402
    first_input_control_index_pattern_ref,
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


{
    "_index": ".kibana_pollux_2",
    "_type": "doc",
    "_id": "visualization:4a23d096-541b-4638-bb4a-441fd9ed5ef4",
    "_source": {
        "visualization": {
            "title": "TEST IMAGE VIZ 2",
            "visState": '{"title": "TEST IMAGE VIZ 2", "type": "vega", "params": {"spec": "{\\"$schema\\": \\"https://vega.github.io/schema/vega-lite/v5.json\\", \\"data\\": {\\"values\\": [{\\"x\\": 0.5, \\"y\\": 0.5, \\"url\\": \\"http://localhost:8000/api/files/00000000-0000-0000-0000-000000000003/cdn\\"}]}, \\"mark\\": {\\"type\\": \\"image\\", \\"width\\": 100, \\"height\\": 100}, \\"encoding\\": {\\"x\\": {\\"field\\": \\"x\\", \\"type\\": \\"quantitative\\", \\"scale\\": {\\"domain\\": [0, 1]}}, \\"y\\": {\\"field\\": \\"y\\", \\"type\\": \\"quantitative\\", \\"scale\\": {\\"domain\\": [0, 1]}}, \\"url\\": {\\"field\\": \\"url\\"}}}"}, "aggs": []}',
            "uiStateJSON": "{}",
            "description": "",
            "version": 1,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": '{"query":{"query":"","language":"lucene"},"filter":[]}'
            },
        },
        "type": "visualization",
        "updated_at": "2024-07-16T08:50:07.501Z",
    },
}
