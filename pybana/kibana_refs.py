# -*- coding: utf-8 -*-
"""
Resolve Kibana 8+ saved-object references to index-pattern / data-view documents.

Legacy visualizations embed the data source id in ``searchSourceJSON.index``.
Kibana 7.8+ often uses ``indexRefName`` plus a root-level ``references`` array instead.
"""

import json

__all__ = (
    "reference_as_dict",
    "resolve_index_pattern_document_id",
    "first_input_control_index_pattern_ref",
    "kibana_saved_object_data_source_dict",
)


def reference_as_dict(ref):
    """Normalize a reference entry (dict or elasticsearch-dsl inner object) to a dict."""
    if isinstance(ref, dict):
        return ref
    if hasattr(ref, "to_dict"):
        return ref.to_dict()
    return dict(ref)


def resolve_index_pattern_document_id(search_source_json_str, references):
    """
    Return the Elasticsearch document ``_id`` for the visualization/search data source.

    :param str search_source_json_str: ``kibanaSavedObjectMeta.searchSourceJSON`` value
    :param references: root-level ``references`` list from the same saved object, or None
    :return: e.g. ``index-pattern:6c172f80-...`` or ``data-view:...``, or None if unresolved
    """
    if not search_source_json_str:
        return None
    try:
        data = json.loads(search_source_json_str)
    except (TypeError, ValueError):
        return None

    key = data.get("index")
    if key is not None and key != "":
        return "index-pattern:%s" % key

    refs = list(references) if references else []
    ref_dicts = [reference_as_dict(r) for r in refs]

    index_ref_name = data.get("indexRefName")

    def doc_id_for_ref(r):
        rtype = r.get("type")
        rid = r.get("id")
        if not rid:
            return None
        if rtype == "data-view":
            return "data-view:%s" % rid
        if rtype == "index-pattern":
            return "index-pattern:%s" % rid
        return None

    if index_ref_name:
        for r in ref_dicts:
            if r.get("name") != index_ref_name:
                continue
            doc_id = doc_id_for_ref(r)
            if doc_id:
                return doc_id
        return None

    # No indexRefName: use the sole index-pattern / data-view reference (Lens / some imports)
    candidates = [
        r
        for r in ref_dicts
        if r.get("type") in ("index-pattern", "data-view") and r.get("id")
    ]
    if len(candidates) == 1:
        return doc_id_for_ref(candidates[0])
    return None


def first_input_control_index_pattern_ref(vis_state):
    """
    Return the first non-empty ``indexPattern`` from ``input_control_vis`` visState.

    Kibana stores the data view / index-pattern saved object id there (or, in some
    setups, the index pattern title).
    """
    if not vis_state or vis_state.get("type") != "input_control_vis":
        return None
    params = vis_state.get("params") or {}
    for ctrl in params.get("controls") or []:
        if not isinstance(ctrl, dict):
            continue
        ref = ctrl.get("indexPattern")
        if ref is not None and str(ref).strip() != "":
            return str(ref).strip()
    return None


def kibana_saved_object_data_source_dict(document):
    """
    Return the inner dict for an index-pattern or data-view saved object (title, fields, …).
    Used by translators so both legacy ``index-pattern`` and Kibana 8+ ``data-view`` work.
    """
    data = document.to_dict()
    for key in ("index-pattern", "data-view"):
        if key in data:
            return data[key]
    raise KeyError(
        "Expected index-pattern or data-view in document, got keys: %s"
        % list(data.keys())
    )
