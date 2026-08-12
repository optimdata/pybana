# -*- coding: utf-8 -*-

import os
import sys
from unittest import TestCase, mock

BASE_DIRECTORY = os.path.join(os.path.dirname(__file__), "..")  # NOQA
sys.path.insert(0, BASE_DIRECTORY)  # NOQA

from pybana.client import Kibana, VISUALIZATION_MAPPING_PROPERTIES  # noqa: E402


INCOMPLETE_ANALYTICS_MAPPING = {
    ".kibana_wendt_analytics_1": {
        "mappings": {
            "properties": {
                "type": {"type": "keyword"},
                "visualization": {
                    "dynamic": "false",
                    "properties": {
                        "description": {"type": "text"},
                        "kibanaSavedObjectMeta": {"type": "object"},
                        "title": {"type": "text"},
                        "version": {"type": "integer"},
                    },
                },
            }
        }
    }
}

COMPLETE_MAPPING = {
    ".kibana_wendt_analytics_1": {
        "mappings": {
            "properties": {
                "type": {"type": "keyword"},
                "visualization": {
                    "properties": {
                        "title": {"type": "text"},
                        "visState": {"type": "text"},
                        "uiStateJSON": {"type": "text"},
                    }
                },
            }
        }
    }
}


class TestEnsureVisualizationMapping(TestCase):
    def _kibana_with_es(self, exists=True, mapping=None):
        elastic = mock.MagicMock()
        elastic.version_major = 8
        elastic.indices.exists.return_value = exists
        elastic.indices.get_mapping.return_value = mapping or {}
        # Bypass Kibana.__init__ / get_es (would require a real ES client).
        kibana = Kibana.__new__(Kibana)
        kibana._default = elastic
        kibana._index = ".kibana_wendt_analytics"
        return kibana, elastic

    def test_needs_update_when_vis_state_missing(self):
        kibana, _ = self._kibana_with_es(mapping=INCOMPLETE_ANALYTICS_MAPPING)
        self.assertTrue(kibana.visualization_mapping_needs_update())

    def test_needs_update_false_when_vis_state_present(self):
        kibana, _ = self._kibana_with_es(mapping=COMPLETE_MAPPING)
        self.assertFalse(kibana.visualization_mapping_needs_update())

    def test_needs_update_false_when_index_missing(self):
        kibana, _ = self._kibana_with_es(exists=False)
        self.assertFalse(kibana.visualization_mapping_needs_update())

    def test_ensure_puts_mapping_and_reindexes(self):
        kibana, elastic = self._kibana_with_es(mapping=INCOMPLETE_ANALYTICS_MAPPING)
        updated = kibana.ensure_visualization_mapping(reindex=True)
        self.assertTrue(updated)
        elastic.indices.put_mapping.assert_called_once_with(
            "doc",
            {
                "properties": {
                    "visualization": {"properties": VISUALIZATION_MAPPING_PROPERTIES}
                }
            },
            index=".kibana_wendt_analytics",
        )
        elastic.update_by_query.assert_called_once_with(
            index=".kibana_wendt_analytics",
            body={"query": {"term": {"type": "visualization"}}},
            conflicts="proceed",
            refresh=True,
            wait_for_completion=True,
        )

    def test_ensure_skips_when_already_mapped(self):
        kibana, elastic = self._kibana_with_es(mapping=COMPLETE_MAPPING)
        updated = kibana.ensure_visualization_mapping(reindex=True)
        self.assertFalse(updated)
        elastic.indices.put_mapping.assert_not_called()
        elastic.update_by_query.assert_not_called()

    def test_ensure_can_skip_reindex(self):
        kibana, elastic = self._kibana_with_es(mapping=INCOMPLETE_ANALYTICS_MAPPING)
        kibana.ensure_visualization_mapping(reindex=False)
        elastic.indices.put_mapping.assert_called_once()
        elastic.update_by_query.assert_not_called()

    def test_init_index_ensures_mapping_on_existing_index(self):
        kibana, elastic = self._kibana_with_es(mapping=INCOMPLETE_ANALYTICS_MAPPING)
        # Index alias already exists → create path skipped, ensure still runs.
        elastic.indices.exists.return_value = True
        kibana.init_index()
        elastic.indices.create.assert_not_called()
        elastic.indices.put_mapping.assert_called_once()
        elastic.update_by_query.assert_called_once()
