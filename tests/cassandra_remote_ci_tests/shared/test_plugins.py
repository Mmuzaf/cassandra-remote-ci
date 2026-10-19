# SPDX-License-Identifier: Apache-2.0
"""Plugin loading and detection."""

import unittest
from pathlib import Path

from cassandra_remote_ci.shared.plugin import available_plugins, detect_plugin, load_plugin

FIXTURES = Path(__file__).parents[1] / "fixtures"


class PluginTest(unittest.TestCase):
    def test_both_plugins_load(self):
        self.assertEqual({"cassandra", "opensearch"}, set(available_plugins()))

    def test_cassandra_detects_fixture(self):
        plugin = detect_plugin(FIXTURES / "cassandra")
        self.assertEqual("cassandra", plugin.name)
        self.assertEqual("11", plugin.default_jdk(FIXTURES / "cassandra"))

    def test_opensearch_is_stubbed(self):
        plugin = load_plugin("opensearch")
        self.assertFalse(plugin.detect(FIXTURES / "cassandra"))
        with self.assertRaisesRegex(NotImplementedError, "not implemented"):
            plugin.default_jdk(FIXTURES / "cassandra")


if __name__ == "__main__":
    unittest.main()
