import importlib.util
import os
import subprocess
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, call, patch


DRIVER = Path(__file__).resolve().parents[1] / "test-templates" / "parallel_driver_migrate.py"


def load_driver():
    antithesis = types.ModuleType("antithesis")
    assertions = types.ModuleType("antithesis.assertions")
    assertions.always = Mock()
    assertions.sometimes = Mock()
    random_module = types.ModuleType("antithesis.random")
    random_module.AntithesisRandom = Mock
    modules = {
        "antithesis": antithesis,
        "antithesis.assertions": assertions,
        "antithesis.random": random_module,
        "requests": types.ModuleType("requests"),
        "helper": types.ModuleType("helper"),
    }
    with patch.dict(sys.modules, modules):
        spec = importlib.util.spec_from_file_location("parallel_driver_migrate", DRIVER)
        driver = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(driver)
    return driver


class MigrationDriverTests(unittest.TestCase):
    def setUp(self):
        self.driver = load_driver()
        self.stable = {
            "write_version": 2,
            "use_version": 1,
            "sync_log_version": 1,
            "migration_complete": True,
            "cleanup_complete": True,
        }

    def test_pinned_versions_still_check_integrity_but_never_migrate(self):
        pending = dict(self.stable, use_version=2, migration_complete=False)
        statuses = {"node1:8080": self.stable, "node2:8080": pending}
        for key in (
            "CORRO_METADATA_WRITE_VERSION",
            "CORRO_METADATA_USE_VERSION",
            "CORRO_SYNC_LOG_VERSION",
        ):
            with self.subTest(key=key), \
                 patch.dict(os.environ, {key: "2"}, clear=True), \
                 patch.object(sys, "argv", ["driver", "--addrs", *statuses]), \
                 patch.object(self.driver, "get_migrate_status", side_effect=statuses.get), \
                 patch.object(self.driver, "check_v1_v2_integrity") as check, \
                 patch.object(self.driver, "post_migrate_step") as post:
                self.driver.main()
                check.assert_called_once_with("node1:8080")
                post.assert_not_called()

    def test_integrity_failure_emits_named_assertion(self):
        result = types.SimpleNamespace(
            returncode=1, stdout="tombstone CL mismatches=1", stderr=""
        )
        with patch.object(self.driver.subprocess, "run", return_value=result), \
             patch.object(self.driver, "get_migrate_status", return_value=self.stable), \
             patch.object(self.driver, "always") as always:
            self.assertFalse(self.driver.check_v1_v2_integrity("node1:8080"))
        self.assertEqual(
            always.call_args,
            call(
                False,
                "V1/V2 metadata remains consistent",
                {
                    "address": "node1:8080",
                    "returncode": 1,
                    "output": "tombstone CL mismatches=1",
                },
            ),
        )

    def test_integrity_failure_skips_if_migration_restarts(self):
        result = types.SimpleNamespace(returncode=1, stdout="incomplete", stderr="")
        status = dict(self.stable, use_version=2, migration_complete=False)
        with patch.object(self.driver.subprocess, "run", return_value=result), \
             patch.object(self.driver, "get_migrate_status", return_value=status), \
             patch.object(self.driver, "always") as always:
            self.assertTrue(self.driver.check_v1_v2_integrity("node1:8080"))
        always.assert_not_called()

    def test_integrity_timeout_does_not_report_a_mismatch(self):
        with patch.object(
            self.driver.subprocess, "run",
            side_effect=subprocess.TimeoutExpired("checker", 120),
        ), patch.object(self.driver, "always") as always:
            self.assertTrue(self.driver.check_v1_v2_integrity("node1:8080"))
        always.assert_not_called()

    def test_inconclusive_checker_result_does_not_report_a_mismatch(self):
        result = types.SimpleNamespace(returncode=2, stdout="database busy", stderr="")
        with patch.object(self.driver.subprocess, "run", return_value=result), \
             patch.object(self.driver, "always") as always:
            self.assertTrue(self.driver.check_v1_v2_integrity("node1:8080"))
        always.assert_not_called()

    def test_inapplicable_snapshot_does_not_report_success(self):
        result = types.SimpleNamespace(
            returncode=0,
            stdout="V1/V2 integrity check NOT APPLICABLE (maintenance pending)",
            stderr="",
        )
        with patch.object(self.driver.subprocess, "run", return_value=result), \
             patch.object(self.driver, "always") as always:
            self.assertTrue(self.driver.check_v1_v2_integrity("node1:8080"))
        always.assert_not_called()

    def test_unpinned_driver_can_start_migration(self):
        status = dict(self.stable, write_version=1)
        self.driver.random.choice.return_value = "node1:8080"
        with patch.dict(os.environ, {}, clear=True), \
             patch.object(sys, "argv", ["driver", "--addrs", "node1:8080"]), \
             patch.object(self.driver, "get_migrate_status", return_value=status), \
             patch.object(self.driver, "post_migrate_step", return_value=(True, {})) as post:
            self.driver.main()
        post.assert_called_once_with("node1:8080", self.driver.STEP_V2_DB_FORMAT)


if __name__ == "__main__":
    unittest.main()
