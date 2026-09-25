#!/usr/bin/env python3

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
MODULE_GATE = ROOT / ".github" / "workflows" / "module-gate.yml"
TRANSPORT_GATE = ROOT / ".github" / "workflows" / "transport-gate.yml"
NIGHTLY_MODULES = ROOT / ".github" / "workflows" / "nightly-modules.yml"
README = ROOT / "README.md"
REDIS_8X_TEST = ROOT / "crates" / "redis-tower" / "tests" / "redis_8x_commands.rs"
TLS_TEST = ROOT / "crates" / "redis-tower" / "tests" / "test_infrastructure.rs"


def coverage_job() -> str:
    workflow = WORKFLOW.read_text()
    start = workflow.index("  coverage:\n")
    end = workflow.index("\n  features:\n", start)
    return workflow[start:end]


class CiWorkflowTests(unittest.TestCase):
    def test_coverage_job_does_not_depend_on_unconfigured_codecov(self) -> None:
        job = coverage_job()
        self.assertIn("    permissions:\n      contents: read\n", job)
        self.assertNotIn("codecov", job.lower())
        self.assertNotIn("CODECOV_TOKEN", job)

    def test_readme_does_not_advertise_unconfigured_codecov(self) -> None:
        self.assertNotIn("codecov", README.read_text().lower())

    def test_native_summary_and_artifact_are_retained(self) -> None:
        job = coverage_job()
        self.assertIn("cargo llvm-cov report --summary-only", job)
        self.assertIn("$GITHUB_STEP_SUMMARY", job)
        self.assertIn("uses: actions/upload-artifact@v4", job)
        self.assertIn("coverage-summary.txt", job)
        self.assertIn("lcov.info", job)
        self.assertIn("if-no-files-found: error", job)
        self.assertIn("retention-days: 30", job)

    def test_module_gate_is_path_bounded_and_fail_closed(self) -> None:
        workflow = MODULE_GATE.read_text()
        for path in (
            "crates/redis-tower-modules/**",
            "crates/redis-tower-commands/**",
            "crates/redis-tower-protocol/**",
            "crates/redis-tower-core/**",
        ):
            self.assertIn(path, workflow)
        self.assertIn("image: redis:8", workflow)
        self.assertIn('REDIS_8X_REQUIRED: "1"', workflow)
        self.assertIn("--test capabilities", workflow)
        self.assertIn("version_parser_accepts_release_and_prerelease_forms", workflow)
        self.assertIn("required_server_version_and_commands_are_present", workflow)
        self.assertIn("--test redis_8x_commands", workflow)
        self.assertIn("redis-tower-modules --all-features --tests", workflow)
        self.assertIn("--ignored --test-threads=1 --nocapture", workflow)

    def test_nightly_modules_preflight_capabilities_before_behavior(self) -> None:
        workflow = NIGHTLY_MODULES.read_text()
        preflight = workflow.index("Prove server version and required commands")
        behavior = workflow.index("Run module integration tests")
        self.assertLess(preflight, behavior)
        self.assertIn("REDIS_MODULE_MIN_VERSION", workflow)
        self.assertIn("REDIS_MODULE_COMMANDS", workflow)

    def test_tls_backends_have_isolated_verified_live_legs(self) -> None:
        workflow = TRANSPORT_GATE.read_text()
        self.assertIn("--features tls-rustls", workflow)
        self.assertIn("--features tls-native-tls", workflow)
        self.assertIn('REDIS_TEST_REQUIRE_TLS: "1"', workflow)
        self.assertIn("--no-default-features", workflow)
        self.assertIn("--test test_infrastructure", workflow)
        self.assertIn("tls_connect_and_roundtrip", workflow)

        tls_test = TLS_TEST.read_text()
        self.assertIn("with_root_ca_pem", tls_test)
        self.assertIn("rejects_wrong_hostname_and_untrusted_ca", tls_test)
        self.assertIn("TLS_PASSWORD_ENCODED", tls_test)
        self.assertNotIn("danger_accept_invalid", tls_test)

    def test_redis_8_and_universal_topology_assertions_are_activated(self) -> None:
        workflow = TRANSPORT_GATE.read_text()
        self.assertIn('REDIS_TEST_REQUIRE_IPV6: "1"', workflow)
        self.assertIn(
            "Run UniversalClient Cluster and Sentinel public-entry assertions",
            workflow,
        )
        self.assertIn("--test topologies --all-features", workflow)

        redis_8x = REDIS_8X_TEST.read_text()
        self.assertIn('Info::new().section("server")', redis_8x)
        self.assertIn("INFO redis_version", redis_8x)
        self.assertIn("REDIS_8X_REQUIRED", redis_8x)


if __name__ == "__main__":
    unittest.main()
