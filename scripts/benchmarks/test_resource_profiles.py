"""Executable contracts for resource profile identity and accounting."""
import copy
import json
from pathlib import Path
import subprocess
import unittest

ROOT = Path(__file__).resolve().parents[2]
FILTER = ROOT / 'crates/resource-bench/validate-probe-result.jq'


def matched(client='redis-tower-mux'):
    report = json.loads((ROOT / 'crates/resource-bench/tests/fixtures/probe-valid.json').read_text())
    report['schema_version'] = 4
    report['client'] = client
    report['config'].update(profile='matched-mux-resp2-v1', runtime_workers=2)
    report['client_features'] = {
        'harness_feature': 'client-redis-rs' if client == 'redis-rs' else 'client-redis-tower',
        'dependency_default_features': False,
        'dependency_features': ['tokio-comp'] if client == 'redis-rs' else []}
    report['policy'] = {
        'client_path': 'redis-rs-multiplexed' if client == 'redis-rs' else 'redis-tower-multiplexed',
        'protocol_selection': 'resp2', 'socket_policy': 'matched-v1',
        'tcp_nodelay': True, 'keepalive': {'idle_secs': 60, 'interval_secs': 10, 'probes': 3},
        'physical_connections': report['config']['connections'], 'inflight_per_socket': 1,
        'runtime_workers': 2, 'measurement_model': 'completion-gated-staggered-get'}
    return report


class ResourceProfiles(unittest.TestCase):
    def accepts(self, report):
        return subprocess.run(['jq', '-e', '-f', str(FILTER)], input=json.dumps(report),
                              text=True, capture_output=True).returncode == 0

    def test_matched_subjects(self):
        for client in ('redis-tower-mux', 'redis-rs'):
            with self.subTest(client=client):
                self.assertTrue(self.accepts(matched(client)))

    def test_rejects_policy_drift(self):
        original = matched()
        for section, field, value in [
            ('policy', 'tcp_nodelay', False), ('policy', 'protocol_selection', 'resp3'),
            ('policy', 'client_path', 'redis-tower-direct'), ('policy', 'runtime_workers', 3),
            ('policy', 'physical_connections', 7), ('policy', 'inflight_per_socket', 2),
            ('policy', 'socket_policy', 'redis-rs-default'), ('config', 'profile', 'unknown'),
            ('config', 'runtime_workers', 0), ('client_features', 'harness_feature', 'client-fred'),
            ('client_features', 'dependency_features', ['unexpected'])]:
            report = copy.deepcopy(original)
            report[section][field] = value
            with self.subTest(field=field):
                self.assertFalse(self.accepts(report))

    def test_rejects_missing_policy_fields(self):
        for field in matched()['policy']:
            report = matched()
            del report['policy'][field]
            with self.subTest(field=field):
                self.assertFalse(self.accepts(report))

    def test_rejects_wrong_subject_and_failures(self):
        for field, value in [('client', 'redis-tower'), ('schema_version', 3)]:
            report = matched()
            report[field] = value
            self.assertFalse(self.accepts(report))
        for field in ('errors', 'cutoff_ops'):
            report = matched()
            report['cpu'][field] = 1
            self.assertFalse(self.accepts(report))
        report = matched()
        report['policy']['keepalive']['idle_secs'] = 1
        self.assertFalse(self.accepts(report))


if __name__ == '__main__':
    unittest.main()
