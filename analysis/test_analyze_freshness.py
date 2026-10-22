import json
import tempfile
import unittest
from pathlib import Path

from analyze_freshness import acceptance_report, analyze_run


class FreshnessAnalyzerTest(unittest.TestCase):
    def test_reconstructs_stale_read_from_completed_write(self):
        events = [
            self.event("op-1", "write", "writer-0", 1, 10, writtenVersion=1),
            self.event("op-2", "read", "reader-0", 20, 30, observedVersion=0),
            self.event("op-3", "read", "reader-0", 31, 40, observedVersion=1),
        ]
        row = self.analyze(events, "secondary", "local", 20)

        self.assertEqual(1, row["staleReads"])
        self.assertEqual(1, row["maxVersionLag"])
        self.assertEqual(0, row["nonMonotonicWrites"])

    def test_acceptance_requires_stale_secondary_and_clean_primary(self):
        secondary = self.analyze([
            self.event("op-1", "write", "writer-0", 1, 10, writtenVersion=1),
            self.event("op-2", "read", "reader-0", 20, 30, observedVersion=0),
        ], "secondary", "local", 20)
        primary = self.analyze([
            self.event("op-1", "write", "writer-0", 1, 10, writtenVersion=1),
            self.event("op-2", "read", "reader-0", 20, 30, observedVersion=1),
        ], "primary", "local", 20)

        report = acceptance_report([secondary, primary])

        self.assertTrue(report["passed"])

    @staticmethod
    def event(operation_id, operation, client_id, started, ended, **version):
        return {
            "operationId": operation_id,
            "operation": operation,
            "clientId": client_id,
            "key": "register-0",
            "startedNs": started,
            "endedNs": ended,
            "result": "success",
            "error": None,
            "runId": "run-1",
            **version,
        }

    def analyze(self, events, preference, concern, latency):
        metadata = {
            "readScenario": {"preference": preference, "concern": concern},
            "network": {"latencyMs": latency, "jitterMs": 0},
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run-history.jsonl"
            path.write_text("\n".join(json.dumps(event) for event in events), encoding="utf-8")
            return analyze_run(path, metadata)


if __name__ == "__main__":
    unittest.main()
