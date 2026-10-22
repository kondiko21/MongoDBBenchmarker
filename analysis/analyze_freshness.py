#!/usr/bin/env python3
"""Offline verifier for the versioned-register consistency experiment.

The verifier reconstructs the reference state from raw operation history. It
does not trust any version-lag value calculated by the workload.
"""

import argparse
import bisect
import json
import sys
from collections import defaultdict
from pathlib import Path


def percentile(values, p):
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, max(0, int(len(ordered) * p + 0.999999) - 1))]


def load_summaries(directory):
    path = directory / "summary.jsonl"
    if not path.exists():
        return {}
    return {
        row["runId"]: row
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
        for row in [json.loads(line)]
    }


def successful(event, operation):
    return event.get("operation") == operation and event.get("result") == "success"


def analyze_run(path, metadata):
    events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    operation_ids = [event.get("operationId") for event in events]
    duplicate_operation_ids = len(operation_ids) - len(set(operation_ids))
    invalid_intervals = sum(
        event.get("startedNs") is None
        or event.get("endedNs") is None
        or event["endedNs"] < event["startedNs"]
        for event in events
    )
    writes_by_key = defaultdict(list)
    attempted_writes_by_key = defaultdict(list)

    for event in events:
        if event.get("operation") == "write" and event.get("writtenVersion") is not None:
            attempted_writes_by_key[event["key"]].append(event)
            if successful(event, "write"):
                writes_by_key[event["key"]].append(event)

    for writes in writes_by_key.values():
        writes.sort(key=lambda event: (event["endedNs"], event["writtenVersion"]))
    write_end_times_by_key = {
        key: [write["endedNs"] for write in writes]
        for key, writes in writes_by_key.items()
    }
    for writes in attempted_writes_by_key.values():
        writes.sort(key=lambda event: (event["startedNs"], event["operationId"]))

    non_monotonic_writes = 0
    for writes in attempted_writes_by_key.values():
        for previous, current in zip(writes, writes[1:]):
            if current["writtenVersion"] <= previous["writtenVersion"]:
                non_monotonic_writes += 1

    successful_reads = [event for event in events if successful(event, "read")]
    stale_examples = []
    version_lags = []
    staleness_ms = []
    future_reads = 0

    for read in successful_reads:
        writes = writes_by_key.get(read["key"], [])
        completion_times = write_end_times_by_key.get(read["key"], [])
        position = bisect.bisect_right(completion_times, read["startedNs"]) - 1
        latest_completed = writes[position] if position >= 0 else None
        expected = latest_completed["writtenVersion"] if latest_completed else 0
        observed = read.get("observedVersion", -1)

        if observed < expected:
            lag = expected - observed
            version_lags.append(lag)
            staleness = (read["endedNs"] - latest_completed["endedNs"]) / 1_000_000.0
            staleness_ms.append(staleness)
            if len(stale_examples) < 5:
                stale_examples.append({
                    "operationId": read["operationId"],
                    "clientId": read["clientId"],
                    "key": read["key"],
                    "expectedVersion": expected,
                    "observedVersion": observed,
                    "versionLag": lag,
                    "stalenessMs": staleness,
                })
        elif observed > expected:
            # A write may finish while this read is in progress. Such an
            # overlapping observation is legal and is not a stale read.
            future_reads += 1

    read_events = [event for event in events if event.get("operation") == "read"]
    write_events = [event for event in events if event.get("operation") == "write"]
    run_id = events[0].get("runId") if events else path.name.removesuffix("-history.jsonl")
    scenario = metadata.get("readScenario") or {}
    network = metadata.get("network") or {}
    return {
        "runId": run_id,
        "file": path.name,
        "readPreference": scenario.get("preference"),
        "readConcern": scenario.get("concern"),
        "networkLatencyMs": network.get("latencyMs"),
        "networkJitterMs": network.get("jitterMs"),
        "duplicateOperationIds": duplicate_operation_ids,
        "invalidTimeIntervals": invalid_intervals,
        "successfulWrites": sum(successful(event, "write") for event in write_events),
        "writeErrors": sum(not successful(event, "write") for event in write_events),
        "successfulReads": len(successful_reads),
        "readErrors": len(read_events) - len(successful_reads),
        "staleReads": len(version_lags),
        "staleReadPercent": 0.0 if not successful_reads else 100.0 * len(version_lags) / len(successful_reads),
        "futureOrOverlappingReads": future_reads,
        "nonMonotonicWrites": non_monotonic_writes,
        "p50VersionLag": percentile(version_lags, 0.50),
        "p95VersionLag": percentile(version_lags, 0.95),
        "p99VersionLag": percentile(version_lags, 0.99),
        "maxVersionLag": max(version_lags, default=0),
        "p50StalenessMs": percentile(staleness_ms, 0.50),
        "p95StalenessMs": percentile(staleness_ms, 0.95),
        "p99StalenessMs": percentile(staleness_ms, 0.99),
        "staleExamples": stale_examples,
    }


def acceptance_report(rows):
    delayed_secondary_local = [
        row for row in rows
        if row["readPreference"] == "secondary"
        and row["readConcern"] == "local"
        and (row["networkLatencyMs"] or 0) > 0
    ]
    primary_local = [
        row for row in rows
        if row["readPreference"] == "primary" and row["readConcern"] == "local"
    ]
    secondary_reads = sum(row["successfulReads"] for row in delayed_secondary_local)
    secondary_stale = sum(row["staleReads"] for row in delayed_secondary_local)
    primary_reads = sum(row["successfulReads"] for row in primary_local)
    primary_stale = sum(row["staleReads"] for row in primary_local)
    monotonicity_violations = sum(row["nonMonotonicWrites"] for row in rows)
    operation_errors = sum(row["readErrors"] + row["writeErrors"] for row in rows)
    duplicate_operation_ids = sum(row["duplicateOperationIds"] for row in rows)
    invalid_intervals = sum(row["invalidTimeIntervals"] for row in rows)

    checks = {
        "secondaryLocalWithDelayWasMeasured": bool(delayed_secondary_local) and secondary_reads > 0,
        "secondaryLocalWithDelayShowsStaleReads": secondary_stale > 0,
        "primaryLocalWasMeasured": bool(primary_local) and primary_reads > 0,
        "primaryLocalShowsNoStaleReads": bool(primary_local) and primary_reads > 0 and primary_stale == 0,
        "writtenVersionsAreStrictlyMonotonic": monotonicity_violations == 0,
        "historyHasNoOperationErrors": operation_errors == 0,
        "operationIdsAreUnique": duplicate_operation_ids == 0,
        "operationTimeIntervalsAreValid": invalid_intervals == 0,
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "evidence": {
            "secondaryLocalDelayedRuns": len(delayed_secondary_local),
            "secondaryLocalDelayedReads": secondary_reads,
            "secondaryLocalDelayedStaleReads": secondary_stale,
            "primaryLocalRuns": len(primary_local),
            "primaryLocalReads": primary_reads,
            "primaryLocalStaleReads": primary_stale,
            "nonMonotonicWrites": monotonicity_violations,
            "operationErrors": operation_errors,
            "duplicateOperationIds": duplicate_operation_ids,
            "invalidTimeIntervals": invalid_intervals,
        },
        "classificationRule": (
            "A read is stale iff it observes a version lower than the newest successful write "
            "to the same key whose endedNs is not later than the read's startedNs."
        ),
    }


def main():
    parser = argparse.ArgumentParser(description="Verify MongoDB versioned-register operation histories")
    parser.add_argument("directory", type=Path)
    parser.add_argument(
        "--report-only", action="store_true",
        help="Print results without returning a failing exit code when acceptance checks fail",
    )
    args = parser.parse_args()

    summaries = load_summaries(args.directory)
    paths = sorted(args.directory.glob("*-history.jsonl"))
    if not paths:
        parser.error(f"no *-history.jsonl files found in {args.directory}")
    rows = []
    for path in paths:
        first_line = next((line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()), None)
        run_id = json.loads(first_line).get("runId") if first_line else None
        rows.append(analyze_run(path, summaries.get(run_id, {})))

    acceptance = acceptance_report(rows)
    print(json.dumps({"acceptance": acceptance, "runs": rows}, indent=2, ensure_ascii=False))
    if not args.report_only and not acceptance["passed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
