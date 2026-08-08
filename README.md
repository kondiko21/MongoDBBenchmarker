# MongoDB consistency/performance PoC

This PoC runs an eight-configuration experiment matrix against a three-node MongoDB 8.0 replica set:

| Parameter | Values | Applied by |
|---|---|---|
| `w` | `1`, `majority` | MongoDB driver write concern |
| `j` | `false`, `true` | MongoDB driver write concern |
| `storage.journal.commitIntervalMs` | `10`, `100` | Docker `mongod` startup configuration |

The first two parameters are changed per benchmark without restarting MongoDB. A change to
`commitIntervalMs` causes Docker Compose to recreate the MongoDB containers. Results are appended
as one JSON object per line, making the file easy to process with Python, R, or a spreadsheet.

## Run

Make sure Docker is running, then execute:

```bash
./gradlew run --args="--duration=20 --warmup=5 --output=results/poc-results.jsonl"
```

The benchmark uses a 50/50 mixed read/write workload with four client threads and reports
throughput, mean, p50, p95, p99, maximum latency, successful operations, and errors.

This is deliberately a measurement harness, not yet the final thesis experiment design. Before
large runs, repeat each configuration several times, randomize configuration order, record host
and Docker resource limits, and add a no-op baseline for warm-up and drift detection.
