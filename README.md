# MongoDB consistency/performance PoC

This PoC runs an experiment matrix against a MongoDB 8.0 replica set with four data-bearing
members and one arbiter. Run settings are defined in
[config/poc-experiment.yml](config/poc-experiment.yml), while tested MongoDB parameter values are
defined separately in [config/poc-parameters.yml](config/poc-parameters.yml).

## Topology

Docker Compose starts five `mongod` processes:

| Service | Host port | Replica set role |
|---|---:|---|
| `mongo1` | 27017 | data-bearing member |
| `mongo2` | 27018 | data-bearing member |
| `mongo3` | 27019 | data-bearing member |
| `mongo4` | 27020 | data-bearing member |
| `mongoarbiter` | 27021 | arbiter |

The tested topology is therefore `4 replica-set data members + 1 arbiter`. The arbiter participates
in elections but does not store benchmark data.

## Tested parameters

The default parameter file currently generates a matrix from:

| Parameter | Values | Applied by | Restart needed? |
|---|---|---|---|
| `w` | `1`, `2`, `majority` | MongoDB driver write concern | no |
| `j` | `false`, `true` | MongoDB driver write concern | no |
| `storage.journal.commitIntervalMs` | `1`, `5`, `10`, `25`, `50`, `100`, `200`, `300` | Docker `mongod` startup configuration (`--journalCommitInterval`) | yes |

The scheduler groups runs by `commitIntervalMs`, so MongoDB is restarted only when the startup
parameter changes. Inside each commit-interval group, configurations and repetitions can be
randomized to reduce order bias.

## Benchmark methodology

For every scheduled run the tool:

1. Starts or reuses the replica set for the selected `commitIntervalMs`.
2. Finds the current primary and connects to it directly from the host JVM.
3. Drops and reloads the benchmark collection.
4. Inserts `dataset.initialDocuments` large base documents.
5. Runs a warm-up phase with the same workload mix, without recording metrics.
6. Runs the measured phase for `experiment.durationSeconds`.
7. Saves one JSON object per line to the configured result file.

The default dataset/workload is:

| Setting | Default |
|---|---:|
| Initial documents | 10,000 |
| Approximate document size | 4 KB |
| WiredTiger cache per data node | 0.25 GB |
| Client threads | 4 |
| Warm-up | 10 s |
| Measured duration | 30 s |
| Repetitions | 3 |
| Reads | 50% |
| Inserts | 30% |
| Updates | 20% |

Reads are random `_id` lookups over the preloaded base-document set. Inserts add new large
documents. Updates modify random preloaded base documents.

The local Docker Desktop setup runs five MongoDB processes. To avoid out-of-memory kills during
the PoC, data-bearing nodes use `--wiredTigerCacheSizeGB` from the `mongo.wiredTigerCacheSizeGB`
setting. If Docker Desktop has more memory assigned, this value and `dataset.initialDocuments` can
be increased for longer final experiments.

## Metrics

The result JSON contains separate metric blocks for:

- `overall`
- `read`
- `insert`
- `update`

Each block contains operation count, successful operations, errors, throughput, mean latency,
standard deviation, min, p50, p90, p95, p99, p99.9, and max latency.

## Run

Make sure Docker is running, then execute:

```bash
./gradlew run --args="--config=config/poc-experiment.yml"
```

Useful overrides:

```bash
./gradlew run --args="--config=config/poc-experiment.yml --duration=60 --warmup=10 --threads=8 --output=results/long-run.jsonl"
```

For a quick smoke test, temporarily reduce `dataset.initialDocuments`, `experiment.repetitions`,
and the number of parameter values in `config/poc-parameters.yml`.
