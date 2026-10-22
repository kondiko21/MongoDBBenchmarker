package org.example

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import java.io.File

class ExperimentTest {
    @Test
    fun `metrics calculate percentiles and throughput`() {
        val result = metrics((1..100).map(Int::toDouble), errors = 2, durationSeconds = 10)
        assertEquals(100, result.operations)
        assertEquals(10.0, result.throughput)
        assertEquals(50.0, result.p50Ms)
        assertEquals(99.0, result.p99Ms)
        assertEquals(2, result.errors)
    }

    @Test
    fun `smoke configuration creates performance and freshness runs`() {
        val root = File(System.getProperty("user.dir"))
        val (experiment, matrix) = ConfigLoader(root).load(File(root, "config/smoke-experiment.yml"))
        val runs = buildRuns(experiment, matrix)
        assertEquals(12, runs.size)
        assertEquals(setOf(Suite.PERFORMANCE, Suite.FRESHNESS), runs.map { it.suite }.toSet())
        assertTrue(runs.none { it.readScenario?.concern == "linearizable" && it.readScenario.preference != "primary" })
    }

    @Test
    fun `run spec JSON round trip preserves scientific parameters`() {
        val spec = RunSpec(
            "id", Suite.FRESHNESS, 5, NetworkProfile("medium", 20, 10), "majority",
            ReadScenario("secondary-majority", "secondary", "majority"), null, 16, 3, 30, 10, 42,
            DatasetSettings(1000, 4096, 100, 10), WorkloadSettings(0.5, 0.25, 0.25), "results/test", 40.2
        )
        assertEquals(spec, runSpecFromJson(spec.toDocument().toJson()))
    }

    @Test
    fun `supplementary configuration extends the complete main matrix`() {
        val root = File(System.getProperty("user.dir"))
        val (experiment, matrix) = ConfigLoader(root).load(File(root, "config/supplementary-experiment.yml"))
        val runs = buildRuns(experiment, matrix)
        assertEquals(10_368, runs.size)
        assertEquals(setOf(Suite.SUPPLEMENTARY), runs.map { it.suite }.toSet())
        assertEquals(setOf(3, 5), runs.map { it.dataNodes }.toSet())
        assertEquals(setOf("local", "low", "medium", "high"), runs.map { it.network.name }.toSet())
        assertEquals(setOf("1", "majority"), runs.mapNotNull { it.writeConcern }.toSet())
        assertEquals(setOf(1, 4, 16), runs.map { it.concurrency }.toSet())
        assertEquals(24, runs.mapNotNull { it.supplementaryVariant?.name }.toSet().size)
        assertTrue(runs.none { it.readScenario?.preference == "secondary" && it.readScenario.concern == "linearizable" })
        assertTrue(runs.filter { it.readScenario?.preference == "primary" }
            .all { it.supplementaryVariant?.maxStalenessSeconds == null })
        assertTrue(runs.all {
            val variant = requireNotNull(it.supplementaryVariant)
            if (variant.enableFlowControl) variant.flowControlTargetLagSeconds != null
            else variant.flowControlTargetLagSeconds == null
        })
    }

    @Test
    fun `offline history rule detects a stale completed read`() {
        val history = listOf(
            HistoryEvent(1, "write", "writer-0", "register-0", 1_000_000, 10_000_000, 1, "success"),
            HistoryEvent(2, "read", "reader-0", "register-0", 20_000_000, 30_000_000, 0, "success"),
            HistoryEvent(3, "read", "reader-0", "register-0", 31_000_000, 40_000_000, 1, "success")
        )

        val result = analyzeHistory(history)

        assertEquals(2, result.successfulReads)
        assertEquals(1, result.staleReads)
        assertEquals(listOf(1L), result.versionLags)
        assertEquals(listOf(20.0), result.stalenessMs)
    }

    @Test
    fun `write overlapping a read is not falsely classified as stale`() {
        val history = listOf(
            HistoryEvent(1, "write", "writer-0", "register-0", 1, 10, 1, "success"),
            HistoryEvent(2, "write", "writer-0", "register-0", 20, 40, 2, "success"),
            HistoryEvent(3, "read", "reader-0", "register-0", 30, 50, 2, "success")
        )

        val result = analyzeHistory(history)

        assertEquals(0, result.staleReads)
        assertEquals(1, result.futureReads)
    }

    @Test
    fun `history document contains fields required for audit`() {
        val write = HistoryEvent(7, "write", "writer-0", "register-2", 100, 120, 4, "success").document()
        val read = HistoryEvent(8, "read", "reader-3", "register-2", 130, 140, 4, "success").document()

        assertEquals("op-7", write["operationId"])
        assertEquals("writer-0", write["clientId"])
        assertEquals(4L, write["writtenVersion"])
        assertEquals(4L, read["observedVersion"])
        assertTrue(write.containsKey("startedNs") && write.containsKey("endedNs"))
        assertTrue(read.containsKey("result") && read.containsKey("error"))
    }
}
