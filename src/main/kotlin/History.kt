package org.example

import org.bson.Document

data class HistoryEvent(
    val operationId: Long,
    val operation: String,
    val clientId: String,
    val key: String,
    val startedNs: Long,
    val endedNs: Long,
    val version: Long?,
    val result: String,
    val error: String? = null
) {
    fun document(): Document = Document("operationId", "op-$operationId")
        .append("operation", operation).append("clientId", clientId).append("key", key)
        .append("startedNs", startedNs).append("endedNs", endedNs)
        .append(if (operation == "write") "writtenVersion" else "observedVersion", version)
        .append("result", result).append("error", error)
}

data class FreshnessAnalysis(
    val successfulReads: Long,
    val readErrors: Long,
    val successfulWrites: Long,
    val writeErrors: Long,
    val staleReads: Long,
    val futureReads: Long,
    val versionLags: List<Long>,
    val stalenessMs: List<Double>
)

fun analyzeHistory(events: Collection<HistoryEvent>): FreshnessAnalysis {
    val writesByKey = events.asSequence()
        .filter { it.operation == "write" && it.result == "success" && it.version != null }
        .groupBy { it.key }
        .mapValues { (_, writes) -> writes.sortedWith(compareBy<HistoryEvent> { it.endedNs }.thenBy { it.version }) }
    val reads = events.filter { it.operation == "read" }
    var stale = 0L
    var future = 0L
    val versionLags = mutableListOf<Long>()
    val staleness = mutableListOf<Double>()
    reads.filter { it.result == "success" && it.version != null }.forEach { read ->
        val observedVersion = requireNotNull(read.version)
        val writes = writesByKey[read.key].orEmpty()
        var low = 0
        var high = writes.size
        while (low < high) {
            val middle = (low + high) ushr 1
            if (writes[middle].endedNs <= read.startedNs) low = middle + 1 else high = middle
        }
        val latestCompleted = writes.getOrNull(low - 1)
        val expectedVersion = latestCompleted?.version ?: 0L
        when {
            observedVersion < expectedVersion -> {
                stale++
                versionLags += expectedVersion - observedVersion
                staleness += (read.endedNs - requireNotNull(latestCompleted).endedNs) / 1_000_000.0
            }
            observedVersion > expectedVersion -> future++ // The read overlapped a write; this is not a stale read.
        }
    }
    return FreshnessAnalysis(
        successfulReads = reads.count { it.result == "success" }.toLong(),
        readErrors = reads.count { it.result != "success" }.toLong(),
        successfulWrites = events.count { it.operation == "write" && it.result == "success" }.toLong(),
        writeErrors = events.count { it.operation == "write" && it.result != "success" }.toLong(),
        staleReads = stale,
        futureReads = future,
        versionLags = versionLags,
        stalenessMs = staleness
    )
}
