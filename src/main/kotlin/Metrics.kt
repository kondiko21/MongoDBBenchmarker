package org.example

import org.bson.Document
import kotlin.math.ceil
import kotlin.math.pow
import kotlin.math.sqrt

data class Metrics(
    val operations: Long,
    val errors: Long,
    val throughput: Double,
    val meanMs: Double,
    val stdDevMs: Double,
    val minMs: Double,
    val p50Ms: Double,
    val p90Ms: Double,
    val p95Ms: Double,
    val p99Ms: Double,
    val maxMs: Double
) {
    fun document() = Document("operations", operations).append("errors", errors)
        .append("throughputOpsPerSecond", throughput).append("meanLatencyMs", meanMs)
        .append("stdDevLatencyMs", stdDevMs).append("minLatencyMs", minMs)
        .append("p50LatencyMs", p50Ms).append("p90LatencyMs", p90Ms)
        .append("p95LatencyMs", p95Ms).append("p99LatencyMs", p99Ms).append("maxLatencyMs", maxMs)
}

fun metrics(latencies: List<Double>, errors: Long, durationSeconds: Long): Metrics {
    val sorted = latencies.sorted()
    fun percentile(p: Double): Double = if (sorted.isEmpty()) 0.0 else sorted[(ceil(p * sorted.size).toInt() - 1).coerceIn(0, sorted.lastIndex)]
    val mean = if (sorted.isEmpty()) 0.0 else sorted.average()
    val stdDev = if (sorted.isEmpty()) 0.0 else sqrt(sorted.sumOf { (it - mean).pow(2) } / sorted.size)
    return Metrics(
        operations = sorted.size.toLong(), errors = errors,
        throughput = sorted.size / durationSeconds.toDouble(), meanMs = mean, stdDevMs = stdDev,
        minMs = sorted.firstOrNull() ?: 0.0, p50Ms = percentile(.50), p90Ms = percentile(.90),
        p95Ms = percentile(.95), p99Ms = percentile(.99), maxMs = sorted.lastOrNull() ?: 0.0
    )
}
