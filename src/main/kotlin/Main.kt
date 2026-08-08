package org.example

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.WriteConcern
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import org.bson.Document
import java.io.File
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.ThreadLocalRandom
import kotlin.math.ceil
import kotlin.system.exitProcess

/** Small, reproducible PoC for the thesis experiment runner. */
data class PocConfig(
    val w: String,
    val journal: Boolean,
    val commitIntervalMs: Int
) {
    val id: String get() = "w-$w-j-$journal-ci-$commitIntervalMs"
}

data class BenchmarkSettings(
    val durationSeconds: Long = 20,
    val warmupSeconds: Long = 5,
    val threads: Int = 4,
    val writeRatio: Double = 0.5
)

data class BenchmarkResult(
    val config: PocConfig,
    val startedAt: String,
    val durationSeconds: Long,
    val operations: Int,
    val successfulOperations: Int,
    val errors: Int,
    val throughputOpsPerSecond: Double,
    val meanLatencyMs: Double,
    val p50LatencyMs: Double,
    val p95LatencyMs: Double,
    val p99LatencyMs: Double,
    val maxLatencyMs: Double
) {
    fun toJson(): String = """
      {"config":{"w":"${config.w}","j":${config.journal},"commitIntervalMs":${config.commitIntervalMs}},
       "startedAt":"$startedAt","durationSeconds":$durationSeconds,"operations":$operations,
       "successfulOperations":$successfulOperations,"errors":$errors,
       "throughputOpsPerSecond":$throughputOpsPerSecond,"meanLatencyMs":$meanLatencyMs,
       "p50LatencyMs":$p50LatencyMs,"p95LatencyMs":$p95LatencyMs,"p99LatencyMs":$p99LatencyMs,
       "maxLatencyMs":$maxLatencyMs}
    """.trimIndent().replace("\n", "")
}

class CommandRunner(private val workDir: File) {
    fun run(vararg command: String, quiet: Boolean = false): String {
        val process = ProcessBuilder(*command).directory(workDir).redirectErrorStream(true).start()
        val output = process.inputStream.bufferedReader().readText()
        check(process.waitFor() == 0) { "Command failed: ${command.joinToString(" ")}\n$output" }
        if (!quiet && output.isNotBlank()) println(output.trim())
        return output
    }
}

class MongoDockerController(private val root: File) {
    private val runner = CommandRunner(root)
    private var runningConfig: Int? = null

    fun start(config: PocConfig) {
        if (runningConfig == config.commitIntervalMs) return
        File(root, ".env").writeText("MONGO_COMMIT_INTERVAL_MS=${config.commitIntervalMs}\n")
        runner.run("docker", "compose", "up", "-d", quiet = true)
        initiateReplicaSet()
        waitForPrimary()
        runningConfig = config.commitIntervalMs
    }

    private fun initiateReplicaSet() {
        runner.run(
            "docker", "exec", "mongo1", "mongosh", "--quiet", "--eval",
            "try { rs.status() } catch (e) { rs.initiate({_id:'rs0',members:[{_id:0,host:'host.docker.internal:27017'},{_id:1,host:'host.docker.internal:27018'},{_id:2,host:'host.docker.internal:27019'}]}) }",
            quiet = true
        )
    }

    private fun waitForPrimary() {
        repeat(30) {
            try {
                if (runner.run("docker", "exec", "mongo1", "mongosh", "--quiet", "--eval", "rs.status().myState", quiet = true).trim() == "1") return
            } catch (_: Exception) {
            }
            Thread.sleep(1_000)
        }
        error("Replica set did not become ready within 30 seconds")
    }
}

class Benchmark(private val uri: String, private val settings: BenchmarkSettings) {
    fun run(config: PocConfig): BenchmarkResult {
        val clientSettings = MongoClientSettings.builder()
            .applyConnectionString(ConnectionString(uri))
            .retryWrites(false)
            .build()
        MongoClients.create(clientSettings).use { client ->
            val collection = client.getDatabase("benchmark").getCollection("operations")
                .withWriteConcern(writeConcern(config))
            collection.drop()
            collection.insertOne(Document("_id", "seed").append("value", "seed"))
            repeat(settings.threads) { thread ->
                Thread {
                    warmup(collection, thread)
                }.apply { start(); join() }
            }

            val latencies = CopyOnWriteArrayList<Double>()
            var successes = 0
            var errors = 0
            val endAt = System.nanoTime() + settings.durationSeconds * 1_000_000_000L
            val workers = (0 until settings.threads).map { thread ->
                Thread {
                    while (System.nanoTime() < endAt) {
                        val started = System.nanoTime()
                        try {
                            if (ThreadLocalRandom.current().nextDouble() < settings.writeRatio) {
                                collection.insertOne(Document("_id", "${thread}-${System.nanoTime()}"))
                            } else {
                                collection.find(Document("_id", "seed")).first()
                            }
                            latencies += (System.nanoTime() - started) / 1_000_000.0
                            synchronized(this) { successes++ }
                        } catch (_: Exception) {
                            synchronized(this) { errors++ }
                        }
                    }
                }
            }
            workers.forEach(Thread::start)
            workers.forEach(Thread::join)
            val sorted = latencies.sorted()
            val total = successes + errors
            fun percentile(p: Double): Double = if (sorted.isEmpty()) 0.0 else sorted[minOf(sorted.lastIndex, ceil(p * sorted.size).toInt() - 1)]
            return BenchmarkResult(
                config, Instant.now().toString(), settings.durationSeconds, total, successes, errors,
                successes / settings.durationSeconds.toDouble(), sorted.averageOrZero(), percentile(.50),
                percentile(.95), percentile(.99), sorted.lastOrZero()
            )
        }
    }

    private fun warmup(collection: MongoCollection<Document>, thread: Int) {
        val until = System.nanoTime() + settings.warmupSeconds * 1_000_000_000L
        while (System.nanoTime() < until) {
            try { collection.find(Document("_id", "seed")).first() } catch (_: Exception) { /* warmup failures are not measured */ }
        }
    }

    private fun writeConcern(config: PocConfig): WriteConcern = when (config.w) {
        "majority" -> WriteConcern.MAJORITY.withJournal(config.journal)
        else -> WriteConcern(config.w.toInt()).withJournal(config.journal)
    }
}

private fun List<Double>.averageOrZero() = if (isEmpty()) 0.0 else average()
private fun List<Double>.lastOrZero() = lastOrNull() ?: 0.0

fun main(args: Array<String>) {
    val root = File(System.getProperty("user.dir"))
    val output = File(args.firstOrNull { it.startsWith("--output=") }?.substringAfter('=') ?: "results/poc-results.jsonl")
    val settings = BenchmarkSettings(
        durationSeconds = args.firstOrNull { it.startsWith("--duration=") }?.substringAfter('=')?.toLong() ?: 20,
        warmupSeconds = args.firstOrNull { it.startsWith("--warmup=") }?.substringAfter('=')?.toLong() ?: 5
    )
    val matrix = listOf("1", "majority").flatMap { w ->
        listOf(false, true).flatMap { j -> listOf(10, 100).map { PocConfig(w, j, it) } }
    }
    val docker = MongoDockerController(root)
    val benchmark = Benchmark("mongodb://host.docker.internal:27017,host.docker.internal:27018,host.docker.internal:27019/?replicaSet=rs0", settings)
    output.parentFile?.mkdirs()
    output.writeText("")
    matrix.forEachIndexed { index, config ->
        println("[${index + 1}/${matrix.size}] ${config.id}")
        docker.start(config)
        output.appendText(benchmark.run(config).toJson() + "\n")
    }
    println("Results written to ${output.absolutePath}")
}
