package org.example

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.WriteConcern
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import org.bson.Document
import org.yaml.snakeyaml.Yaml
import java.io.File
import java.time.Instant
import java.util.EnumMap
import java.util.concurrent.ThreadLocalRandom
import kotlin.math.ceil
import kotlin.math.pow
import kotlin.math.sqrt
import kotlin.random.Random

/** Small, reproducible PoC for the thesis experiment runner. */
data class PocConfig(
    val w: String,
    val journal: Boolean,
    val commitIntervalMs: Int
) {
    val id: String get() = "w-$w-j-$journal-ci-$commitIntervalMs"
}

data class DatasetSettings(
    val initialDocuments: Int = 50_000,
    val documentSizeBytes: Int = 4_096,
    val batchSize: Int = 1_000
)

data class WorkloadSettings(
    val readRatio: Double = 0.5,
    val insertRatio: Double = 0.3,
    val updateRatio: Double = 0.2,
    val readMode: String = "randomById"
)

data class MongoRuntimeSettings(
    val wiredTigerCacheSizeGB: Double = 0.25
)

data class BenchmarkSettings(
    val durationSeconds: Long = 30,
    val warmupSeconds: Long = 10,
    val threads: Int = 4,
    val dataset: DatasetSettings = DatasetSettings(),
    val workload: WorkloadSettings = WorkloadSettings()
)

data class ExperimentConfig(
    val parametersFile: String,
    val durationSeconds: Long,
    val warmupSeconds: Long,
    val threads: Int,
    val repetitions: Int,
    val randomizeOrder: Boolean,
    val randomSeed: Int,
    val output: String,
    val dataset: DatasetSettings,
    val workload: WorkloadSettings,
    val mongo: MongoRuntimeSettings
)

data class ScheduledRun(
    val config: PocConfig,
    val repetition: Int
)

class YamlConfigLoader(private val root: File) {
    private val yaml = Yaml()

    @Suppress("UNCHECKED_CAST")
    fun loadExperiment(path: File): Pair<ExperimentConfig, List<PocConfig>> {
        val experiment = yaml.load<Map<String, Any>>(path.readText())
        val run = experiment.map("experiment")
        val datasetMap = experiment.optionalMap("dataset")
        val workloadMap = experiment.optionalMap("workload")
        val mongoMap = experiment.optionalMap("mongo")
        val legacyWriteRatio = run.optionalDouble("writeRatio")
        val dataset = DatasetSettings(
            initialDocuments = datasetMap?.optionalInt("initialDocuments") ?: 50_000,
            documentSizeBytes = datasetMap?.optionalInt("documentSizeBytes") ?: 4_096,
            batchSize = datasetMap?.optionalInt("batchSize") ?: 1_000
        )
        val workload = WorkloadSettings(
            readRatio = workloadMap?.optionalDouble("readRatio") ?: legacyWriteRatio?.let { 1.0 - it } ?: 0.5,
            insertRatio = workloadMap?.optionalDouble("insertRatio") ?: legacyWriteRatio?.let { it } ?: 0.3,
            updateRatio = workloadMap?.optionalDouble("updateRatio") ?: 0.2,
            readMode = workloadMap?.optionalString("readMode") ?: "randomById"
        )
        val mongo = MongoRuntimeSettings(
            wiredTigerCacheSizeGB = mongoMap?.optionalDouble("wiredTigerCacheSizeGB") ?: 0.25
        )
        validate(dataset, workload, mongo)
        val config = ExperimentConfig(
            parametersFile = experiment.string("parametersFile"),
            durationSeconds = run.long("durationSeconds"),
            warmupSeconds = run.long("warmupSeconds"),
            threads = run.int("threads"),
            repetitions = run.optionalInt("repetitions") ?: 3,
            randomizeOrder = run.optionalBoolean("randomizeOrder") ?: true,
            randomSeed = run.optionalInt("randomSeed") ?: 42,
            output = run.string("output"),
            dataset = dataset,
            workload = workload,
            mongo = mongo
        )
        val parameterPath = File(root, config.parametersFile)
        val parameterDocument = yaml.load<Map<String, Any>>(parameterPath.readText())
        val parameters = parameterDocument.map("parameters")
        val ws = parameters.map("w").list("values").map(Any::toString)
        val js = parameters.map("j").list("values").map { it.toString().toBooleanStrict() }
        val commitIntervals = parameters.map("commitIntervalMs").list("values").map { it.toString().toInt() }
        require(ws.isNotEmpty() && js.isNotEmpty() && commitIntervals.isNotEmpty()) {
            "Each parameter must define at least one value in ${parameterPath.absolutePath}"
        }
        val matrix = commitIntervals.flatMap { ci ->
            ws.flatMap { w -> js.map { j -> PocConfig(w, j, ci) } }
        }
        return config to matrix
    }

    private fun validate(dataset: DatasetSettings, workload: WorkloadSettings, mongo: MongoRuntimeSettings) {
        require(dataset.initialDocuments > 0) { "dataset.initialDocuments must be greater than 0" }
        require(dataset.documentSizeBytes >= 512) { "dataset.documentSizeBytes should be at least 512 bytes" }
        require(dataset.batchSize > 0) { "dataset.batchSize must be greater than 0" }
        require(mongo.wiredTigerCacheSizeGB >= 0.25) { "mongo.wiredTigerCacheSizeGB must be at least 0.25" }
        require(workload.readMode == "randomById") { "Only workload.readMode=randomById is supported in this PoC" }
        val ratioSum = workload.readRatio + workload.insertRatio + workload.updateRatio
        require(kotlin.math.abs(ratioSum - 1.0) < 0.0001) {
            "workload read/insert/update ratios must sum to 1.0, got $ratioSum"
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun Map<String, Any>.map(key: String): Map<String, Any> =
        get(key) as? Map<String, Any> ?: error("Missing YAML map: $key")

    @Suppress("UNCHECKED_CAST")
    private fun Map<String, Any>.optionalMap(key: String): Map<String, Any>? =
        get(key) as? Map<String, Any>

    @Suppress("UNCHECKED_CAST")
    private fun Map<String, Any>.list(key: String): List<Any> =
        get(key) as? List<Any> ?: error("Missing YAML list: $key")

    private fun Map<String, Any>.string(key: String): String =
        get(key)?.toString() ?: error("Missing YAML value: $key")

    private fun Map<String, Any>.optionalString(key: String): String? = get(key)?.toString()
    private fun Map<String, Any>.long(key: String) = string(key).toLong()
    private fun Map<String, Any>.int(key: String) = string(key).toInt()
    private fun Map<String, Any>.optionalInt(key: String) = optionalString(key)?.toInt()
    private fun Map<String, Any>.optionalDouble(key: String) = optionalString(key)?.toDouble()
    private fun Map<String, Any>.optionalBoolean(key: String) = optionalString(key)?.toBooleanStrict()
}

enum class OperationType(val jsonName: String) {
    READ("read"),
    INSERT("insert"),
    UPDATE("update")
}

data class OperationSample(
    val type: OperationType,
    val latencyMs: Double
)

data class ThreadStats(
    val samples: MutableList<OperationSample> = mutableListOf(),
    val errors: EnumMap<OperationType, Int> = emptyErrorMap()
)

data class WorkerHandle(
    val stats: ThreadStats,
    val thread: Thread
)

data class OperationMetrics(
    val operations: Int,
    val successfulOperations: Int,
    val errors: Int,
    val throughputOpsPerSecond: Double,
    val meanLatencyMs: Double,
    val stdDevLatencyMs: Double,
    val minLatencyMs: Double,
    val p50LatencyMs: Double,
    val p90LatencyMs: Double,
    val p95LatencyMs: Double,
    val p99LatencyMs: Double,
    val p999LatencyMs: Double,
    val maxLatencyMs: Double
) {
    fun toDocument(): Document = Document()
        .append("operations", operations)
        .append("successfulOperations", successfulOperations)
        .append("errors", errors)
        .append("throughputOpsPerSecond", throughputOpsPerSecond)
        .append("meanLatencyMs", meanLatencyMs)
        .append("stdDevLatencyMs", stdDevLatencyMs)
        .append("minLatencyMs", minLatencyMs)
        .append("p50LatencyMs", p50LatencyMs)
        .append("p90LatencyMs", p90LatencyMs)
        .append("p95LatencyMs", p95LatencyMs)
        .append("p99LatencyMs", p99LatencyMs)
        .append("p999LatencyMs", p999LatencyMs)
        .append("maxLatencyMs", maxLatencyMs)
}

data class BenchmarkResult(
    val config: PocConfig,
    val repetition: Int,
    val startedAt: String,
    val durationSeconds: Long,
    val warmupSeconds: Long,
    val threads: Int,
    val primaryPort: Int,
    val dataset: DatasetSettings,
    val workload: WorkloadSettings,
    val overall: OperationMetrics,
    val read: OperationMetrics,
    val insert: OperationMetrics,
    val update: OperationMetrics
) {
    fun toJson(): String = Document()
        .append(
            "config",
            Document()
                .append("w", config.w)
                .append("j", config.journal)
                .append("commitIntervalMs", config.commitIntervalMs)
        )
        .append("topology", "4-data-replica-set-members-plus-arbiter")
        .append("repetition", repetition)
        .append("startedAt", startedAt)
        .append("durationSeconds", durationSeconds)
        .append("warmupSeconds", warmupSeconds)
        .append("threads", threads)
        .append("primaryPort", primaryPort)
        .append(
            "dataset",
            Document()
                .append("initialDocuments", dataset.initialDocuments)
                .append("documentSizeBytes", dataset.documentSizeBytes)
                .append("batchSize", dataset.batchSize)
        )
        .append(
            "workload",
            Document()
                .append("readRatio", workload.readRatio)
                .append("insertRatio", workload.insertRatio)
                .append("updateRatio", workload.updateRatio)
                .append("readMode", workload.readMode)
        )
        .append(
            "metrics",
            Document()
                .append("overall", overall.toDocument())
                .append("read", read.toDocument())
                .append("insert", insert.toDocument())
                .append("update", update.toDocument())
        )
        .toJson()
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
    private val containers = listOf("mongo1", "mongo2", "mongo3", "mongo4", "mongoarbiter")
    private var runningConfig: Int? = null

    fun start(config: PocConfig, mongo: MongoRuntimeSettings): Int {
        if (runningConfig == config.commitIntervalMs && containersAreRunning()) {
            return try {
                waitForPrimary()
                primaryPort()
            } catch (_: Exception) {
                recreateReplicaSet(config, mongo)
            }
        }
        return recreateReplicaSet(config, mongo)
    }

    private fun recreateReplicaSet(config: PocConfig, mongo: MongoRuntimeSettings): Int {
        File(root, ".env").writeText(
            """
            MONGO_COMMIT_INTERVAL_MS=${config.commitIntervalMs}
            MONGO_WT_CACHE_GB=${mongo.wiredTigerCacheSizeGB}
            """.trimIndent() + "\n"
        )
        runner.run("docker", "compose", "up", "-d", "--force-recreate", quiet = true)
        waitForContainersRunning()
        waitForMongoAvailable()
        initiateReplicaSet()
        waitForPrimary()
        runningConfig = config.commitIntervalMs
        return primaryPort()
    }

    private fun containersAreRunning(): Boolean =
        containers.all { container ->
            try {
                runner.run("docker", "inspect", "--format", "{{.State.Running}}", container, quiet = true).trim() == "true"
            } catch (_: Exception) {
                false
            }
        }

    private fun waitForContainersRunning() {
        repeat(60) {
            val states = containers.associateWith { container ->
                try {
                    runner.run("docker", "inspect", "--format", "{{.State.Running}}", container, quiet = true).trim()
                } catch (_: Exception) {
                    "false"
                }
            }
            if (states.values.all { it == "true" }) return
            Thread.sleep(1_000)
        }
        val logs = runner.run("docker", "compose", "logs", "--tail", "120", quiet = true)
        error("MongoDB containers did not stay running. Docker logs:\n$logs")
    }

    private fun waitForMongoAvailable() {
        repeat(60) {
            try {
                containers.forEach { container ->
                    runner.run(
                        "docker", "exec", container, "mongosh", "--quiet", "--eval", "db.adminCommand({ping:1})",
                        quiet = true
                    )
                }
                return
            } catch (_: Exception) {
                Thread.sleep(1_000)
            }
        }
        val logs = runner.run("docker", "compose", "logs", "--tail", "120", quiet = true)
        error("MongoDB did not accept connections within 60 seconds. Docker logs:\n$logs")
    }

    private fun initiateReplicaSet() {
        runner.run(
            "docker", "exec", "mongo1", "mongosh", "--quiet", "--eval",
            """
            try {
              rs.status()
            } catch (e) {
              rs.initiate({
                _id:'rs0',
                members:[
                  {_id:0,host:'host.docker.internal:27017'},
                  {_id:1,host:'host.docker.internal:27018'},
                  {_id:2,host:'host.docker.internal:27019'},
                  {_id:3,host:'host.docker.internal:27020'},
                  {_id:4,host:'host.docker.internal:27021',arbiterOnly:true}
                ]
              })
            }
            """.trimIndent(),
            quiet = true
        )
    }

    private fun waitForPrimary() {
        repeat(60) {
            try {
                val hasPrimary = runner.run(
                    "docker", "exec", "mongo1", "mongosh", "--quiet", "--eval",
                    "rs.status().members.some(m => m.stateStr === 'PRIMARY')", quiet = true
                ).trim()
                if (hasPrimary == "true") return
            } catch (_: Exception) {
            }
            Thread.sleep(1_000)
        }
        error("Replica set did not become ready within 60 seconds")
    }

    private fun primaryPort(): Int {
        val primary = runner.run(
            "docker", "exec", "mongo1", "mongosh", "--quiet", "--eval",
            "rs.status().members.find(m => m.stateStr === 'PRIMARY').name", quiet = true
        ).trim()
        return primary.substringAfterLast(':').toIntOrNull()
            ?: error("Could not determine the published port for the primary: $primary")
    }
}

class Benchmark(
    private val uri: String,
    private val settings: BenchmarkSettings,
    private val repetition: Int,
    private val primaryPort: Int
) {
    private val payload = payloadFor(settings.dataset.documentSizeBytes)

    fun run(config: PocConfig): BenchmarkResult {
        val clientSettings = MongoClientSettings.builder()
            .applyConnectionString(ConnectionString(uri))
            .retryWrites(false)
            .build()
        MongoClients.create(clientSettings).use { client ->
            val database = client.getDatabase("benchmark")
            val prepareCollection = database.getCollection("operations")
                .withWriteConcern(WriteConcern.MAJORITY.withJournal(true))
            prepareDataset(prepareCollection)
            val measuredCollection = prepareCollection.withWriteConcern(writeConcern(config))
            runWarmup(measuredCollection)

            val startedAt = Instant.now().toString()
            val endAt = System.nanoTime() + settings.durationSeconds * 1_000_000_000L
            val workers = (0 until settings.threads).map { thread ->
                val stats = ThreadStats()
                val worker = Thread {
                        var operationCounter = 0L
                        while (System.nanoTime() < endAt) {
                            val type = chooseOperation()
                            val started = System.nanoTime()
                            try {
                                performOperation(measuredCollection, type, thread, operationCounter, "measured")
                                stats.samples += OperationSample(type, (System.nanoTime() - started) / 1_000_000.0)
                            } catch (_: Exception) {
                                stats.errors[type] = stats.errors.getValue(type) + 1
                            }
                            operationCounter++
                        }
                    }
                worker.start()
                WorkerHandle(stats, worker)
            }
            workers.forEach { it.thread.join() }
            val samples = workers.flatMap { it.stats.samples }
            val errors = mergeErrors(workers.map { it.stats.errors })
            return BenchmarkResult(
                config = config,
                repetition = repetition,
                startedAt = startedAt,
                durationSeconds = settings.durationSeconds,
                warmupSeconds = settings.warmupSeconds,
                threads = settings.threads,
                primaryPort = primaryPort,
                dataset = settings.dataset,
                workload = settings.workload,
                overall = metrics(samples, errors, null),
                read = metrics(samples, errors, OperationType.READ),
                insert = metrics(samples, errors, OperationType.INSERT),
                update = metrics(samples, errors, OperationType.UPDATE)
            )
        }
    }

    private fun prepareDataset(collection: MongoCollection<Document>) {
        collection.drop()
        val batchSize = settings.dataset.batchSize
        for (start in 0 until settings.dataset.initialDocuments step batchSize) {
            val endExclusive = minOf(start + batchSize, settings.dataset.initialDocuments)
            val documents = (start until endExclusive).map { index -> baseDocument("base-$index", index) }
            collection.insertMany(documents)
        }
    }

    private fun runWarmup(collection: MongoCollection<Document>) {
        val endAt = System.nanoTime() + settings.warmupSeconds * 1_000_000_000L
        val workers = (0 until settings.threads).map { thread ->
            Thread {
                var operationCounter = 0L
                while (System.nanoTime() < endAt) {
                    try {
                        performOperation(collection, chooseOperation(), thread, operationCounter, "warmup")
                    } catch (_: Exception) {
                        // Warm-up failures are intentionally not measured.
                    }
                    operationCounter++
                }
            }
        }
        workers.forEach(Thread::start)
        workers.forEach(Thread::join)
    }

    private fun chooseOperation(): OperationType {
        val roll = ThreadLocalRandom.current().nextDouble()
        return when {
            roll < settings.workload.readRatio -> OperationType.READ
            roll < settings.workload.readRatio + settings.workload.insertRatio -> OperationType.INSERT
            else -> OperationType.UPDATE
        }
    }

    private fun performOperation(
        collection: MongoCollection<Document>,
        type: OperationType,
        thread: Int,
        operationCounter: Long,
        phase: String
    ) {
        when (type) {
            OperationType.READ -> {
                val id = randomBaseId()
                collection.find(Document("_id", id)).first()
            }
            OperationType.INSERT -> {
                val id = "$phase-r$repetition-t$thread-$operationCounter-${System.nanoTime()}"
                collection.insertOne(baseDocument(id, ThreadLocalRandom.current().nextInt()))
            }
            OperationType.UPDATE -> {
                val id = randomBaseId()
                collection.updateOne(
                    Document("_id", id),
                    Document("\$inc", Document("version", 1))
                        .append("\$set", Document("updatedAt", Instant.now().toString()))
                )
            }
        }
    }

    private fun baseDocument(id: String, index: Int): Document = Document("_id", id)
        .append("tenantId", index % 100)
        .append("status", listOf("new", "active", "archived")[kotlin.math.abs(index % 3)])
        .append("createdAt", Instant.EPOCH.plusSeconds(index.toLong()).toString())
        .append("updatedAt", Instant.EPOCH.plusSeconds(index.toLong()).toString())
        .append("version", 0)
        .append("payload", payload)

    private fun randomBaseId(): String = "base-${ThreadLocalRandom.current().nextInt(settings.dataset.initialDocuments)}"

    private fun metrics(
        samples: List<OperationSample>,
        errors: EnumMap<OperationType, Int>,
        type: OperationType?
    ): OperationMetrics {
        val selected = samples
            .asSequence()
            .filter { type == null || it.type == type }
            .map { it.latencyMs }
            .sorted()
            .toList()
        val errorCount = if (type == null) errors.values.sum() else errors.getValue(type)
        val successes = selected.size
        val operations = successes + errorCount
        fun percentile(p: Double): Double = if (selected.isEmpty()) 0.0 else selected[minOf(selected.lastIndex, ceil(p * selected.size).toInt() - 1)]
        val mean = selected.averageOrZero()
        val stdDev = if (selected.isEmpty()) 0.0 else sqrt(selected.sumOf { (it - mean).pow(2) } / selected.size)
        return OperationMetrics(
            operations = operations,
            successfulOperations = successes,
            errors = errorCount,
            throughputOpsPerSecond = successes / settings.durationSeconds.toDouble(),
            meanLatencyMs = mean,
            stdDevLatencyMs = stdDev,
            minLatencyMs = selected.firstOrZero(),
            p50LatencyMs = percentile(.50),
            p90LatencyMs = percentile(.90),
            p95LatencyMs = percentile(.95),
            p99LatencyMs = percentile(.99),
            p999LatencyMs = percentile(.999),
            maxLatencyMs = selected.lastOrZero()
        )
    }

    private fun writeConcern(config: PocConfig): WriteConcern = when (config.w) {
        "majority" -> WriteConcern.MAJORITY.withJournal(config.journal)
        else -> WriteConcern(config.w.toInt()).withJournal(config.journal)
    }
}

private fun emptyErrorMap(): EnumMap<OperationType, Int> =
    EnumMap<OperationType, Int>(OperationType::class.java).apply {
        OperationType.entries.forEach { this[it] = 0 }
    }

private fun mergeErrors(errors: List<EnumMap<OperationType, Int>>): EnumMap<OperationType, Int> =
    emptyErrorMap().apply {
        errors.forEach { threadErrors ->
            OperationType.entries.forEach { type -> this[type] = getValue(type) + threadErrors.getValue(type) }
        }
    }

private fun payloadFor(targetDocumentSizeBytes: Int): String =
    "x".repeat((targetDocumentSizeBytes - 256).coerceAtLeast(256))

private fun List<Double>.averageOrZero() = if (isEmpty()) 0.0 else average()
private fun List<Double>.firstOrZero() = firstOrNull() ?: 0.0
private fun List<Double>.lastOrZero() = lastOrNull() ?: 0.0

private fun buildSchedule(matrix: List<PocConfig>, experiment: ExperimentConfig): List<ScheduledRun> {
    val rng = Random(experiment.randomSeed)
    val commitGroups = matrix.groupBy { it.commitIntervalMs }.toList()
    val orderedGroups = if (experiment.randomizeOrder) commitGroups.shuffled(rng) else commitGroups
    return orderedGroups.flatMap { (_, configs) ->
        val runs = configs.flatMap { config ->
            (1..experiment.repetitions).map { repetition -> ScheduledRun(config, repetition) }
        }
        if (experiment.randomizeOrder) runs.shuffled(rng) else runs
    }
}

fun main(args: Array<String>) {
    val root = File(System.getProperty("user.dir"))
    val configPath = File(args.firstOrNull { it.startsWith("--config=") }?.substringAfter('=') ?: "config/poc-experiment.yml")
    val (experiment, matrix) = YamlConfigLoader(root).loadExperiment(configPath)
    val output = File(args.firstOrNull { it.startsWith("--output=") }?.substringAfter('=') ?: experiment.output)
    val settings = BenchmarkSettings(
        durationSeconds = args.firstOrNull { it.startsWith("--duration=") }?.substringAfter('=')?.toLong() ?: experiment.durationSeconds,
        warmupSeconds = args.firstOrNull { it.startsWith("--warmup=") }?.substringAfter('=')?.toLong() ?: experiment.warmupSeconds,
        threads = args.firstOrNull { it.startsWith("--threads=") }?.substringAfter('=')?.toInt() ?: experiment.threads,
        dataset = experiment.dataset,
        workload = experiment.workload
    )
    val docker = MongoDockerController(root)
    // Docker advertises host.docker.internal to members, but that name is not resolvable
    // by the host JVM on macOS. Direct primary connection still preserves replica-set
    // acknowledgement semantics for w=majority while avoiding host-side DNS discovery.
    output.parentFile?.mkdirs()
    output.writeText("")
    val schedule = buildSchedule(matrix, experiment)
    schedule.forEachIndexed { index, scheduledRun ->
        println("[${index + 1}/${schedule.size}] ${scheduledRun.config.id} repetition=${scheduledRun.repetition}")
        val primaryPort = docker.start(scheduledRun.config, experiment.mongo)
        val result = Benchmark(
            uri = "mongodb://localhost:$primaryPort/?directConnection=true",
            settings = settings,
            repetition = scheduledRun.repetition,
            primaryPort = primaryPort
        ).run(scheduledRun.config)
        output.appendText(result.toJson() + "\n")
    }
    println("Results written to ${output.absolutePath}")
}
