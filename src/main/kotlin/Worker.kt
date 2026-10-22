package org.example

import com.mongodb.MongoClientSettings
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.WriteConcern
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.UpdateOptions
import org.bson.Document
import java.io.File
import java.time.Instant
import java.util.Collections
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicLongArray

class BenchmarkWorker(private val spec: RunSpec) {
    private val uri = "mongodb://${(1..spec.dataNodes).joinToString(",") { "mongo$it:27017" }}/?replicaSet=rs0&retryWrites=true"
    private val payload = "x".repeat((spec.dataset.documentSizeBytes - 256).coerceAtLeast(256))

    fun run() {
        val settings = MongoClientSettings.builder().applyConnectionString(com.mongodb.ConnectionString(uri)).build()
        MongoClients.create(settings).use { client ->
            val database = client.getDatabase("benchmark")
            val base = database.getCollection("documents")
            ensureDataset(base.withWriteConcern(WriteConcern.MAJORITY))
            val result = when (spec.suite) {
                Suite.PERFORMANCE -> runPerformance(readCollection(base.withWriteConcern(writeConcern())))
                Suite.FRESHNESS -> runFreshness(database.getCollection("registers"))
                Suite.SUPPLEMENTARY -> runPerformance(readCollection(base.withWriteConcern(writeConcern())))
            }
            writeSummary(result, client.getDatabase("admin"))
        }
    }

    private fun ensureDataset(collection: MongoCollection<Document>) {
        if (collection.countDocuments() == spec.dataset.initialDocuments.toLong()) return
        collection.drop()
        for (start in 0 until spec.dataset.initialDocuments step spec.dataset.batchSize) {
            val end = minOf(start + spec.dataset.batchSize, spec.dataset.initialDocuments)
            collection.insertMany((start until end).map { index ->
                Document("_id", "base-$index").append("version", 0).append("payload", payload)
            })
        }
    }

    private enum class Operation { READ, INSERT, UPDATE }
    private data class Sample(val operation: Operation, val latencyMs: Double)
    private data class PhaseResult(val samples: List<Sample>, val errors: Map<Operation, Long>)

    private fun runPerformance(collection: MongoCollection<Document>): Document {
        performancePhase(collection, spec.warmupSeconds, record = false, phaseName = "warmup")
        val result = performancePhase(collection, spec.durationSeconds, record = true, phaseName = "measured")
        collection.deleteMany(Document("_id", Document("\$regex", "^insert-${spec.runId}-")))
        fun block(operation: Operation?): Document {
            val latencies = result.samples.filter { operation == null || it.operation == operation }.map { it.latencyMs }
            val errors = if (operation == null) result.errors.values.sum() else result.errors.getValue(operation)
            return metrics(latencies, errors, spec.durationSeconds).document()
        }
        return Document("metrics", Document("overall", block(null))
            .append("read", block(Operation.READ)).append("insert", block(Operation.INSERT))
            .append("update", block(Operation.UPDATE)))
    }

    private fun performancePhase(
        collection: MongoCollection<Document>, seconds: Long, record: Boolean, phaseName: String
    ): PhaseResult {
        if (seconds == 0L) return PhaseResult(emptyList(), Operation.entries.associateWith { 0L })
        val samples = Collections.synchronizedList(mutableListOf<Sample>())
        val errors = Operation.entries.associateWith { AtomicLong() }
        val sequence = AtomicLong()
        val endAt = System.nanoTime() + seconds * 1_000_000_000L
        val threads = (0 until spec.concurrency).map { clientId ->
            Thread {
                var operationCounter = 0L
                while (System.nanoTime() < endAt) {
                    val index = Math.floorMod(sequence.getAndIncrement(), spec.dataset.initialDocuments.toLong()).toInt()
                    val roll = ThreadLocalRandom.current().nextDouble()
                    val operation = when {
                        roll < spec.workload.readRatio -> Operation.READ
                        roll < spec.workload.readRatio + spec.workload.insertRatio -> Operation.INSERT
                        else -> Operation.UPDATE
                    }
                    val started = System.nanoTime()
                    try {
                        when (operation) {
                            Operation.READ -> collection.find(Document("_id", "base-$index")).first()
                            Operation.INSERT -> collection.insertOne(
                                Document("_id", "insert-${spec.runId}-$phaseName-$clientId-$operationCounter")
                                    .append("version", 0).append("payload", payload)
                            )
                            Operation.UPDATE -> collection.updateOne(
                                Document("_id", "base-$index"), Document("\$inc", Document("version", 1))
                            )
                        }
                        if (record) samples += Sample(operation, (System.nanoTime() - started) / 1_000_000.0)
                    } catch (_: Exception) {
                        if (record) errors.getValue(operation).incrementAndGet()
                    }
                    operationCounter++
                }
            }.also(Thread::start)
        }
        threads.forEach(Thread::join)
        return PhaseResult(samples.toList(), errors.mapValues { it.value.get() })
    }

    private fun runFreshness(registers: MongoCollection<Document>): Document {
        resetRegisters(registers)
        freshnessPhase(registers, spec.warmupSeconds, record = false)
        resetRegisters(registers)
        return freshnessPhase(registers, spec.durationSeconds, record = true)
    }

    private fun resetRegisters(registers: MongoCollection<Document>) {
        val prepared = registers.withWriteConcern(WriteConcern.MAJORITY)
        prepared.drop()
        prepared.insertMany((0 until spec.dataset.registerCount).map {
            Document("_id", "register-$it").append("version", 0).append("payload", payload)
        })
    }

    private fun freshnessPhase(registers: MongoCollection<Document>, seconds: Long, record: Boolean): Document {
        val writeCollection = registers.withWriteConcern(writeConcern())
        val readCollection = readCollection(registers)
        val nextVersion = AtomicLongArray(spec.dataset.registerCount)
        val operationIds = AtomicLong()
        val events = ConcurrentLinkedQueue<HistoryEvent>()
        val readLatencies = Collections.synchronizedList(mutableListOf<Double>())
        val writeLatencies = Collections.synchronizedList(mutableListOf<Double>())
        val readErrors = AtomicLong()
        val writeErrors = AtomicLong()
        val output = outputDir()
        output.mkdirs()
        val startNs = System.nanoTime()
        val endAt = startNs + seconds * 1_000_000_000L

        val writer = Thread {
            var key = 0
            while (System.nanoTime() < endAt) {
                val version = nextVersion.incrementAndGet(key)
                val operationId = operationIds.incrementAndGet()
                val started = System.nanoTime()
                try {
                    writeCollection.updateOne(
                        Document("_id", "register-$key"),
                        Document("\$set", Document("version", version)), UpdateOptions().upsert(false)
                    )
                    val ended = System.nanoTime()
                    if (record) {
                        writeLatencies += (ended - started) / 1_000_000.0
                        events += HistoryEvent(operationId, "write", "writer-0", "register-$key",
                            started - startNs, ended - startNs, version, "success")
                    }
                } catch (e: Exception) {
                    if (record) {
                        val ended = System.nanoTime()
                        writeErrors.incrementAndGet()
                        events += HistoryEvent(operationId, "write", "writer-0", "register-$key",
                            started - startNs, ended - startNs, version, "error", e.javaClass.simpleName)
                    }
                }
                key = (key + 1) % spec.dataset.registerCount
            }
        }.also(Thread::start)

        val readers = (0 until spec.concurrency).map { clientId ->
            Thread {
                while (System.nanoTime() < endAt) {
                    val key = ThreadLocalRandom.current().nextInt(spec.dataset.registerCount)
                    val operationId = operationIds.incrementAndGet()
                    val started = System.nanoTime()
                    try {
                        val document = readCollection.find(Document("_id", "register-$key")).first()
                        val ended = System.nanoTime()
                        val observed = document?.get("version")?.toString()?.toLong() ?: -1L
                        if (record) {
                            readLatencies += (ended - started) / 1_000_000.0
                            events += HistoryEvent(operationId, "read", "reader-$clientId", "register-$key",
                                started - startNs, ended - startNs, observed, "success")
                        }
                    } catch (e: Exception) {
                        if (record) {
                            val ended = System.nanoTime()
                            readErrors.incrementAndGet()
                            events += HistoryEvent(operationId, "read", "reader-$clientId", "register-$key",
                                started - startNs, ended - startNs, null, "error", e.javaClass.simpleName)
                        }
                    }
                }
            }.also(Thread::start)
        }
        writer.join()
        readers.forEach(Thread::join)
        if (!record) return Document()
        val orderedEvents = events.sortedWith(compareBy<HistoryEvent> { it.startedNs }.thenBy { it.operationId })
        File(output, "${spec.label}-history.jsonl").bufferedWriter().use { history ->
            orderedEvents.forEach { event ->
                history.appendLine(event.document().append("runId", spec.runId).toJson())
            }
        }
        val analysis = analyzeHistory(orderedEvents)
        return Document("readMetrics", metrics(readLatencies.toList(), readErrors.get(), seconds).document())
            .append("writeMetrics", metrics(writeLatencies.toList(), writeErrors.get(), seconds).document())
            .append("freshness", Document("reads", analysis.successfulReads)
                .append("staleReads", analysis.staleReads).append("futureReads", analysis.futureReads)
                .append("staleReadPercent", if (analysis.successfulReads == 0L) 0.0 else analysis.staleReads * 100.0 / analysis.successfulReads)
                .append("maxVersionLag", analysis.versionLags.maxOrNull() ?: 0L)
                .append("staleness", metrics(analysis.stalenessMs, 0, seconds).document()))
    }

    private fun readCollection(collection: MongoCollection<Document>): MongoCollection<Document> {
        val scenario = requireNotNull(spec.readScenario)
        val concern = when (scenario.concern) {
            "local" -> ReadConcern.LOCAL
            "majority" -> ReadConcern.MAJORITY
            "linearizable" -> ReadConcern.LINEARIZABLE
            else -> error("Unsupported readConcern ${scenario.concern}")
        }
        val preference = when (scenario.preference) {
            "primary" -> ReadPreference.primary()
            "secondary" -> spec.supplementaryVariant?.maxStalenessSeconds
                ?.takeIf { it >= 90 }
                ?.let { ReadPreference.secondary(emptyList(), it.toLong(), TimeUnit.SECONDS) }
                ?: ReadPreference.secondary()
            else -> error("Unsupported readPreference ${scenario.preference}")
        }
        return collection.withReadConcern(concern).withReadPreference(preference)
    }

    private fun writeConcern(): WriteConcern = when (spec.writeConcern) {
        "1" -> WriteConcern.W1
        "majority" -> WriteConcern.MAJORITY
        else -> error("Missing or unsupported write concern: ${spec.writeConcern}")
    }

    private fun writeSummary(result: Document, admin: MongoDatabase) {
        val summary = spec.toDocument()
            .append("label", spec.label).append("startedAt", Instant.now().toString())
            .append("mongoVersion", "8.0").append("result", result)
        if (spec.suite == Suite.SUPPLEMENTARY) {
            val serverStatus = admin.runCommand(Document("serverStatus", 1))
            val replicaStatus = admin.runCommand(Document("replSetGetStatus", 1))
            val members = replicaStatus.getList("members", Document::class.java).map { member ->
                Document("name", member.getString("name")).append("state", member.getString("stateStr"))
                    .append("syncSourceHost", member.getString("syncSourceHost"))
                    .append("optimeDate", member["optimeDate"])
            }
            summary.append(
                "supplementaryDiagnostics",
                Document("flowControl", serverStatus["flowControl"]).append("members", members)
            )
        }
        val output = outputDir()
        output.mkdirs()
        File(output, "summary.jsonl").appendText(summary.toJson() + "\n")
    }

    private fun outputDir(): File = File("/results", spec.outputDirectory.removePrefix("results/"))
}
