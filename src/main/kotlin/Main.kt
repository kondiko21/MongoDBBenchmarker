package org.example

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

fun main(args: Array<String>) {
    val workerPath = args.firstOrNull { it.startsWith("--worker=") }?.substringAfter('=')
    if (workerPath != null) {
        BenchmarkWorker(runSpecFromJson(File(workerPath).readText())).run()
        return
    }

    val root = File(System.getProperty("user.dir"))
    val configPath = File(root, args.firstOrNull { it.startsWith("--config=") }?.substringAfter('=') ?: "config/performance-experiment.yml")
    val (baseExperiment, matrix) = ConfigLoader(root).load(configPath)
    val stamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"))
    val actualOutput = "${baseExperiment.outputDirectory}/experiment-$stamp"
    val experiment = baseExperiment.copy(outputDirectory = actualOutput)
    val runs = buildRuns(experiment, matrix)
    val runtime = File(root, ".runtime").apply { mkdirs() }
    File(root, actualOutput).mkdirs()
    val infrastructure = MongoInfrastructure(root, experiment.mongo)
    var currentEnvironment: Pair<Int, NetworkProfile>? = null
    var measuredRtt = 0.0

    runs.forEachIndexed { index, original ->
        val environment = original.dataNodes to original.network
        if (environment != currentEnvironment) {
            measuredRtt = infrastructure.prepare(original.dataNodes, original.network)
            currentEnvironment = environment
            println("Network ${original.network.name}: measured RTT ${"%.2f".format(measuredRtt)} ms")
        }
        val spec = original.copy(measuredRttMs = measuredRtt)
        println("[${index + 1}/${runs.size}] ${spec.label}")
        infrastructure.applySupplementary(spec)
        val specFile = File(runtime, "current-run.json")
        specFile.writeText(spec.toDocument().toJson())
        infrastructure.runWorker(specFile.name)
    }
    println("Results written to ${File(root, actualOutput).absolutePath}")
}
