package org.example

import org.bson.Document
import org.yaml.snakeyaml.Yaml
import java.io.File
import java.util.UUID

class ConfigLoader(private val root: File) {
    private val yaml = Yaml()

    fun load(path: File): Pair<ExperimentSettings, ParameterMatrix> {
        val document = yaml.load<Map<String, Any>>(path.readText())
        val experimentMap = document.map("experiment")
        val datasetMap = document.map("dataset")
        val workloadMap = document.map("workload")
        val mongoMap = document.map("mongo")
        val experiment = ExperimentSettings(
            durationSeconds = experimentMap.long("durationSeconds"),
            warmupSeconds = experimentMap.long("warmupSeconds"),
            repetitions = experimentMap.int("repetitions"),
            randomizeOrder = experimentMap.bool("randomizeOrder"),
            randomSeed = experimentMap.int("randomSeed"),
            suites = experimentMap.list("suites").map { Suite.valueOf(it.toString().uppercase()) }.toSet(),
            outputDirectory = experimentMap.string("outputDirectory"),
            dataset = DatasetSettings(
                initialDocuments = datasetMap.int("initialDocuments"),
                documentSizeBytes = datasetMap.int("documentSizeBytes"),
                batchSize = datasetMap.int("batchSize"),
                registerCount = datasetMap.int("registerCount")
            ),
            workload = WorkloadSettings(
                readRatio = workloadMap.double("readRatio"),
                insertRatio = workloadMap.double("insertRatio"),
                updateRatio = workloadMap.double("updateRatio")
            ),
            mongo = MongoSettings(
                wiredTigerCacheSizeGB = mongoMap.double("wiredTigerCacheSizeGB"),
                imageVersion = mongoMap.string("imageVersion"),
                readinessTimeoutSeconds = mongoMap.int("readinessTimeoutSeconds")
            )
        )
        val parametersPath = File(root, document.string("parametersFile"))
        val parameters = yaml.load<Map<String, Any>>(parametersPath.readText())
        val matrix = ParameterMatrix(
            topologies = parameters.list("topologies").map { it.toString().toInt() },
            networkProfiles = parameters.list("networkProfiles").map { raw ->
                val value = raw as Map<String, Any>
                NetworkProfile(value.string("name"), value.int("latencyMs"), value.int("jitterMs"))
            },
            writeConcerns = parameters.list("writeConcerns").map(Any::toString),
            readConcerns = parameters.list("readConcerns").map(Any::toString),
            readPreferences = parameters.list("readPreferences").map(Any::toString),
            excludedCombinations = parameters.list("excludedCombinations").map { raw ->
                @Suppress("UNCHECKED_CAST")
                (raw as Map<String, Any>).mapValues { it.value.toString() }
            },
            maxStalenessSeconds = parameters.optionalList("maxStalenessSeconds").orEmpty()
                .map { it.toString().toInt().takeUnless { value -> value == -1 } },
            enableFlowControl = parameters.optionalList("enableFlowControl").orEmpty()
                .map { it.toString().toBooleanStrict() },
            flowControlTargetLagSeconds = parameters.optionalList("flowControlTargetLagSeconds").orEmpty()
                .map { it.toString().toInt().takeUnless { value -> value == -1 } },
            chainingAllowed = parameters.optionalList("chainingAllowed").orEmpty()
                .map { it.toString().toBooleanStrict() },
            concurrency = parameters.list("concurrency").map { it.toString().toInt() }
        )
        validate(experiment, matrix)
        return experiment to matrix
    }

    private fun validate(experiment: ExperimentSettings, matrix: ParameterMatrix) {
        require(matrix.topologies.isNotEmpty() && matrix.topologies.all { it == 3 || it == 5 }) {
            "topologies supports only replica sets with 3 and 5 data-bearing members"
        }
        require(matrix.writeConcerns.isNotEmpty() && matrix.writeConcerns.all { it == "1" || it == "majority" }) {
            "writeConcerns supports only 1 and majority"
        }
        require(matrix.networkProfiles.isNotEmpty() && matrix.networkProfiles.all { it.latencyMs >= 0 && it.jitterMs >= 0 })
        require(matrix.concurrency.isNotEmpty() && matrix.concurrency.all { it > 0 })
        require(experiment.repetitions > 0 && experiment.durationSeconds > 0 && experiment.warmupSeconds >= 0)
        require(experiment.dataset.initialDocuments > 0 && experiment.dataset.registerCount > 0)
        val ratioSum = experiment.workload.readRatio + experiment.workload.insertRatio + experiment.workload.updateRatio
        require(kotlin.math.abs(ratioSum - 1.0) < 0.000001) { "workload ratios must sum to 1.0" }
        require(matrix.readPreferences.all { it in setOf("primary", "secondary") })
        require(matrix.readConcerns.all { it in setOf("local", "majority", "linearizable") })
        require(matrix.excludedCombinations.any {
            it["readPreference"] == "secondary" && it["readConcern"] == "linearizable"
        }) { "excludedCombinations must exclude secondary + linearizable" }
        if (Suite.SUPPLEMENTARY in experiment.suites) {
            require(matrix.maxStalenessSeconds.isNotEmpty() && matrix.maxStalenessSeconds.all { it == null || it >= 90 })
            require(matrix.enableFlowControl.isNotEmpty())
            require(matrix.flowControlTargetLagSeconds.isNotEmpty() && matrix.flowControlTargetLagSeconds.all { it == null || it > 0 })
            require(matrix.chainingAllowed.isNotEmpty())
            fun excludes(vararg fields: Pair<String, String>) = matrix.excludedCombinations.any { exclusion ->
                fields.all { (key, value) -> exclusion[key] == value }
            }
            matrix.maxStalenessSeconds.filterNotNull().forEach { value ->
                require(excludes("readPreference" to "primary", "maxStalenessSeconds" to value.toString())) {
                    "excludedCombinations must exclude maxStalenessSeconds=$value with primary"
                }
            }
            require(excludes("enableFlowControl" to "true", "flowControlTargetLagSeconds" to "-1"))
            matrix.flowControlTargetLagSeconds.filterNotNull().forEach { value ->
                require(excludes("enableFlowControl" to "false", "flowControlTargetLagSeconds" to value.toString())) {
                    "excludedCombinations must exclude target lag $value when Flow Control is disabled"
                }
            }
        }
    }

    @Suppress("UNCHECKED_CAST") private fun Map<String, Any>.map(key: String) =
        get(key) as? Map<String, Any> ?: error("Missing YAML map: $key")
    @Suppress("UNCHECKED_CAST") private fun Map<String, Any>.list(key: String) =
        get(key) as? List<Any> ?: error("Missing YAML list: $key")
    @Suppress("UNCHECKED_CAST") private fun Map<String, Any>.optionalList(key: String) = get(key) as? List<Any>
    private fun Map<String, Any>.string(key: String) = get(key)?.toString() ?: error("Missing YAML value: $key")
    private fun Map<String, Any>.int(key: String) = string(key).toInt()
    private fun Map<String, Any>.optionalInt(key: String) = get(key)?.toString()?.toInt()
    private fun Map<String, Any>.long(key: String) = string(key).toLong()
    private fun Map<String, Any>.double(key: String) = string(key).toDouble()
    private fun Map<String, Any>.bool(key: String) = string(key).toBooleanStrict()
}

fun buildRuns(experiment: ExperimentSettings, matrix: ParameterMatrix): List<RunSpec> {
    val runs = mutableListOf<RunSpec>()
    val readScenarios = matrix.readPreferences.flatMap { preference ->
        matrix.readConcerns.mapNotNull { concern ->
            val excluded = matrix.excludedCombinations.filter {
                it.keys.all { key -> key == "readPreference" || key == "readConcern" }
            }.any { exclusion ->
                exclusion.all { (key, value) ->
                    (key == "readPreference" && value == preference) || (key == "readConcern" && value == concern)
                }
            }
            if (excluded) null else ReadScenario("$preference-$concern", preference, concern)
        }
    }
    for (nodes in matrix.topologies) for (network in matrix.networkProfiles) for (concurrency in matrix.concurrency) {
        for (repetition in 1..experiment.repetitions) {
            if (Suite.PERFORMANCE in experiment.suites) matrix.writeConcerns.forEach { write ->
                readScenarios.forEach { read ->
                    runs += spec(experiment, Suite.PERFORMANCE, nodes, network, write, read, concurrency, repetition)
                }
            }
            if (Suite.FRESHNESS in experiment.suites) matrix.writeConcerns.forEach { write ->
                readScenarios.forEach { read ->
                    runs += spec(experiment, Suite.FRESHNESS, nodes, network, write, read, concurrency, repetition)
                }
            }
            if (Suite.SUPPLEMENTARY in experiment.suites) matrix.writeConcerns.forEach { write ->
                readScenarios.forEach { read ->
                    matrix.maxStalenessSeconds.forEach { maxStaleness ->
                        matrix.enableFlowControl.forEach { flowControl ->
                            matrix.flowControlTargetLagSeconds.forEach { targetLag ->
                                matrix.chainingAllowed.forEach { chaining ->
                                    val variant = SupplementaryVariant(
                                        name = supplementaryName(maxStaleness, flowControl, targetLag, chaining),
                                        maxStalenessSeconds = maxStaleness,
                                        enableFlowControl = flowControl,
                                        flowControlTargetLagSeconds = targetLag,
                                        chainingAllowed = chaining
                                    )
                                    if (!isExcluded(matrix.excludedCombinations, read, variant)) {
                                        runs += spec(experiment, Suite.SUPPLEMENTARY, nodes, network, write, read, concurrency, repetition, variant)
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    val rng = kotlin.random.Random(experiment.randomSeed)
    return runs.groupBy { it.dataNodes }.toSortedMap().flatMap { (_, topologyRuns) ->
        topologyRuns.groupBy { it.network.name }.values.let { groups ->
            val orderedGroups = if (experiment.randomizeOrder) groups.shuffled(rng) else groups
            orderedGroups.flatMap { if (experiment.randomizeOrder) it.shuffled(rng) else it }
        }
    }
}

private fun supplementaryName(maxStaleness: Int?, flowControl: Boolean, targetLag: Int?, chaining: Boolean) =
    "ms-${maxStaleness ?: "off"}-fc-${if (flowControl) "on" else "off"}-lag-${targetLag ?: "na"}-chain-${if (chaining) "on" else "off"}"

private fun isExcluded(
    exclusions: List<Map<String, String>>, read: ReadScenario, variant: SupplementaryVariant
): Boolean = exclusions.any { exclusion ->
    exclusion.all { (key, value) ->
        when (key) {
            "readPreference" -> read.preference == value
            "readConcern" -> read.concern == value
            "maxStalenessSeconds" -> (variant.maxStalenessSeconds ?: -1).toString() == value
            "enableFlowControl" -> variant.enableFlowControl.toString() == value
            "flowControlTargetLagSeconds" -> (variant.flowControlTargetLagSeconds ?: -1).toString() == value
            "chainingAllowed" -> variant.chainingAllowed.toString() == value
            else -> false
        }
    }
}

private fun spec(
    experiment: ExperimentSettings, suite: Suite, nodes: Int, network: NetworkProfile,
    write: String?, read: ReadScenario?, concurrency: Int, repetition: Int,
    supplementary: SupplementaryVariant? = null
) = RunSpec(
    runId = UUID.randomUUID().toString(), suite = suite, dataNodes = nodes, network = network,
    writeConcern = write, readScenario = read, supplementaryVariant = supplementary,
    concurrency = concurrency, repetition = repetition,
    durationSeconds = experiment.durationSeconds, warmupSeconds = experiment.warmupSeconds,
    randomSeed = experiment.randomSeed, dataset = experiment.dataset, workload = experiment.workload,
    outputDirectory = experiment.outputDirectory, measuredRttMs = 0.0
)

fun RunSpec.toDocument(): Document = Document()
    .append("runId", runId).append("suite", suite.name).append("dataNodes", dataNodes)
    .append("network", Document("name", network.name).append("latencyMs", network.latencyMs).append("jitterMs", network.jitterMs))
    .append("writeConcern", writeConcern)
    .append("readScenario", readScenario?.let { Document("name", it.name).append("preference", it.preference).append("concern", it.concern) })
    .append("supplementaryVariant", supplementaryVariant?.let {
        Document("name", it.name).append("maxStalenessSeconds", it.maxStalenessSeconds)
            .append("enableFlowControl", it.enableFlowControl)
            .append("flowControlTargetLagSeconds", it.flowControlTargetLagSeconds)
            .append("chainingAllowed", it.chainingAllowed)
    })
    .append("concurrency", concurrency).append("repetition", repetition)
    .append("durationSeconds", durationSeconds).append("warmupSeconds", warmupSeconds)
    .append("randomSeed", randomSeed).append("outputDirectory", outputDirectory)
    .append("measuredRttMs", measuredRttMs)
    .append("workload", Document("readRatio", workload.readRatio).append("insertRatio", workload.insertRatio).append("updateRatio", workload.updateRatio))
    .append("dataset", Document("initialDocuments", dataset.initialDocuments)
        .append("documentSizeBytes", dataset.documentSizeBytes).append("batchSize", dataset.batchSize)
        .append("registerCount", dataset.registerCount))

fun runSpecFromJson(json: String): RunSpec {
    val d = Document.parse(json)
    val n = d.get("network", Document::class.java)
    val r = d.get("readScenario", Document::class.java)
    val supplementary = d.get("supplementaryVariant", Document::class.java)
    val ds = d.get("dataset", Document::class.java)
    val workload = d.get("workload", Document::class.java)
    return RunSpec(
        runId = d.getString("runId"), suite = Suite.valueOf(d.getString("suite")), dataNodes = d.getInteger("dataNodes"),
        network = NetworkProfile(n.getString("name"), n.getInteger("latencyMs"), n.getInteger("jitterMs")),
        writeConcern = d.getString("writeConcern"),
        readScenario = r?.let { ReadScenario(it.getString("name"), it.getString("preference"), it.getString("concern")) },
        supplementaryVariant = supplementary?.let {
            SupplementaryVariant(
                it.getString("name"), it.get("maxStalenessSeconds")?.toString()?.toInt(),
                it.getBoolean("enableFlowControl"), it.get("flowControlTargetLagSeconds")?.toString()?.toInt(),
                it.getBoolean("chainingAllowed")
            )
        },
        concurrency = d.number("concurrency").toInt(), repetition = d.number("repetition").toInt(),
        durationSeconds = d.number("durationSeconds").toLong(), warmupSeconds = d.number("warmupSeconds").toLong(),
        randomSeed = d.getInteger("randomSeed"), outputDirectory = d.getString("outputDirectory"),
        measuredRttMs = d.number("measuredRttMs").toDouble(),
        dataset = DatasetSettings(ds.getInteger("initialDocuments"), ds.getInteger("documentSizeBytes"), ds.getInteger("batchSize"), ds.getInteger("registerCount")),
        workload = WorkloadSettings(workload.number("readRatio").toDouble(), workload.number("insertRatio").toDouble(), workload.number("updateRatio").toDouble())
    )
}

private fun Document.number(key: String): Number = get(key) as? Number ?: error("Missing numeric JSON value: $key")
