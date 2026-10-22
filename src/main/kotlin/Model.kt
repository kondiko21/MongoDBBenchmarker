package org.example

enum class Suite { PERFORMANCE, FRESHNESS, SUPPLEMENTARY }

data class NetworkProfile(val name: String, val latencyMs: Int, val jitterMs: Int)

data class ReadScenario(val name: String, val preference: String, val concern: String)

data class SupplementaryVariant(
    val name: String,
    val maxStalenessSeconds: Int?,
    val enableFlowControl: Boolean,
    val flowControlTargetLagSeconds: Int?,
    val chainingAllowed: Boolean
)

data class DatasetSettings(
    val initialDocuments: Int,
    val documentSizeBytes: Int,
    val batchSize: Int,
    val registerCount: Int
)

data class WorkloadSettings(
    val readRatio: Double,
    val insertRatio: Double,
    val updateRatio: Double
)

data class MongoSettings(
    val wiredTigerCacheSizeGB: Double,
    val imageVersion: String,
    val readinessTimeoutSeconds: Int
)

data class ExperimentSettings(
    val durationSeconds: Long,
    val warmupSeconds: Long,
    val repetitions: Int,
    val randomizeOrder: Boolean,
    val randomSeed: Int,
    val suites: Set<Suite>,
    val outputDirectory: String,
    val dataset: DatasetSettings,
    val workload: WorkloadSettings,
    val mongo: MongoSettings
)

data class ParameterMatrix(
    val topologies: List<Int>,
    val networkProfiles: List<NetworkProfile>,
    val writeConcerns: List<String>,
    val readConcerns: List<String>,
    val readPreferences: List<String>,
    val excludedCombinations: List<Map<String, String>>,
    val maxStalenessSeconds: List<Int?>,
    val enableFlowControl: List<Boolean>,
    val flowControlTargetLagSeconds: List<Int?>,
    val chainingAllowed: List<Boolean>,
    val concurrency: List<Int>
)

data class RunSpec(
    val runId: String,
    val suite: Suite,
    val dataNodes: Int,
    val network: NetworkProfile,
    val writeConcern: String?,
    val readScenario: ReadScenario?,
    val supplementaryVariant: SupplementaryVariant?,
    val concurrency: Int,
    val repetition: Int,
    val durationSeconds: Long,
    val warmupSeconds: Long,
    val randomSeed: Int,
    val dataset: DatasetSettings,
    val workload: WorkloadSettings,
    val outputDirectory: String,
    val measuredRttMs: Double
) {
    val label: String
        get() = listOfNotNull(
            suite.name.lowercase(), "n$dataNodes", network.name,
            writeConcern?.let { "w-$it" }, readScenario?.name,
            supplementaryVariant?.name,
            "c$concurrency", "r$repetition"
        ).joinToString("-")
}
