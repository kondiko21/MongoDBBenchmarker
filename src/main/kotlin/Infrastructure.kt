package org.example

import java.io.File

class CommandRunner(private val workDir: File) {
    fun run(vararg command: String, quiet: Boolean = false): String {
        val process = ProcessBuilder(*command).directory(workDir).redirectErrorStream(true).start()
        val output = process.inputStream.bufferedReader().readText()
        val exit = process.waitFor()
        check(exit == 0) { "Command failed (${command.joinToString(" ")}):\n$output" }
        if (!quiet && output.isNotBlank()) println(output.trim())
        return output
    }
}

class MongoInfrastructure(
    private val root: File,
    private val mongo: MongoSettings
) {
    private val runner = CommandRunner(root)
    private var topology: Int? = null
    private var network: NetworkProfile? = null

    fun prepare(dataNodes: Int, profile: NetworkProfile): Double {
        if (topology != dataNodes || !containersRunning(dataNodes)) {
            recreate(dataNodes)
            topology = dataNodes
            network = null
        }
        if (network != profile) {
            applyNetwork(dataNodes, profile)
            network = profile
        }
        assertHealthy(dataNodes)
        return measureRtt()
    }

    fun runWorker(specFileName: String) {
        runner.run(
            "docker", "compose", "--profile", "worker", "run", "--rm", "--no-deps", "benchmark",
            "java", "-jar", "/app/benchmark.jar", "--worker=/work/$specFileName",
            quiet = false
        )
    }

    fun applySupplementary(spec: RunSpec) {
        val variant = spec.supplementaryVariant ?: return
        for (node in 1..spec.dataNodes) {
            val target = variant.flowControlTargetLagSeconds?.let { ",flowControlTargetLagSeconds:$it" } ?: ""
            runner.run(
                "docker", "exec", "mongo$node", "mongosh", "--quiet", "--eval",
                "db.adminCommand({setParameter:1,enableFlowControl:${variant.enableFlowControl}$target})",
                quiet = true
            )
        }
        runner.run(
            "docker", "exec", "mongo1", "mongosh", "--quiet", "--eval",
            "const c=rs.conf(); c.settings=c.settings||{}; c.settings.chainingAllowed=${variant.chainingAllowed}; rs.reconfig(c)",
            quiet = true
        )
        assertHealthy(spec.dataNodes)
    }

    private fun recreate(dataNodes: Int) {
        File(root, ".env").writeText("MONGO_WT_CACHE_GB=${mongo.wiredTigerCacheSizeGB}\n")
        runner.run("docker", "compose", "--profile", "five-nodes", "down", "--remove-orphans", quiet = true)
        val services = (1..dataNodes).map { "mongo$it" }
        val command = mutableListOf("docker", "compose")
        if (dataNodes == 5) command += listOf("--profile", "five-nodes")
        command += listOf("up", "-d", "--build")
        command += services
        runner.run(*command.toTypedArray(), quiet = false)
        waitForMongo(dataNodes)
        initiate(dataNodes)
        assertHealthy(dataNodes)
    }

    private fun waitForMongo(dataNodes: Int) {
        repeat(mongo.readinessTimeoutSeconds) {
            val ready = (1..dataNodes).all { index ->
                runCatching {
                    runner.run("docker", "exec", "mongo$index", "mongosh", "--quiet", "--eval", "db.adminCommand({ping:1}).ok", quiet = true).trim() == "1"
                }.getOrDefault(false)
            }
            if (ready) return
            Thread.sleep(1_000)
        }
        error("MongoDB nodes did not become ready within ${mongo.readinessTimeoutSeconds}s")
    }

    private fun initiate(dataNodes: Int) {
        val members = (1..dataNodes).joinToString(",") { "{_id:${it - 1},host:'mongo$it:27017',priority:${if (it == 1) 2 else 1}}" }
        val script = "try { rs.status() } catch (e) { rs.initiate({_id:'rs0',members:[$members]}) }"
        runner.run("docker", "exec", "mongo1", "mongosh", "--quiet", "--eval", script, quiet = true)
    }

    private fun assertHealthy(dataNodes: Int) {
        repeat(mongo.readinessTimeoutSeconds) {
            val status = runCatching {
                runner.run(
                    "docker", "exec", "mongo1", "mongosh", "--quiet", "--eval",
                    "const s=rs.status(); print(s.members.filter(m=>m.stateStr==='PRIMARY').length+','+s.members.filter(m=>m.stateStr==='SECONDARY').length)",
                    quiet = true
                ).trim()
            }.getOrDefault("")
            if (status == "1,${dataNodes - 1}") return
            Thread.sleep(1_000)
        }
        error("Replica set is not healthy: expected one PRIMARY and ${dataNodes - 1} SECONDARY nodes")
    }

    private fun containersRunning(dataNodes: Int): Boolean = (1..dataNodes).all { index ->
        runCatching {
            runner.run("docker", "inspect", "--format", "{{.State.Running}}", "mongo$index", quiet = true).trim() == "true"
        }.getOrDefault(false)
    }

    private fun applyNetwork(dataNodes: Int, profile: NetworkProfile) {
        for (node in 1..dataNodes) {
            val reset = "tc qdisc del dev eth0 root 2>/dev/null || true"
            runner.run("docker", "exec", "mongo$node", "sh", "-c", reset, quiet = true)
            if (profile.latencyMs == 0 && profile.jitterMs == 0) continue
            val peers = (1..dataNodes).filter { it != node }.joinToString("; ") { peer ->
                "tc filter add dev eth0 protocol ip parent 1:0 prio 3 u32 match ip dst 172.28.0.${10 + peer}/32 flowid 1:3"
            }
            val jitter = if (profile.jitterMs > 0) " ${profile.jitterMs}ms distribution normal" else ""
            val script = "tc qdisc add dev eth0 root handle 1: prio; " +
                "tc qdisc add dev eth0 parent 1:3 handle 30: netem delay ${profile.latencyMs}ms$jitter; $peers"
            runner.run("docker", "exec", "mongo$node", "sh", "-c", script, quiet = true)
        }
    }

    private fun measureRtt(): Double {
        val output = runner.run("docker", "exec", "mongo1", "ping", "-c", "5", "-q", "mongo2", quiet = true)
        val line = output.lineSequence().firstOrNull { it.contains("min/avg/max") }
            ?: error("Could not parse control RTT from ping output:\n$output")
        return line.substringAfter("=").trim().substringBefore(" ms").split("/")[1].toDouble()
    }
}
