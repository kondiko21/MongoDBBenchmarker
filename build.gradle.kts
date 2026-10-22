plugins {
    application
    kotlin("jvm") version "2.4.0"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.mongodb:mongodb-driver-sync:5.6.0")
    implementation("org.yaml:snakeyaml:2.4")
    implementation("org.slf4j:slf4j-simple:2.0.17")
    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(17)
}

application {
    mainClass.set("org.example.MainKt")
}

val fatJar = tasks.register<Jar>("fatJar") {
    archiveBaseName.set("mongo-benchmark")
    archiveVersion.set("")
    archiveClassifier.set("all")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    manifest { attributes["Main-Class"] = "org.example.MainKt" }
    from(sourceSets.main.get().output)
    dependsOn(configurations.runtimeClasspath)
    from({ configurations.runtimeClasspath.get().filter { it.exists() }.map { zipTree(it) } })
}

tasks.named<JavaExec>("run") {
    dependsOn(fatJar)
}

tasks.test {
    useJUnitPlatform()
}
