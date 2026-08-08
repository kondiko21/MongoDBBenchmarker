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
    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(17)
}

application {
    mainClass.set("org.example.MainKt")
}

tasks.test {
    useJUnitPlatform()
}
