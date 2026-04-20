// cryptolake-java/writer/build.gradle.kts
plugins {
    application
}

application {
    mainClass.set("com.cryptolake.writer.Main")
}

dependencies {
    implementation(project(":common"))
    implementation(libs.kafka.clients)
    implementation(libs.zstd.jni)
    implementation(libs.postgresql)
    implementation(libs.hikaricp)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.kafka)
    testImplementation(libs.testcontainers.postgres)
}

tasks.register("dumpMetricSkeleton") {
    group = "port"
    description = "Start service in dump mode, scrape /metrics, write canonicalized skeleton."
    doLast {
        println("stub: implement after Main exists; placeholder for gate4")
    }
}

tasks.register("produceSyntheticArchives") {
    group = "port"
    description = "Run integration harness that produces archives for gate5."
    doLast {
        println("stub: implement after Main exists; placeholder for gate5")
    }
}
