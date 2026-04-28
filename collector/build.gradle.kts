// collector/build.gradle.kts
plugins {
    application
}

application {
    mainClass.set("com.cryptolake.collector.Main")
}

dependencies {
    implementation(project(":common"))
    implementation(libs.kafka.clients)
    implementation(libs.micrometer.prometheus)
    implementation(libs.logback.classic)
    implementation(libs.logstash.encoder)
    implementation(libs.slf4j.api)
    implementation(libs.postgresql)
    implementation(libs.hikaricp)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.kafka)
}

tasks.register<JavaExec>("dumpMetricSkeleton") {
    group = "port"
    description = "Scrape Java collector's Prometheus registry -> build/metrics-skeleton.txt (app-level only)."
    dependsOn("compileJava")
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.cryptolake.collector.harness.MetricSkeletonDump")
    val outFile = layout.buildDirectory.file("metrics-skeleton.txt")
    args(outFile.get().asFile.absolutePath)
    outputs.file(outFile)
}

tasks.register<JavaExec>("runRawTextParity") {
    group = "port"
    description = "Replay fixtures through capture path; compare raw_text/raw_sha256."
    dependsOn("compileJava")
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.cryptolake.collector.harness.RawTextParityHarness")
    args(
        rootProject.file("parity-fixtures/websocket-frames").absolutePath,
        layout.buildDirectory.file("reports/gate3-parity.txt").get().asFile.absolutePath,
    )
}
