// writer/build.gradle.kts
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

tasks.register<JavaExec>("dumpMetricSkeleton") {
    group = "port"
    description = "Scrape Java writer's Prometheus registry → build/metrics-skeleton.txt (app-level only)."
    dependsOn("compileJava")
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.cryptolake.writer.harness.MetricSkeletonDump")
    val outFile = layout.buildDirectory.file("metrics-skeleton.txt")
    args(outFile.get().asFile.absolutePath)
    outputs.file(outFile)
}

tasks.register<JavaExec>("produceSyntheticArchives") {
    group = "port"
    description = "Produce sample Java-writer archives for gate 5 verify-parity check."
    dependsOn("compileJava")
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.cryptolake.writer.harness.SyntheticArchiveHarness")
    val outDir = layout.buildDirectory.dir("synthetic-archives")
    args(outDir.get().asFile.absolutePath)
    outputs.dir(outDir)
}
