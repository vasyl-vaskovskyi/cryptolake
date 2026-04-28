// consolidation/build.gradle.kts
plugins {
    application
}

application {
    mainClass.set("com.cryptolake.consolidation.Main")
}

dependencies {
    implementation(project(":common"))
    implementation(project(":verify"))
    implementation(libs.picocli)
    implementation(libs.zstd.jni)
    implementation(libs.jackson.databind)
    implementation(libs.micrometer.prometheus)
    implementation(libs.slf4j.api)
    implementation(libs.logback.classic)
    implementation(libs.logstash.encoder)
    implementation(libs.commons.compress)
}

tasks.register<JavaExec>("dumpMetricSkeleton") {
    group = "port"
    description = "Scrape Java consolidation scheduler Prometheus registry -> build/metrics-skeleton.txt."
    dependsOn("compileJava")
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.cryptolake.consolidation.harness.MetricSkeletonDump")
    val outFile = layout.buildDirectory.file("metrics-skeleton.txt")
    args(outFile.get().asFile.absolutePath)
    outputs.file(outFile)
}
