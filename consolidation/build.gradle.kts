// consolidation/build.gradle.kts
import java.time.Duration

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

// Exclude @Tag("chaos") from the default :test task (full chaos run is ~70 min
// and requires docker). Run chaos via the :chaosTest task or
// scripts/run-chaos-tests.sh.
tasks.test {
    useJUnitPlatform {
        excludeTags("chaos")
    }
}

tasks.register<Test>("chaosTest") {
    group = "verification"
    description = "Run @Tag(\"chaos\") scenarios via ChaosVerifyIT (~70 min, needs docker)."
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    useJUnitPlatform {
        includeTags("chaos")
    }
    timeout.set(Duration.ofHours(2))
    outputs.upToDateWhen { false }
}
