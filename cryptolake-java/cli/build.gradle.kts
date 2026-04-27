// cryptolake-java/cli/build.gradle.kts
// Aggregator project: gates 1, 5, 6 run against :cli which delegates to the three subprojects.

plugins {
    // No `application` plugin — this is a pure aggregator.
    java
}

dependencies {
    // Depend on all three CLI subprojects so :cli:test picks up all tests
    // and :cli:check runs all static checks.
    implementation(project(":verify"))
    implementation(project(":consolidation"))
    implementation(project(":backfill"))
    // Writer is a runtimeOnly dep for producing synthetic archives
    runtimeOnly(project(":writer"))
}

// Gate 1: aggregate all three subproject tests
tasks.named("test") {
    dependsOn(
        ":verify:test",
        ":consolidation:test",
        ":backfill:test",
    )
}

// Gate 5: produce synthetic archives (reuse writer's SyntheticArchiveHarness)
tasks.register<JavaExec>("produceSyntheticArchives") {
    group = "port"
    description = "Produce synthetic archives for gate 5 verify-parity check (reuses writer harness)."
    dependsOn(":writer:compileJava", ":verify:compileJava")
    classpath = configurations["runtimeClasspath"] + project(":writer").sourceSets["main"].output
    mainClass.set("com.cryptolake.writer.harness.SyntheticArchiveHarness")
    val outDir = layout.buildDirectory.dir("synthetic-archives")
    args(outDir.get().asFile.absolutePath)
    outputs.dir(outDir)
}

// Gate 6: aggregate static checks from all three subprojects
tasks.named("check") {
    dependsOn(
        ":verify:check",
        ":consolidation:check",
        ":backfill:check",
    )
}
