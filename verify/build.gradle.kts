// verify/build.gradle.kts
plugins {
    application
}

application {
    mainClass.set("com.cryptolake.verify.Main")
}

dependencies {
    implementation(project(":common"))
    implementation(libs.picocli)
    implementation(libs.zstd.jni)
    implementation(libs.jackson.databind)
    implementation(libs.postgresql)
    implementation(libs.slf4j.api)
    implementation(libs.logback.classic)
    implementation(libs.logstash.encoder)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.postgres)
}

tasks.register<JavaExec>("runVerifyParity") {
    group = "port"
    description = "Run Java verify against Python-produced archive corpus; diff stdout byte-for-byte."
    dependsOn("compileJava")
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.cryptolake.verify.harness.VerifyStdoutParityHarness")
    args(
        rootProject.file("parity-fixtures/verify/archive").absolutePath,
        rootProject.file("parity-fixtures/verify/expected.txt").absolutePath,
        layout.buildDirectory.file("reports/gate5-verify.txt").get().asFile.absolutePath,
    )
}
