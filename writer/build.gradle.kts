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
    testImplementation(libs.mockito.core)
}
