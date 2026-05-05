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
