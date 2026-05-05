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
