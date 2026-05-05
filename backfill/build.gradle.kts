// backfill/build.gradle.kts
plugins {
    application
}

application {
    mainClass.set("com.cryptolake.backfill.Main")
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
}
