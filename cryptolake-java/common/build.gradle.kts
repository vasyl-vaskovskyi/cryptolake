// cryptolake-java/common/build.gradle.kts
plugins {
    `java-library`
}

dependencies {
    api(libs.slf4j.api)
    api(libs.jackson.databind)
    api(libs.jackson.yaml)
    api(libs.micrometer.prometheus)
    implementation(libs.logback.classic)
    implementation(libs.logstash.encoder)
}
