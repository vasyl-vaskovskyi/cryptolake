// common/build.gradle.kts
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
    // Hibernate Validator + Jakarta EE validation API (Tier 5 J3; design §5)
    api(libs.hibernate.validator)
    // Expression Language implementation required by Hibernate Validator
    implementation("org.glassfish:jakarta.el:4.0.2")
}
