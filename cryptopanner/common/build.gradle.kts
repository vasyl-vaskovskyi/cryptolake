plugins {
    `java-library`
}

dependencies {
    api(libs.jackson.databind)
    api(libs.jackson.yaml)
    api(libs.slf4j.api)
    api(libs.snakeyaml)
    api(libs.zstd)

    testImplementation(libs.junit.jupiter)
}
