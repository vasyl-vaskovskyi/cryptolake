plugins {
    application
}

application {
    mainClass.set("com.cryptopanner.sealer.Main")
}

dependencies {
    implementation(project(":common"))
    runtimeOnly(libs.slf4j.simple)

    testImplementation(libs.junit.jupiter)
}
