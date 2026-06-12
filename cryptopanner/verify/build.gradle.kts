plugins {
    application
}

application {
    mainClass.set("com.cryptopanner.verify.Main")
}

dependencies {
    implementation(project(":common"))
    implementation(libs.picocli)
    runtimeOnly(libs.slf4j.simple)

    testImplementation(libs.junit.jupiter)
}
