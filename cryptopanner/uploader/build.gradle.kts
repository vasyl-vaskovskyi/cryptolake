plugins {
    application
}

application {
    mainClass.set("com.cryptopanner.uploader.Main")
}

dependencies {
    implementation(project(":common"))
    implementation(libs.aws.s3)
    runtimeOnly(libs.slf4j.simple)

    testImplementation(libs.junit.jupiter)
}
