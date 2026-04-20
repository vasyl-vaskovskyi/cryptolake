// cryptolake-java/backfill/build.gradle.kts
plugins {
    application
}

application {
    mainClass.set("com.cryptolake.backfill.Main")
}

dependencies {
    implementation(project(":common"))
    implementation(libs.picocli)
}
