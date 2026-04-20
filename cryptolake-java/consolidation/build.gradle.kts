// cryptolake-java/consolidation/build.gradle.kts
plugins {
    application
}

application {
    mainClass.set("com.cryptolake.consolidation.Main")
}

dependencies {
    implementation(project(":common"))
    implementation(libs.picocli)
}
