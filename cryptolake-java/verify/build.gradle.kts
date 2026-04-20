// cryptolake-java/verify/build.gradle.kts
plugins {
    application
}

application {
    mainClass.set("com.cryptolake.verify.Main")
}

dependencies {
    implementation(project(":common"))
    implementation(libs.picocli)
}
