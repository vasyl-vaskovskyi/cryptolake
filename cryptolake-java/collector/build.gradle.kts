// cryptolake-java/collector/build.gradle.kts
plugins {
    application
}

application {
    mainClass.set("com.cryptolake.collector.Main")
}

dependencies {
    implementation(project(":common"))
    implementation(libs.kafka.clients)
}

tasks.register("dumpMetricSkeleton") {
    group = "port"
    doLast {
        println("stub: implement after Main exists; placeholder for gate4")
    }
}

tasks.register("runRawTextParity") {
    group = "port"
    description = "Replay fixtures through capture path and compare raw_text/raw_sha256."
    doLast {
        println("stub: implement after capture path exists; placeholder for gate3")
    }
}
