plugins {
    id("com.diffplug.spotless") version "6.25.0" apply false
}

allprojects {
    group = "com.cryptopanner"
    version = "0.1.0-SNAPSHOT"
}

subprojects {
    apply(plugin = "java")
    apply(plugin = "com.diffplug.spotless")

    repositories {
        mavenCentral()
    }

    configure<JavaPluginExtension> {
        toolchain.languageVersion.set(JavaLanguageVersion.of(21))
    }

    configure<com.diffplug.gradle.spotless.SpotlessExtension> {
        java {
            googleJavaFormat("1.23.0")
            // Intentionally do NOT call removeUnusedImports() —
            // google-java-format already removes unused imports
            // (matches CryptoLake parent convention).
            target("src/**/*.java")
        }
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
    }
}
