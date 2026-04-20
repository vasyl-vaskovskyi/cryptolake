// cryptolake-java/build.gradle.kts
plugins {
    alias(libs.plugins.spotless) apply false
}

allprojects {
    group = "com.cryptolake"
    version = "0.1.0"
    repositories { mavenCentral() }
}

subprojects {
    apply(plugin = "java")
    apply(plugin = "com.diffplug.spotless")

    extensions.configure<JavaPluginExtension> {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(21))
        }
    }

    // Use string-coordinate form instead of the `libs` type-safe accessor:
    // the version catalog accessor isn't available inside `subprojects { ... }`
    // blocks of the root build. Test deps are common to every subproject so
    // we declare them here once rather than duplicating per build file.
    dependencies {
        "testImplementation"(platform("org.junit:junit-bom:5.11.0"))
        "testImplementation"("org.junit.jupiter:junit-jupiter")
        "testImplementation"("org.assertj:assertj-core:3.26.3")
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
    }

    extensions.configure<com.diffplug.gradle.spotless.SpotlessExtension> {
        java {
            googleJavaFormat("1.23.0")
            removeUnusedImports()
            endWithNewline()
        }
    }
}
