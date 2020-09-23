import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    id("org.springframework.boot") version "2.3.4.RELEASE" apply false
    id("io.spring.dependency-management") version "1.0.10.RELEASE"
    kotlin("jvm") version "1.3.61"
    `maven-publish`
    id("net.researchgate.release") version "2.8.1"
    jacoco
    kotlin("plugin.spring") version "1.3.72"
}

group = "no.ruter.sb.grunnplattform.commons"

java.sourceCompatibility = JavaVersion.VERSION_1_8

repositories {
    mavenCentral()
    maven(url = "https://packages.confluent.io/maven/")
    maven(url = "https://nexus.dev.transhub.io/nexus/content/repositories/releases")
}

dependencyManagement {
    imports {
        mavenBom(org.springframework.boot.gradle.plugin.SpringBootPlugin.BOM_COORDINATES)
    }
}

// NB: Make sure dependencies that should be transitively included in consumers uses api and that internal dependencies uses compileOnly
dependencies {
    val kafkaVersion = "2.5.1"

    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

    // kafka
    implementation("org.apache.kafka:kafka-clients:$kafkaVersion")

    // spring
    implementation("org.springframework.boot:spring-boot-starter")

    // micrometer
    implementation("io.micrometer:micrometer-core")

    testImplementation("org.springframework.boot:spring-boot-test")
    testImplementation("org.springframework.boot:spring-boot-starter-web")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    testImplementation("io.mockk:mockk:1.10.0")
    testImplementation("org.awaitility:awaitility:4.0.2")
    testImplementation("org.assertj:assertj-core")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

jacoco {
    toolVersion = "0.8.5"
    reportsDir = file("$buildDir/reports/jacoco")
}

tasks.withType<JacocoReport> {
    reports {
        xml.isEnabled = true
        html.isEnabled = true
        csv.isEnabled = false
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
//    testLogging {
//        showCauses = true
//        showExceptions = true
//        showStackTraces = true
//        showStandardStreams = true
//    }
//    reports.html.isEnabled = false
//    reports.junitXml.isEnabled = true
//    finalizedBy("jacocoTestReport")
}

val compileKotlin: KotlinCompile by tasks
val compileTestKotlin: KotlinCompile by tasks

compileKotlin.kotlinOptions {
    freeCompilerArgs = listOf("-Xjsr305=strict")
    jvmTarget = "1.8"
}

compileTestKotlin.kotlinOptions {
    freeCompilerArgs = listOf("-Xjsr305=strict")
    jvmTarget = "1.8"
}

val sourcesJar by tasks.registering(Jar::class) {
    archiveClassifier.set("sources")
    from(sourceSets.main.get().allSource)
}

publishing {
    publications {
        register("mavenJava", MavenPublication::class.java) {
            from(components["java"])
            artifact(sourcesJar.get())
        }
    }
    repositories {
        maven {
            val baseUrl = System.getenv("NEXUS_URL")
            val releasesUrl = "$baseUrl/releases"
            val snapshotsUrl = "$baseUrl/snapshots"
            url = uri(if (version.toString().endsWith("SNAPSHOT")) snapshotsUrl else releasesUrl).also {
                println(it)
            }

            credentials {
                username = System.getenv("MAVEN_USER")
                password = System.getenv("MAVEN_PASSWORD")
            }
        }
    }
}

tasks {
    "afterReleaseBuild" {
        dependsOn(publish)
    }
}
