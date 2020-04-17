import java.time.LocalDate

group = "com.github.f0xdx"
version = "0.1-SNAPSHOT"

val junitVersion by extra("5.6.2")
val kafkaVersion by extra("2.3.1")
val lombokVersion by extra("1.18.12")

plugins {
  java
  jacoco
  id("com.diffplug.gradle.spotless") version "3.28.0"
  id("org.sonarqube") version "2.8"
}

repositories {
  mavenCentral()
}

dependencies {
  // bom
  implementation(platform("org.junit:junit-bom:${junitVersion}"))

  // annotation processors
  annotationProcessor("org.projectlombok:lombok:$lombokVersion")

  // implementation
  compileOnly("org.projectlombok:lombok:$lombokVersion")
  compileOnly("org.apache.kafka:connect-api:${kafkaVersion}")
  implementation("org.apache.kafka:connect-transforms:${kafkaVersion}")

  // test

  testAnnotationProcessor("org.projectlombok:lombok:$lombokVersion")
  testCompileOnly("org.projectlombok:lombok:$lombokVersion")
  testImplementation("org.apache.kafka:connect-api:${kafkaVersion}")
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

configure<JavaPluginConvention> {
  sourceCompatibility = JavaVersion.VERSION_1_8
}

tasks.test {
  useJUnitPlatform()
}

tasks.jacocoTestReport {
  reports {
    xml.isEnabled = true
    csv.isEnabled = false
  }
}

tasks.sonarqube {
  dependsOn(tasks.jacocoTestReport)
}

java {
  sourceCompatibility = JavaVersion.VERSION_1_8
  targetCompatibility = JavaVersion.VERSION_1_8
  withSourcesJar()
  withJavadocJar()
}

spotless {
  java {
    googleJavaFormat()
    licenseHeaderFile(file("$rootDir/spotless/apache-license-2.0.java.comment"))
    trimTrailingWhitespace()
    endWithNewline()
  }
  encoding("UTF-8")
}

sonarqube {
  properties {
    property("sonar.projectKey", "f0xdx_kafka-connect-wrap-smt")
    property("sonar.organization", "f0xdx")
    property("sonar.host.url", "https://sonarcloud.io")
    property("sonar.login", System.getenv("SONAR_TOKEN"))
  }
}

tasks.register<Zip>("confluent_hub_archive") {
  destinationDirectory.set(file("${buildDir}/dist"))
  archiveFileName.set("f0xdx-${rootProject.name}-${rootProject.version}.zip")

  from("manifest.json") {
    expand(
        "name" to rootProject.name,
        "version" to rootProject.version,
        "build_date" to LocalDate.now()
    )
  }
  from("README.md") {
    into("doc")
  }
  from("LICENSE") {
    into("doc")
  }
  from(tasks.javadoc) {
    into("doc/javadoc")
  }
  from(tasks.jar) {
    into("lib")
  }
}