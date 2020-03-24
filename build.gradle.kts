group = "com.github.f0xdx"
version = "0.1-SNAPSHOT"

plugins {
  java
  jacoco
  id("com.diffplug.gradle.spotless") version "3.28.0"
  id("org.sonarqube") version "2.8"
}

java {
  sourceCompatibility = JavaVersion.VERSION_1_8
  targetCompatibility = JavaVersion.VERSION_1_8
  withSourcesJar()
  withJavadocJar()
}

repositories {
  mavenCentral()
}

val junitVersion by extra("5.6.1")
val kafkaVersion by extra("2.3.1")
val lombokVersion by extra("1.18.12")

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