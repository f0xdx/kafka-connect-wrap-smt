group = "com.github.f0xdx"
version = "1.0-SNAPSHOT"

plugins {
  java
}

repositories {
  mavenCentral()
}

val junitVersion by extra("5.6.0")
val kafkaVersion by extra("2.4.0")
val lombokVersion by extra("1.18.10")

dependencies {
  // bom
  implementation(platform("org.junit:junit-bom:${junitVersion}"))

  // annotation processors
  annotationProcessor("org.projectlombok:lombok:$lombokVersion")

  // implementation
  compileOnly("org.projectlombok:lombok:$lombokVersion")
  compileOnly("org.apache.kafka:connect-api:${kafkaVersion}")
  compileOnly("org.apache.kafka:connect-transforms:${kafkaVersion}")

  // test
  testImplementation("org.apache.kafka:connect-api:${kafkaVersion}")
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

configure<JavaPluginConvention> {
  sourceCompatibility = JavaVersion.VERSION_1_8
}

tasks.test {
  useJUnitPlatform()
}