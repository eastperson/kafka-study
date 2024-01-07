plugins {
    id("java")
}

group = "org.example"
version = "unspecified"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.kafka:kafka-clients:3.1.0")
    implementation("org.slf4j:slf4j-api:1.7.36")
    implementation("ch.qos.logback:logback-classic:1.2.6")
    implementation("com.github.javafaker:javafaker:1.0.2")
    testImplementation("org.slf4j:slf4j-simple:1.7.36")
    implementation("org.postgresql:postgresql:42.4.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.13.3")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.13.3")
}

tasks.test {
    useJUnitPlatform()
}