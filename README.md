![CI](https://github.com/f0xdx/kafka-connect-wrap-smt/workflows/CI/badge.svg)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=f0xdx_kafka-connect-wrap-smt&metric=alert_status)](https://sonarcloud.io/dashboard?id=f0xdx_kafka-connect-wrap-smt)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=f0xdx_kafka-connect-wrap-smt&metric=coverage)](https://sonarcloud.io/dashboard?id=f0xdx_kafka-connect-wrap-smt)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=f0xdx_kafka-connect-wrap-smt&metric=reliability_rating)](https://sonarcloud.io/dashboard?id=f0xdx_kafka-connect-wrap-smt)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=f0xdx_kafka-connect-wrap-smt&metric=security_rating)](https://sonarcloud.io/dashboard?id=f0xdx_kafka-connect-wrap-smt)
![GitHub](https://img.shields.io/github/license/f0xdx/kafka-connect-wrap-smt?color=00aa00)

# kafka-connect-wrap-smt

The *kafka-connect-wrap-smt* is a [single message transform (SMT)](https://docs.confluent.io/current/connect/transforms/index.html)
that wraps key and record of kafka messages into a single struct. This ensures, e.g., that data
contained in complex keys is not lost when ingesting data from kafka in a sink such as
elasticsearch. Additionally, it supports exporting meta-data including partition, offset, timestamp,
topic name and kafka headers.

Note that *kafka-connect-wrap-smt* does only support sink connectors, as it wraps kafka specific
meta-data that is not available for all source connectors.

## Install

To install the latest release, you can download the plugin binaries directly from github or build
them from source (see section *Build* below):

```shell script
curl -sLJO https://github.com/f0xdx/kafka-connect-wrap-smt/releases/download/v0.1.0/kafka-connect-wrap-smt-0.1.0.jar
cp kafka-connect-wrap-smt-0.1-SNAPSHOT.jar connect/plugin/folder
```

Make sure that the plugin folder is picked up by kafka connect by verifying its logs. For instance,
with `docker-compose`, you could run `docker-compose logs connect | grep Wrap` which should show
relevant logs, e.g.,

```shell script
connect            | [2020-03-25 12:48:00,429] INFO Added plugin 'com.github.f0xdx.Wrap' (org.apache.kafka.connect.runtime.isolation.DelegatingClassLoader)
connect            | [2020-03-25 12:48:01,463] INFO Added alias 'Wrap' to plugin 'com.github.f0xdx.Wrap' (org.apache.kafka.connect.runtime.isolation.DelegatingClassLoader)
```

## Configuration

After installing the plugin, you can configure your connector to apply the SMT, e.g.:

```json
{
  "transforms": "wrap",
  "transforms.wrap.type": "Wrap",
  "transforms.wrap.include.headers": false
}
```

As stated above, this SMT can only be used in conjunction with sink connectors.

## Build

To build this project locally simply run:

```shell script
git clone git@github.com:f0xdx/kafka-connect-wrap-smt.git
./gradlew build
```

After building, you can deploy the `build/libs/kafka-connect-wrap-smt-0.1-SNAPSHOT.jar` into the
plugins folder of your kafka connect instance, e.g.:

```shell script
cp build/libs/kafka-connect-wrap-smt-0.1-SNAPSHOT.jar connect/plugin/folder
```

## Roadmap

Upcoming features are:

 * CI/CD and automated publishing using github packages
 * install script + docker image for ready made connect
 * typed kafka header export
 * schema based export with schema-less keys (if key schema can be derived)