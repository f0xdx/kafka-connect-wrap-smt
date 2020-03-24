[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=f0xdx_kafka-connect-wrap-smt&metric=alert_status)](https://sonarcloud.io/dashboard?id=f0xdx_kafka-connect-wrap-smt)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=f0xdx_kafka-connect-wrap-smt&metric=coverage)](https://sonarcloud.io/dashboard?id=f0xdx_kafka-connect-wrap-smt)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=f0xdx_kafka-connect-wrap-smt&metric=reliability_rating)](https://sonarcloud.io/dashboard?id=f0xdx_kafka-connect-wrap-smt)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=f0xdx_kafka-connect-wrap-smt&metric=security_rating)](https://sonarcloud.io/dashboard?id=f0xdx_kafka-connect-wrap-smt)

# kafka-connect-wrap-smt

The *kafka-connect-wrap-smt* is a single message transform (SMT) that wraps key and record of kafka
messages into a single struct. This ensures, e.g., that data contained in complex keys is not lost
when ingesting data from kafka in a sink such as elasticsearch. Additionally, it supports exporting
meta-data such as partition, offset, timestamp, topic and kafka headers.

## Install

TODO: describe build and deployment as connect plugin
TODO: add curl snippet to download jar and check checksum

## TODO

 * configuration docs
 * doc string
 * handling w/ schema
 * handling w/o schema
 * caching of schema creation
 * options and config (meta-data)
 * kafka header export into schema / schemaless