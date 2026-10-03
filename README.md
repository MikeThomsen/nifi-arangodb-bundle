## Overview

This bundle adds support for ArangoDB to Apache NiFi. It aims to allow users of both products to full leverage NiFi features like the Record API for efficiently working with ArangoDB.

It provides:

* `ArangoDBClientServiceImpl` — a controller service that hands out configured ArangoDB driver connections.
* `ArangoDBLookupService` — a record lookup service backed by an AQL query, for enriching record sets.
* `PutArangoDBRecord` — writes a record set into a collection, one document per record.
* `QueryArangoDB` — runs AQL for aggregations, updates and deletes.
* `QueryArangoDBRecord` — runs AQL and serializes the result set through a NiFi record writer.

## Requirements

| | Version |
| --- | --- |
| Apache NiFi | 2.12.0 |
| Java | 21 |
| ArangoDB Java driver | 7.28.0 |
| ArangoDB server | 3.12 (tested against `arangodb:3.12.12`) |

## Build Instructions

Install Apache Maven and a JDK 21 or newer, then run `mvn clean install` from the root of the repository to create the NAR file.

The version of NiFi can be adjusted by opening the `pom.xml` file at the root of the repository and changing the property `nifi.version`. Only the NiFi 2.x line is supported; for NiFi 1.x, use release 1.0.3 of this bundle.

### Running the integration tests

The integration tests start a real ArangoDB server using [Testcontainers](https://testcontainers.com/), so they need a
working Docker environment. They are not part of the default build:

```
mvn clean verify -Pintegration-tests
```

The container image is pinned by the `arangodb.docker.image` property in the root `pom.xml` and can be overridden on the
command line, e.g. `-Darangodb.docker.image=arangodb:3.12.11`. A single container is shared by every test class; each
test creates and drops its own database.

## Notes on upgrading from the NiFi 1.x releases

* The **Protocol** property no longer offers VelocyStream (VST) or VelocyPack bodies. VST was removed from the ArangoDB
  server in 3.12, and VelocyPack would require a serializer that this bundle does not package. Existing flows that
  selected one of the old values are migrated automatically to an equivalent HTTP protocol when the flow is loaded.
* The **Password** property on the client service is now marked sensitive, so it is encrypted in the flow configuration.
  Re-enter the password after upgrading.
* The **SSL Context Service** property now identifies NiFi 2.x's `SSLContextProvider`.

## Maintenance

This repository started out as a human-generated work. To the extent that it is maintained going forward, it will be
maintained primarily through AI-generated updates.

## License

This project is licensed under the Apache License, Version 2.0. See the [LICENSE](LICENSE) file for the full text and
[NOTICE](NOTICE) for attribution of the dependencies it builds on.
