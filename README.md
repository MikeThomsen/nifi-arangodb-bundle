## Overview

This bundle adds support for ArangoDB to Apache NiFi. It aims to allow users of both products to full leverage NiFi features like the Record API for efficiently working with ArangoDB.

## Build Instructions

Install Apache Maven, then run `mvn clean install` from the root of the repository to create the NAR file.

The version of NiFi can be adjusted by opening the `pom.xml` file at the root of the repository and changing the property `nifi.version` to whichever release >= NiFi 1.8.0 that you want to target.
