/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processor;

import org.apache.nifi.controller.ArangoDBClientService;
import org.apache.nifi.controller.ArangoDBClientServiceImpl;
import org.apache.nifi.util.TestRunner;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/**
 * A single ArangoDB container shared by every integration test in this module. Each test creates and drops its own
 * database, so there is no reason to pay for a container restart per test class. The container is started on first
 * use and torn down by the Testcontainers reaper when the JVM exits.
 */
final class ArangoDBTestContainer {
    static final String ROOT_USER = "root";
    static final String ROOT_PASSWORD = "testing1234";

    private static final int ARANGO_PORT = 8529;
    private static final String DEFAULT_IMAGE = "arangodb:3.12.12";

    private static final GenericContainer<?> CONTAINER =
        new GenericContainer<>(DockerImageName.parse(System.getProperty("arangodb.docker.image", DEFAULT_IMAGE)))
            .withExposedPorts(ARANGO_PORT)
            .withEnv("ARANGO_ROOT_PASSWORD", ROOT_PASSWORD)
            .waitingFor(Wait.forHttp("/_api/version")
                .forPort(ARANGO_PORT)
                .withBasicCredentials(ROOT_USER, ROOT_PASSWORD)
                .forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(3));

    private ArangoDBTestContainer() {
    }

    static synchronized String hostAndPort() {
        if (!CONTAINER.isRunning()) {
            CONTAINER.start();
        }

        return String.format("%s:%d", CONTAINER.getHost(), CONTAINER.getMappedPort(ARANGO_PORT));
    }

    /**
     * Points a client service at the shared container. The host list is not fetched because the container runs a
     * single server and would advertise an endpoint that is only reachable from inside the Docker network.
     */
    static void configure(TestRunner runner, ArangoDBClientService clientService) {
        runner.setProperty(clientService, ArangoDBClientServiceImpl.HOSTS, hostAndPort());
        runner.setProperty(clientService, ArangoDBClientServiceImpl.USERNAME, ROOT_USER);
        runner.setProperty(clientService, ArangoDBClientServiceImpl.PASSWORD, ROOT_PASSWORD);
        runner.setProperty(clientService, ArangoDBClientServiceImpl.FETCH_HOST_LIST, "false");
    }
}
