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
package org.apache.nifi.controller;

import com.arangodb.ArangoDB;
import com.arangodb.Protocol;
import com.arangodb.entity.LoadBalancingStrategy;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Tags({"arangodb", "driver", "client"})
@CapabilityDescription("Provides a client driver for accessing ArangoDB.")
public class ArangoDBClientServiceImpl extends AbstractControllerService implements ArangoDBClientService {
    public static final PropertyDescriptor HOSTS = new PropertyDescriptor.Builder()
        .name("arangodb-client-service-hosts")
        .displayName("Coordinator Hosts")
        .description("A list of one or more ArangoDB coordinators. Can be a single host for a cluster. Should be a comma-separated list " +
                "of hostnames and ports.")
        .required(true)
        .addValidator((subject, input, validationContext) -> {
            if (input == null || input.isBlank()) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                        .explanation("At least one host must be supplied.").build();
            }

            for (String value : input.split(",[\\s]*")) {
                String[] parts = value.split(":");
                if (parts.length != 2) {
                    return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                            .explanation(String.format("\"%s\" is not in the form hostname:port.", value)).build();
                }
                try {
                    Integer.parseInt(parts[1]);
                } catch (NumberFormatException e) {
                    return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                            .explanation(String.format("\"%s\" does not have a numeric port.", value)).build();
                }
            }

            return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
        })
        .build();

    public static final AllowableValue LOAD_BALANCE_NONE = new AllowableValue("none", "None",
            "No load balancing.");
    public static final AllowableValue LOAD_BALANCE_ROUND_ROBIN = new AllowableValue("round_robin", "Round Robin",
            "Use the Round Robin load balancing strategy");
    public static final AllowableValue LOAD_BALANCE_RANDOM = new AllowableValue("random", "Random",
            "Use the Random coordinator load balancing strategy.");
    public static final PropertyDescriptor LOAD_BALANCING_STRATEGY = new PropertyDescriptor.Builder()
        .name("arangodb-client-service-load-balance-strategy")
        .displayName("Load Balancing Strategy")
        .description("Set the load-balancing strategy for the driver.")
        .required(true)
        .allowableValues(LOAD_BALANCE_NONE, LOAD_BALANCE_RANDOM, LOAD_BALANCE_ROUND_ROBIN)
        .defaultValue(LOAD_BALANCE_NONE.getValue())
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor FETCH_HOST_LIST = new PropertyDescriptor.Builder()
        .name("arangodb-client-service-fetch-host-list")
        .displayName("Fetch Host List")
        .description("If enabled, this feature will cause the ArangoDB driver to query the configured coordinator(s) for all of the " +
                "hosts in the cluster. It can be used to figure out the entire cluster when you only know a limited number of nodes in it.")
        .required(false)
        .allowableValues("true", "false")
        .defaultValue("true")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
        .name("arangodb-client-service-username")
        .displayName("Username")
        .required(false)
        .addValidator(Validator.VALID)
        .description("The username for connecting to the database, if authentication is configured on the database.")
        .build();
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
        .name("arangodb-client-service-password")
        .displayName("Password")
        .description("The password for connecting to the database, if authentication is configured on the database.")
        .addValidator(Validator.VALID)
        .sensitive(true)
        .required(false)
        .build();
    public static final PropertyDescriptor USE_AUTHENTICATION = new PropertyDescriptor.Builder()
        .name("arangodb-client-service-use-authentication")
        .displayName("Use Authentication")
        .description("Control whether or not to use authentication when connecting to the database.")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .required(true)
        .allowableValues("true", "false")
        .defaultValue("true")
        .build();

    public static final AllowableValue PROTOCOL_HTTP_JSON = new AllowableValue(Protocol.HTTP_JSON.name(), "HTTP/1.1 with JSON",
            "HTTP/1.1 with a JSON request and response body.");
    public static final AllowableValue PROTOCOL_HTTP2_JSON = new AllowableValue(Protocol.HTTP2_JSON.name(), "HTTP/2 with JSON",
            "HTTP/2 with a JSON request and response body. This is the default for the ArangoDB Java driver.");
    public static final PropertyDescriptor PROTOCOL = new PropertyDescriptor.Builder()
            .name("arangodb-client-service-protocol")
            .displayName("Protocol")
            .description("Set the wire protocol for the driver. The VelocyStream (VST) and VelocyPack (VPACK) options that earlier " +
                    "releases of this bundle offered are no longer available: VST was removed from the ArangoDB server in 3.12, and " +
                    "VelocyPack bodies would require a serializer that this bundle does not package.")
            .required(false)
            .allowableValues(PROTOCOL_HTTP_JSON, PROTOCOL_HTTP2_JSON)
            .defaultValue(PROTOCOL_HTTP2_JSON.getValue())
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
            .name("arangodb-client-service-timeout")
            .displayName("Timeout")
            .description("Sets the connection and request timeout in milliseconds.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor TTL = new PropertyDescriptor.Builder()
            .name("arangodb-client-service-ttl")
            .displayName("TTL")
            .description("Set the maximum time to life of a connection. After this time the connection will be closed automatically.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .build();

    public static final PropertyDescriptor CHUNK_SIZE = new PropertyDescriptor.Builder()
            .name("arangodb-client-service-chunk-size")
            .displayName("Chunk size")
            .description("Sets the maximum size of the HTTP request and response chunks in bytes.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_CONNECTIONS = new PropertyDescriptor.Builder()
            .name("arangodb-client-service-max-connections")
            .displayName("Max connections")
            .description("Sets the maximum number of connections the built in connection pool will open per host.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT = new PropertyDescriptor.Builder()
            .name("arangodb-client-service-ssl-context")
            .displayName("SSL Context Service")
            .description("Sets the SSL context to be used when Use SSL is true.")
            .required(false)
            .identifiesControllerService(SSLContextProvider.class)
            .build();

    public static final PropertyDescriptor USE_SSL = new PropertyDescriptor.Builder()
            .name("arangodb-client-service-use-ssl")
            .displayName("Use SSL")
            .description("If set to true SSL will be used when connecting to an ArangoDB server.")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        HOSTS, LOAD_BALANCING_STRATEGY, FETCH_HOST_LIST, USERNAME, PASSWORD, USE_AUTHENTICATION, PROTOCOL, TIMEOUT, TTL,
            CHUNK_SIZE, MAX_CONNECTIONS, SSL_CONTEXT, USE_SSL
    );

    /**
     * Protocol values that this service used to accept, mapped onto the protocol that replaces them. VST is gone from
     * the ArangoDB server as of 3.12 and VelocyPack bodies need a serializer that is not packaged here, so both fall
     * back to the driver's own default.
     */
    private static final Map<String, String> LEGACY_PROTOCOLS = Map.of(
        "VST", Protocol.HTTP2_JSON.name(),
        "PROTOCOL_HTTP_JSON", Protocol.HTTP_JSON.name(),
        "PROTOCOL_HTTP_VPACK", Protocol.HTTP2_JSON.name()
    );

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        config.getPropertyValue(PROTOCOL)
            .map(LEGACY_PROTOCOLS::get)
            .ifPresent(replacement -> config.setProperty(PROTOCOL, replacement));
    }

    @Override
    public Collection<ValidationResult> customValidate(ValidationContext context) {
        List<ValidationResult> problems = new ArrayList<>();
        boolean useAuthentication = context.getProperty(USE_AUTHENTICATION).asBoolean();
        if (useAuthentication) {
            PropertyValue user = context.getProperty(USERNAME);
            PropertyValue pass = context.getProperty(PASSWORD);

            if (!isSet(user)) {
                problems.add(new ValidationResult.Builder().subject(USERNAME.getDisplayName()).valid(false)
                        .explanation("A username is required when Use Authentication is true.").build());
            }
            if (!isSet(pass)) {
                problems.add(new ValidationResult.Builder().subject(PASSWORD.getDisplayName()).valid(false)
                        .explanation("A password is required when Use Authentication is true.").build());
            }
        }

        return problems;
    }

    private static boolean isSet(PropertyValue value) {
        return value.isSet() && value.getValue() != null && !value.getValue().isBlank();
    }

    private volatile ArangoDB.Builder builder;

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        ArangoDB.Builder arangoBuilder = new ArangoDB.Builder();
        String hosts = context.getProperty(HOSTS).getValue();
        for (String part : hosts.split(",[\\s]*")) {
            String[] split = part.split(":");
            arangoBuilder = arangoBuilder.host(split[0], Integer.parseInt(split[1]));
        }

        String loadBalancing = context.getProperty(LOAD_BALANCING_STRATEGY).getValue();
        if (loadBalancing.equals(LOAD_BALANCE_RANDOM.getValue())) {
            arangoBuilder = arangoBuilder.loadBalancingStrategy(LoadBalancingStrategy.ONE_RANDOM);
        } else if (loadBalancing.equals(LOAD_BALANCE_ROUND_ROBIN.getValue())) {
            arangoBuilder = arangoBuilder.loadBalancingStrategy(LoadBalancingStrategy.ROUND_ROBIN);
        } else {
            arangoBuilder = arangoBuilder.loadBalancingStrategy(LoadBalancingStrategy.NONE);
        }

        arangoBuilder = arangoBuilder.acquireHostList(context.getProperty(FETCH_HOST_LIST).asBoolean());

        if (context.getProperty(USE_AUTHENTICATION).asBoolean()) {
            arangoBuilder = arangoBuilder.user(context.getProperty(USERNAME).getValue())
                    .password(context.getProperty(PASSWORD).getValue());
        }

        if (context.getProperty(PROTOCOL).isSet()) {
            arangoBuilder = arangoBuilder.protocol(Protocol.valueOf(context.getProperty(PROTOCOL).getValue()));
        }

        if (context.getProperty(TIMEOUT).isSet()) {
            arangoBuilder = arangoBuilder.timeout(context.getProperty(TIMEOUT).asInteger());
        }

        if (context.getProperty(TTL).isSet()) {
            arangoBuilder = arangoBuilder.connectionTtl(context.getProperty(TTL).asLong());
        }

        if (context.getProperty(CHUNK_SIZE).isSet()) {
            arangoBuilder = arangoBuilder.chunkSize(context.getProperty(CHUNK_SIZE).asInteger());
        }

        if (context.getProperty(MAX_CONNECTIONS).isSet()) {
            arangoBuilder = arangoBuilder.maxConnections(context.getProperty(MAX_CONNECTIONS).asInteger());
        }

        if (context.getProperty(USE_SSL).isSet()) {
            arangoBuilder = arangoBuilder.useSsl(context.getProperty(USE_SSL).asBoolean());
        }

        if (context.getProperty(SSL_CONTEXT).isSet()) {
            SSLContextProvider sslContextProvider = context.getProperty(SSL_CONTEXT).asControllerService(SSLContextProvider.class);
            arangoBuilder = arangoBuilder.sslContext(sslContextProvider.createContext());
        }

        this.builder = arangoBuilder;
    }

    @Override
    public ArangoDB getConnection() {
        return this.builder.build();
    }
}
