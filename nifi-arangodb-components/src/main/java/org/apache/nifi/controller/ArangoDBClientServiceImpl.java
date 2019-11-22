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
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
            if (StringUtils.isEmpty(input)) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(false).build();
            }

            String[] values = input.split(",[\\s]*");
            boolean valid = true;
            for (String value : values) {
                String[] parts = value.split(":");
                if (parts.length != 2) {
                    valid = false;
                    break;
                }
            }

            return new ValidationResult.Builder().subject(subject).input(input).valid(valid).build();
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

    public static final AllowableValue PROTOCOL_VST = new AllowableValue("VST", "VST",
            "VelocyStream");
    public static final AllowableValue PROTOCOL_HTTP_JSON = new AllowableValue("PROTOCOL_HTTP_JSON", "PROTOCOL_HTTP_JSON",
            "HTTP with JSON body");
    public static final AllowableValue PROTOCOL_HTTP_VPACK = new AllowableValue("PROTOCOL_HTTP_VPACK", "PROTOCOL_HTTP_VPACK",
            "HTTP with VelocyPack body");
    public static final PropertyDescriptor PROTOCOL = new PropertyDescriptor.Builder()
            .name("arangodb-client-service-protocol")
            .displayName("Protocol")
            .description("Set the protocol for the driver.")
            .required(false)
            .allowableValues(PROTOCOL_VST, PROTOCOL_HTTP_JSON, PROTOCOL_HTTP_VPACK)
            .defaultValue(PROTOCOL_VST.getValue())
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
            .description("Sets the chunk size when Protocol VST is used.")
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
            .displayName("SSL Context")
            .description("Sets the SSL context to be used when useSsl is true.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final PropertyDescriptor USE_SSL = new PropertyDescriptor.Builder()
            .name("arangodb-client-service-use-ssl")
            .displayName("Use SSL")
            .description("If set to true SSL will be used when connecting to an ArangoDB server.")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
        HOSTS, LOAD_BALANCING_STRATEGY, FETCH_HOST_LIST, USERNAME, PASSWORD, USE_AUTHENTICATION, PROTOCOL, TIMEOUT, TTL,
            CHUNK_SIZE, MAX_CONNECTIONS, SSL_CONTEXT, USE_SSL
    ));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Collection<ValidationResult> customValidate(ValidationContext context) {
        List<ValidationResult> problems = new ArrayList<>();
        boolean useAuthentication = context.getProperty(USE_AUTHENTICATION).asBoolean();
        if (useAuthentication) {
            PropertyValue user = context.getProperty(USERNAME);
            PropertyValue pass = context.getProperty(PASSWORD);
            boolean userIsGood = user.isSet() && !StringUtils.isEmpty(user.getValue());
            boolean passIsGood = pass.isSet() && !StringUtils.isEmpty(pass.getValue());

            if (!userIsGood) {
                problems.add(new ValidationResult.Builder().subject(USERNAME.getName()).input(user.getValue()).valid(false).build());
            }
            if (!passIsGood) {
                problems.add(new ValidationResult.Builder().subject(PASSWORD.getName()).input(pass.getValue()).valid(false).build());
            }
        }

        return problems;
    }

    private volatile ArangoDB.Builder builder;

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        ArangoDB.Builder _builder = new ArangoDB.Builder();
        String hosts = context.getProperty(HOSTS).getValue();
        String[] hostsSplit = hosts.split(",[\\s]*");
        for (String part : hostsSplit) {
            String[] split = part.split(":");
            _builder = _builder.host(split[0], Integer.valueOf(split[1]));
        }
        String loadBalancing = context.getProperty(LOAD_BALANCING_STRATEGY).getValue();
        if (loadBalancing.equals(LOAD_BALANCE_RANDOM.getValue())) {
            _builder = _builder.loadBalancingStrategy(LoadBalancingStrategy.ONE_RANDOM);
        } else if (loadBalancing.equals(LOAD_BALANCE_ROUND_ROBIN.getValue())) {
            _builder = _builder.loadBalancingStrategy(LoadBalancingStrategy.ROUND_ROBIN);
        } else {
            _builder = _builder.loadBalancingStrategy(LoadBalancingStrategy.NONE);
        }

        boolean fetchList = context.getProperty(FETCH_HOST_LIST).asBoolean();
        _builder = _builder.acquireHostList(fetchList);

        if (context.getProperty(USE_AUTHENTICATION).asBoolean()) {
            _builder = _builder.user(context.getProperty(USERNAME).getValue())
                    .password(context.getProperty(PASSWORD).getValue());
        }

        if (context.getProperty(PROTOCOL).isSet()) {
            String protocol = context.getProperty(PROTOCOL).getValue();
            if (protocol.equals(PROTOCOL_VST.getValue())) {
                _builder = _builder.useProtocol(Protocol.VST);
            } else if (protocol.equals(PROTOCOL_HTTP_JSON.getValue())) {
                _builder = _builder.useProtocol(Protocol.HTTP_JSON);
            } else if (protocol.equals(PROTOCOL_HTTP_VPACK.getValue())) {
                _builder = _builder.useProtocol(Protocol.HTTP_VPACK);
            }
        }

        if (context.getProperty(TIMEOUT).isSet()) {
            Integer timeout = context.getProperty(TIMEOUT).asInteger();
            _builder = _builder.timeout(timeout);
        }

        if (context.getProperty(TTL).isSet()) {
            Long ttl = context.getProperty(TTL).asLong();
            _builder = _builder.connectionTtl(ttl);
        }

        if (context.getProperty(CHUNK_SIZE).isSet()) {
            Integer chunkSize = context.getProperty(CHUNK_SIZE).asInteger();
            _builder = _builder.chunksize(chunkSize);
        }

        if (context.getProperty(MAX_CONNECTIONS).isSet()) {
            Integer maxConnections = context.getProperty(MAX_CONNECTIONS).asInteger();
            _builder = _builder.maxConnections(maxConnections);
        }

        if (context.getProperty(USE_SSL).isSet()) {
            Boolean useSsl = context.getProperty(USE_SSL).asBoolean();
            _builder = _builder.useSsl(useSsl);
        }

        if (context.getProperty(SSL_CONTEXT).isSet()) {
            SSLContextService sslContextService = context.getProperty(SSL_CONTEXT).asControllerService(SSLContextService.class);
            _builder = _builder.sslContext(sslContextService.createSSLContext(SSLContextService.ClientAuth.REQUIRED));
        }

        this.builder = _builder;
    }

    @Override
    public ArangoDB getConnection() {
        return this.builder.build();
    }
}
