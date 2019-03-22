package org.apache.nifi.controller;

import com.arangodb.ArangoDB;
import com.arangodb.entity.LoadBalancingStrategy;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;

import java.util.Arrays;
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

    public static final AllowableValue LOAD_BALANCE_ROUND_ROBIN = new AllowableValue("round_robin", "Round Robin",
            "Use the Round Robin load balancing strategy");
    public static final AllowableValue LOAD_BALANCE_RANDOM = new AllowableValue("random", "Random",
            "Use the Random coordinator load balancing strategy.");
    public static final PropertyDescriptor LOAD_BALANCING_STRATEGY = new PropertyDescriptor.Builder()
        .name("arangodb-client-service-load-balance-strategy")
        .displayName("Load Balancing Strategy")
        .description("Set the load-balancing strategy for the driver.")
        .required(true)
        .allowableValues(LOAD_BALANCE_RANDOM, LOAD_BALANCE_ROUND_ROBIN)
        .defaultValue(LOAD_BALANCE_ROUND_ROBIN.getValue())
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

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
        HOSTS, LOAD_BALANCING_STRATEGY, FETCH_HOST_LIST
    ));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
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
        this.builder = _builder;
    }

    @Override
    public ArangoDB getConnection() {
        return this.builder.build();
    }
}
