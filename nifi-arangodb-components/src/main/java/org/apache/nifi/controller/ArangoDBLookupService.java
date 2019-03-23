package org.apache.nifi.controller;

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.arango.common.ArangoClientConfiguration;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.serialization.record.Record;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ArangoDBLookupService extends AbstractControllerService implements LookupService<Record>, ArangoClientConfiguration {
    public static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
        CLIENT_SERVICE,
        new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(DATABASE_NAME)
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .build()
    ));

    public static final String QUERY_KEY = "query";

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    private volatile ArangoDBClientService clientService;
    private volatile String databaseName;

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        clientService = context.getProperty(CLIENT_SERVICE).asControllerService(ArangoDBClientService.class);
        databaseName = context.getProperty(DATABASE_NAME).evaluateAttributeExpressions().getValue();
    }

    @Override
    public Optional<Record> lookup(Map<String, Object> map) throws LookupFailureException {
        return lookup(map, new HashMap<>());
    }

    @Override
    public Optional<Record> lookup(Map<String, Object> coordinates, Map<String, String> context) throws LookupFailureException {
        if (!coordinates.containsKey(QUERY_KEY)) {
            throw new LookupFailureException(String.format("Nothing specified for lookup key \"%s\"", QUERY_KEY));
        }

        return Optional.empty();
    }

    @Override
    public Class<?> getValueType() {
        return Record.class;
    }

    @Override
    public Set<String> getRequiredKeys() {
        return Collections.singleton(QUERY_KEY);
    }
}
