package org.apache.nifi.controller;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoIterator;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.arango.common.ArangoClientConfiguration;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.serialization.JsonInferenceSchemaRegistryService;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.nifi.schema.access.SchemaAccessUtils.INFER_SCHEMA;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT_PROPERTY;

@Tags({ "lookup", "record", "enrichment", "arangodb" })
@CapabilityDescription("This controller service provides a lookup service that is built around ArangoDB for enriching record sets.")
public class ArangoDBLookupService extends JsonInferenceSchemaRegistryService implements LookupService<Record>, ArangoClientConfiguration {
    public static final AllowableValue[] STRATEGIES = new AllowableValue[] {
            SCHEMA_NAME_PROPERTY, SCHEMA_TEXT_PROPERTY, INFER_SCHEMA
    };

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.unmodifiableList(Arrays.asList(
            CLIENT_SERVICE,
            new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(DATABASE_NAME)
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .build(),
            new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(QUERY)
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .build(),
            new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(SCHEMA_ACCESS_STRATEGY)
                .allowableValues(STRATEGIES)
                .defaultValue(getDefaultSchemaAccessStrategy().getValue())
                .build()
        ));
    }

    private volatile ArangoDBClientService clientService;
    private volatile String databaseName;
    private volatile String query;

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        clientService = context.getProperty(CLIENT_SERVICE).asControllerService(ArangoDBClientService.class);
        databaseName = context.getProperty(DATABASE_NAME).evaluateAttributeExpressions().getValue();
        query = context.getProperty(QUERY).evaluateAttributeExpressions().getValue();
        super.onEnabled(context);
    }

    @Override
    public Optional<Record> lookup(Map<String, Object> map) throws LookupFailureException {
        return lookup(map, new HashMap<>());
    }

    @Override
    public Optional<Record> lookup(Map<String, Object> coordinates, Map<String, String> context) throws LookupFailureException {
        ArangoDB connection = clientService.getConnection();
        try {
            Map<String, Object> params = new HashMap<>();
            params.putAll(coordinates);
            params.putAll(context);
            ArangoIterator<Object> iterator = connection.db(databaseName).query(query, params, Object.class).iterator();
            Record record = null;
            if (iterator.hasNext()) {
                Object next = iterator.next();
                if (next instanceof Map) {
                    Map<String, Object> doc = (Map<String, Object>)next;
                    RecordSchema schema = loadSchema(context, doc);
                    record = new MapRecord(schema, doc);
                }
            }

            return Optional.ofNullable(record);
        } catch (Exception ex) {
            getLogger().error("", ex);
            throw new LookupFailureException(ex);
        } finally {
            connection.shutdown();
        }
    }

    private RecordSchema loadSchema(Map<String, String> context, Map doc) throws LookupFailureException {
        try {
            return getSchema(context, doc, null);
        } catch (Exception ex) {
            throw new LookupFailureException(ex);
        }
    }

    @Override
    public Class<?> getValueType() {
        return Record.class;
    }

    @Override
    public Set<String> getRequiredKeys() {
        return Collections.emptySet();
    }
}
