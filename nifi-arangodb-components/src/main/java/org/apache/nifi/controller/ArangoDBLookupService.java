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

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
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
        return List.of(
            CLIENT_SERVICE,
            new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(DATABASE_NAME)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .build(),
            new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(QUERY)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .build(),
            new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(SCHEMA_ACCESS_STRATEGY)
                .allowableValues(STRATEGIES)
                .defaultValue(getDefaultSchemaAccessStrategy().getValue())
                .build()
        );
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
            Map<String, Object> params = new HashMap<>(coordinates);
            params.putAll(context);

            try (ArangoCursor<Object> cursor = connection.db(databaseName).query(query, Object.class, params)) {
                Record record = null;
                if (cursor.hasNext() && cursor.next() instanceof Map<?, ?> next) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> doc = (Map<String, Object>) next;
                    RecordSchema schema = loadSchema(context, doc);
                    record = new MapRecord(schema, doc);
                }

                return Optional.ofNullable(record);
            }
        } catch (Exception ex) {
            getLogger().error("Lookup against database {} failed.", databaseName, ex);
            throw new LookupFailureException(ex);
        } finally {
            connection.shutdown();
        }
    }

    private RecordSchema loadSchema(Map<String, String> context, Map<String, Object> doc) throws LookupFailureException {
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
