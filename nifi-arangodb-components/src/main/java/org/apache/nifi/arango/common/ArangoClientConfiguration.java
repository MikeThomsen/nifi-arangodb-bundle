package org.apache.nifi.arango.common;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ArangoDBClientService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

public interface ArangoClientConfiguration {
    PropertyDescriptor CLIENT_SERVICE = new PropertyDescriptor.Builder()
        .name("arango-client-service")
        .displayName("Client Service")
        .description("Client service for getting database connections.")
        .required(true)
        .identifiesControllerService(ArangoDBClientService.class)
        .build();
    PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
        .name("arango-database-name")
        .displayName("Database Name")
        .description("The name of the database.")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
        .build();
    PropertyDescriptor COLLECTION_NAME = new PropertyDescriptor.Builder()
        .name("arango-collection-name")
        .displayName("Collection Name")
        .description("The name of the collection.")
        .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .build();
    PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
        .name("arango-query")
        .displayName("Query")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
        .description("An AQL query to execute.")
        .build();
}
