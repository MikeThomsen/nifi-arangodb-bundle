package org.apache.nifi.processor;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ArangoDBClientService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

public abstract class AbstractArangoDBProcessor extends AbstractProcessor {
    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
        .name("arango-query")
        .displayName("Query")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
        .description("An AQL query to execute.")
        .build();
    public static final PropertyDescriptor CLIENT_SERVICE = new PropertyDescriptor.Builder()
        .name("arango-client-service")
        .displayName("Client Service")
        .description("Client service for getting database connections.")
        .required(true)
        .identifiesControllerService(ArangoDBClientService.class)
        .build();
    public static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
        .name("arango-database-name")
        .displayName("Database Name")
        .description("The name of the database.")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
        .build();
    public static final PropertyDescriptor COLLECTION_NAME = new PropertyDescriptor.Builder()
        .name("arango-collection-name")
        .displayName("Collection Name")
        .description("The name of the collection.")
        .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All successful flowfiles go to this relationship.")
        .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("All failed flowfiles go to this relationship.")
        .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .description("When the operation succeeeds, the original input flowfile will go this relationship.")
        .build();

    protected volatile ArangoDBClientService arangoDBClientService;
    public void onScheduled(ProcessContext context) {
        arangoDBClientService = context.getProperty(CLIENT_SERVICE).asControllerService(ArangoDBClientService.class);
    }
}
