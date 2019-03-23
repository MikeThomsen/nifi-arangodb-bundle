package org.apache.nifi.processor;

import org.apache.nifi.arango.common.ArangoClientConfiguration;
import org.apache.nifi.controller.ArangoDBClientService;

public abstract class AbstractArangoDBProcessor extends AbstractProcessor implements ArangoClientConfiguration {
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
