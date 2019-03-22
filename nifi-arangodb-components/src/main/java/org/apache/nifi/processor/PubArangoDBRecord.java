package org.apache.nifi.processor;

import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ArangoDBClientService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.RecordReaderFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({ "record", "put", "arango" })
public class PubArangoDBRecord extends AbstractArangoDBProcessor {
    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name("put-arango-record-record-reader")
        .displayName("Record Reader")
        .description("The record reader to use for reading in the record set from a flowfile.")
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .build();

    public static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
        CLIENT_SERVICE, RECORD_READER, DATABASE_NAME, COLLECTION_NAME
    ));

    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        REL_SUCCESS, REL_FAILURE, REL_ORIGINAL
    )));

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    private volatile ArangoDBClientService arangoDBClientService;
    private volatile RecordReaderFactory readerFactory;

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        arangoDBClientService = context.getProperty(CLIENT_SERVICE).asControllerService(ArangoDBClientService.class);
        readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {

    }
}
