package org.apache.nifi.processor;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoIterator;
import com.arangodb.entity.BaseDocument;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({ "query", "arangodb", "record" })
@CapabilityDescription("This processor is intended to be used for fetching large volumes of data from ArangoDB. It uses " +
        "the NiFi Record API to provide the ability serialize result sets in a clean and consistent manner. For deletes, updates " +
        "and aggregation queries, see QueryArangoDB.")
public class QueryArangoDBRecord extends AbstractArangoDBProcessor {
    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
        .name("arango-query-record-writer")
        .displayName("Record Writer")
        .description("The record writer to use for writing the result set.")
        .required(true)
        .addValidator(Validator.VALID)
        .build();

    public static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
        CLIENT_SERVICE, QUERY, RECORD_WRITER, DATABASE_NAME
    ));

    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        REL_SUCCESS, REL_FAILURE, REL_ORIGINAL
    )));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    private volatile RecordSetWriterFactory writerFactory;

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        super.onScheduled(context);
        writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) {
        FlowFile flowFile = session.get();
        FlowFile output = flowFile != null ? session.create(flowFile) : session.create();
        ArangoDB connection = arangoDBClientService.getConnection();

        try (OutputStream os = session.write(output)) {
            String query = context.getProperty(QUERY).evaluateAttributeExpressions(flowFile).getValue();
            String dbName = context.getProperty(DATABASE_NAME).evaluateAttributeExpressions(flowFile).getValue();
            RecordSchema schema = writerFactory.getSchema(flowFile.getAttributes(), null);
            RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, os);
            ArangoIterator<BaseDocument> results = connection.db(dbName).query(query, BaseDocument.class).iterator();
            writer.beginRecordSet();
            while (results.hasNext()) {
                BaseDocument document = results.next();
                Record record = new MapRecord(schema, document.getProperties());
                writer.write(record);
            }
            writer.finishRecordSet();
            writer.close();
            os.close();

            session.transfer(output, REL_SUCCESS);
            session.transfer(flowFile, REL_ORIGINAL);
        } catch (Exception ex) {
            getLogger().error("", ex);
            session.remove(output);
            session.transfer(flowFile, REL_FAILURE);
        } finally {
            connection.shutdown();
        }
    }
}
