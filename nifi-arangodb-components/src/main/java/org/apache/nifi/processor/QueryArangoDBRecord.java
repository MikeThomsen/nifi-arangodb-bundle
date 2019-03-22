package org.apache.nifi.processor;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoIterator;
import com.arangodb.entity.BaseDocument;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.RecordWriter;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.OutputStream;

public class QueryArangoDBRecord extends AbstractArangoDBProcessor {
    public PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
        .name("arango-query")
        .displayName("Query")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
        .description("An AQL query to execute.")
        .build();
    public PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
        .name("arango-query-record-writer")
        .displayName("Record Writer")
        .description("The record writer to use for writing the result set.")
        .required(true)
        .addValidator(Validator.VALID)
        .build();

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

        } finally {
            session.remove(output);
            connection.shutdown();
        }
    }
}
