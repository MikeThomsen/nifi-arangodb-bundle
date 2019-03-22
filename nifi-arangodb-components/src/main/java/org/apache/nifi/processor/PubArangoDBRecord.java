package org.apache.nifi.processor;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDB;
import com.arangodb.entity.BaseDocument;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ArangoDBClientService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({ "record", "put", "arango" })
public class PubArangoDBRecord extends AbstractArangoDBProcessor {
    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name("put-arango-record-record-reader")
        .displayName("Record Reader")
        .description("The record reader to use for reading in the record set from a flowfile.")
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .build();
    public static final PropertyDescriptor KEY_RECORD_PATH = new PropertyDescriptor.Builder()
        .name("put-arango-record-key-path")
        .displayName("Key Record Path")
        .description("The record path where the document key is stored.")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    public static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
        CLIENT_SERVICE, RECORD_READER, KEY_RECORD_PATH, DATABASE_NAME, COLLECTION_NAME
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
    private volatile RecordPathCache recordPathCache;

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        arangoDBClientService = context.getProperty(CLIENT_SERVICE).asControllerService(ArangoDBClientService.class);
        readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        recordPathCache = new RecordPathCache(25);
    }

    private String getKey(Record record, RecordPath keyPath) {
        RecordPathResult result = keyPath.evaluate(record);
        Optional<FieldValue> value = result.getSelectedFields().findFirst();
        if (value.isPresent()) {
            Object val = value.get().getValue();
            if (val == null) {
                throw new ProcessException("No record value for key field.");
            }
            return val.toString();
        } else {
            return null;
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        FlowFile output = session.create(flowFile);
        ArangoDB db = arangoDBClientService.getConnection();
        try (InputStream is = session.read(flowFile);
             OutputStream os = session.write(output)) {
            String recordPath = context.getProperty(KEY_RECORD_PATH).evaluateAttributeExpressions(flowFile).getValue();
            String dbName = context.getProperty(DATABASE_NAME).evaluateAttributeExpressions(flowFile).getValue();
            String colName = context.getProperty(COLLECTION_NAME).evaluateAttributeExpressions(flowFile).getValue();
            ArangoCollection collection = db.db(dbName).collection(colName);

            RecordPath keyPath = recordPathCache.getCompiled(recordPath);
            RecordReader reader = readerFactory.createRecordReader(flowFile, is, getLogger());
            Record record;

            while ((record = reader.nextRecord()) != null) {
                String key = getKey(record, keyPath);
                Map<String, Object> contentMap = (Map<String, Object>) DataTypeUtils
                        .convertRecordFieldtoObject(record, RecordFieldType.RECORD.getRecordDataType(record.getSchema()));
                BaseDocument document = new BaseDocument();
                document.setKey(key);
                document.setProperties(contentMap);
                collection.insertDocument(document);
            }
            
            reader.close();
            os.close();
            is.close();

            session.transfer(output, REL_SUCCESS);
            session.transfer(flowFile, REL_ORIGINAL);
        } catch (Exception ex) {
            getLogger().error("Failed processing record set.", ex);
            session.remove(output);
            session.transfer(flowFile, REL_FAILURE);
        } finally {
            db.shutdown();
        }
    }
}
