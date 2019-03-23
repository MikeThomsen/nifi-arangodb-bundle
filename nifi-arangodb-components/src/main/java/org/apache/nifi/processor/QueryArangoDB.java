package org.apache.nifi.processor;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoIterator;
import com.arangodb.entity.BaseDocument;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({ "query", "arangodb" })
@CapabilityDescription("This is a generic query processor for ArangoDB. It is mainly intended for running aggregation queries such " +
        "as counts, deletes, updates, etc. It is not suitable for running large fetches of data because it keeps track of all of the output flowfiles " +
        "in an in-memory cache and will likely cause NiFi to run out of memory if pulling down a very large data set. Use QueryArangoDBRecord " +
        "for large fetches of records.")
public class QueryArangoDB extends AbstractArangoDBProcessor {
    public static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
        CLIENT_SERVICE, QUERY, DATABASE_NAME
    ));

    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        REL_SUCCESS, REL_ORIGINAL, REL_FAILURE
    )));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        super.onScheduled(context);
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        String databaseName = context.getProperty(DATABASE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        String query = context.getProperty(QUERY).evaluateAttributeExpressions(flowFile).getValue();

        ArangoDB connection = arangoDBClientService.getConnection();
        List<FlowFile> flowFiles = new ArrayList<>();
        try {
            ArangoIterator<Object> iterator = connection.db(databaseName).query(query, Object.class).iterator();
            while (iterator.hasNext()) {
                Object next = iterator.next();
                Map<String, Object> map;
                if (next instanceof BaseDocument) {
                    BaseDocument doc = (BaseDocument)next;
                    map = doc.getProperties();
                } else if (next instanceof Map) {
                    map = (Map<String, Object>)next;
                } else if (next instanceof Number) {
                    map = new HashMap<String, Object>(){{
                        put("result", next);
                    }};
                } else {
                    map = new HashMap<String, Object>(){{
                        put("result", next.toString());
                    }};
                }
                flowFiles.add(writeOutput(map, session, flowFile));
            }

            if (flowFile != null) {
                session.transfer(flowFile, REL_ORIGINAL);
            }
        } catch (Exception ex) {
            getLogger().error("", ex);
            for (FlowFile ff : flowFiles) {
                session.remove(ff);
            }
            if (flowFile != null) {
                session.transfer(flowFile, REL_FAILURE);
            }
        } finally {
            connection.shutdown();
        }
    }

    private FlowFile writeOutput(Map<String, Object> result, ProcessSession session, FlowFile parent) throws JsonProcessingException {
        String resultString = MAPPER.writeValueAsString(result);
        FlowFile resultFF = parent != null ? session.create(parent) : session.create();
        resultFF = session.write(resultFF, out -> out.write(resultString.getBytes()));
        session.transfer(resultFF, REL_SUCCESS);

        return resultFF;
    }
}
