package org.apache.nifi.processor

import com.arangodb.entity.BaseDocument
import groovy.json.JsonSlurper
import org.apache.nifi.json.JsonRecordSetWriter
import org.apache.nifi.schema.access.SchemaAccessUtils
import org.apache.nifi.serialization.RecordSetWriterFactory
import org.apache.nifi.serialization.SimpleRecordSchema
import org.apache.nifi.serialization.record.*
import org.junit.Before
import org.junit.Test

class QueryArangoDBRecordIT extends AbstractArangoDBIT {
    RecordSetWriterFactory writer

    @Before
    void setup() {
        super.setup(QueryArangoDBRecord.class)
        def schema = new SimpleRecordSchema([
            new RecordField("id", new DataType(RecordFieldType.LONG, null)),
            new RecordField("message", new DataType(RecordFieldType.STRING, null)),
            new RecordField("from", new DataType(RecordFieldType.STRING, null)),
            new RecordField("to", new DataType(RecordFieldType.STRING, null))
        ], new StandardSchemaIdentifier.Builder().name("message").build())
        def schemaRegistry = new MockSchemaRegistry()
        schemaRegistry.addSchema("message", schema)
        writer = new JsonRecordSetWriter()
        runner.addControllerService("schemaRegistry", schemaRegistry)
        runner.addControllerService("writer", writer)
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_REGISTRY, "schemaRegistry")
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_NAME_PROPERTY)
        runner.setProperty(QueryArangoDBRecord.RECORD_WRITER, "writer");
        runner.setProperty(QueryArangoDBRecord.QUERY, "FOR message IN messages RETURN message")
        runner.enableControllerService(schemaRegistry)
        runner.enableControllerService(writer)
        runner.enableControllerService(clientService)

        arangoDB = clientService.getConnection()
        def messages = arangoDB.db("nifi").collection("messages")
        messages.insertDocument(new BaseDocument().with { doc ->
            doc.key = "1"
            doc.properties = [ "from": "john.smith", "to": "jane.doe", "message": "Hi!"]
        })
        messages.insertDocument(new BaseDocument().with { doc ->
            doc.key = "2"
            doc.properties = [ "from": "jane.doe", "to": "john.smith", "message": "Bye!"]
        })
    }

    @Test
    void testRetrieveRecords() {
        runner.setProperty(QueryArangoDBRecord.QUERY, "FOR message IN messages RETURN message")
        runner.enqueue("", [ "schema.name": "message"])
        runner.run()
        runner.assertTransferCount(QueryArangoDBRecord.REL_FAILURE, 0)
        runner.assertTransferCount(QueryArangoDBRecord.REL_SUCCESS, 1)
        runner.assertTransferCount(QueryArangoDBRecord.REL_ORIGINAL, 1)

        def ff = runner.getFlowFilesForRelationship(QueryArangoDBRecord.REL_SUCCESS)[0]
        def content = runner.getContentAsByteArray(ff)
        def raw = new String(content)
        def parsed = new JsonSlurper().parseText(raw)
        assert parsed
        assert parsed instanceof List
        assert parsed.size() == 2
    }
}
