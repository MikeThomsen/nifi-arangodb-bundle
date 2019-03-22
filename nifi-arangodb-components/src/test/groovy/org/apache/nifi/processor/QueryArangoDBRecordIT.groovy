package org.apache.nifi.processor


import org.apache.nifi.serialization.record.MockRecordWriter
import org.junit.Before
import org.junit.Test

class QueryArangoDBRecordIT extends AbstractArangoDBIT {
    MockRecordWriter writer

    @Before
    void setup() {
        super.setup(QueryArangoDBRecord.class)
        writer = new MockRecordWriter()
        runner.addControllerService("writer", writer)
        runner.setProperty(QueryArangoDBRecord.RECORD_WRITER, "writer");
        runner.setProperty(QueryArangoDBRecord.QUERY, "FOR message IN messages RETURN message")
        runner.enableControllerService(writer)
        runner.enableControllerService(clientService)
        runner.assertValid()
        arangoDB = clientService.getConnection()
    }

    @Test
    void testRetrieveRecords() {

    }
}
