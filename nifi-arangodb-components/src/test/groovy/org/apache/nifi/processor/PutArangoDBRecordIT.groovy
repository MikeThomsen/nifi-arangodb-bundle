package org.apache.nifi.processor

import com.arangodb.ArangoDB
import org.apache.nifi.serialization.RecordReaderFactory
import org.apache.nifi.serialization.record.MockRecordParser
import org.apache.nifi.serialization.record.RecordFieldType
import org.junit.Before
import org.junit.Test
import org.testng.Assert

class PutArangoDBRecordIT extends AbstractArangoDBIT {
    RecordReaderFactory readerFactory

    @Before
    void setup() {
        readerFactory = new MockRecordParser()
        super.setup()
        runner.addControllerService("recordReader", readerFactory)
        runner.setProperty(PutArangoDBRecord.RECORD_READER, "recordReader")
        runner.setProperty(PutArangoDBRecord.KEY_RECORD_PATH, "/id")
        runner.enableControllerService(clientService)
        runner.enableControllerService(readerFactory)
        runner.assertValid()

        readerFactory.addSchemaField("id", RecordFieldType.INT)
        readerFactory.addSchemaField("message", RecordFieldType.STRING)
        readerFactory.addSchemaField("from", RecordFieldType.STRING)
        readerFactory.addSchemaField("to", RecordFieldType.STRING)

        readerFactory.addRecord(1, "Hello, world", "john.smith", "jane.doe")
        readerFactory.addRecord(2, "Goodbye!", "jane.doe", "john.smith")
        arangoDB = clientService.getConnection()
    }

    @Test
    void testBulkInsert() {
        runner.enqueue("test")
        runner.run()

        runner.assertTransferCount(PutArangoDBRecord.REL_FAILURE, 0)
        runner.assertTransferCount(PutArangoDBRecord.REL_SUCCESS, 1)

        def count = arangoDB.db("nifi").query("FOR message IN messages COLLECT WITH COUNT INTO cnt RETURN cnt", Long.class).iterator().next()
        Assert.assertEquals(2l, count)
    }
}
