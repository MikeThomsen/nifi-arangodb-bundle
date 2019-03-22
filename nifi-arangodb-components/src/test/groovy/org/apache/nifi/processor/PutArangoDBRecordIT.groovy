package org.apache.nifi.processor

import com.arangodb.entity.BaseDocument
import org.apache.nifi.controller.ArangoDBClientService
import org.apache.nifi.controller.ArangoDBClientServiceImpl
import org.apache.nifi.serialization.RecordReaderFactory
import org.apache.nifi.serialization.record.MockRecordParser
import org.apache.nifi.serialization.record.RecordFieldType
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.After
import org.junit.Before
import org.junit.Test

class PutArangoDBRecordIT {
    TestRunner runner
    ArangoDBClientService clientService
    RecordReaderFactory readerFactory

    @Before
    void setup() {
        readerFactory = new MockRecordParser()
        clientService = new ArangoDBClientServiceImpl()
        runner = TestRunners.newTestRunner(PutArangoDBRecord.class)
        runner.addControllerService("clientService", clientService)
        runner.addControllerService("recordReader", readerFactory)
        runner.setProperty(clientService, ArangoDBClientServiceImpl.HOSTS, "localhost:8529")
        runner.setProperty(clientService, ArangoDBClientServiceImpl.LOAD_BALANCING_STRATEGY, ArangoDBClientServiceImpl.LOAD_BALANCE_RANDOM)
        runner.setProperty(clientService, ArangoDBClientServiceImpl.USERNAME, "root")
        runner.setProperty(clientService, ArangoDBClientServiceImpl.PASSWORD, "testing1234")
        runner.setProperty(PutArangoDBRecord.CLIENT_SERVICE, "clientService")
        runner.setProperty(PutArangoDBRecord.RECORD_READER, "recordReader")
        runner.setProperty(PutArangoDBRecord.DATABASE_NAME, "nifi")
        runner.setProperty(PutArangoDBRecord.COLLECTION_NAME, "messages")
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
    }

    @After
    void tearDown() {
        def arango = clientService.getConnection()
        arango.db("nifi").query("FOR message IN messages REMOVE message IN messages", BaseDocument.class)
        arango.shutdown()
    }

    @Test
    void testBulkInsert() {
        runner.enqueue("test")
        runner.run()

        runner.assertTransferCount(PutArangoDBRecord.REL_FAILURE, 0)
        runner.assertTransferCount(PutArangoDBRecord.REL_SUCCESS, 1)
    }
}
