package org.apache.nifi.processor

import org.apache.nifi.controller.ArangoDBClientService
import org.apache.nifi.controller.ArangoDBClientServiceImpl
import org.apache.nifi.serialization.RecordReaderFactory
import org.apache.nifi.serialization.record.MockRecordParser
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
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
        runner.setProperty(PutArangoDBRecord.CLIENT_SERVICE, "clientService")
        runner.setProperty(PutArangoDBRecord.RECORD_READER, "recordReader")
        runner.setProperty(PutArangoDBRecord.DATABASE_NAME, "nifi")
        runner.setProperty(PutArangoDBRecord.COLLECTION_NAME, "messages")
        runner.setProperty(PutArangoDBRecord.KEY_RECORD_PATH, "/id")
        runner.enableControllerService(clientService)
        runner.enableControllerService(readerFactory)
        runner.assertValid()
    }

    @Test
    void testBulkInsert() {

    }
}
