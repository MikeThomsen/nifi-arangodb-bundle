package org.apache.nifi.processor

import com.arangodb.ArangoDB
import org.apache.nifi.controller.ArangoDBClientService
import org.apache.nifi.controller.ArangoDBClientServiceImpl
import org.apache.nifi.controller.ArangoDBLookupService
import org.apache.nifi.lookup.LookupService
import org.apache.nifi.schema.access.SchemaAccessUtils
import org.apache.nifi.serialization.record.Record
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.After
import org.junit.Before
import org.junit.Test

class ArangoDBLookupServiceIT {
    TestRunner runner
    LookupService<Record> lookupService
    ArangoDB connection

    static final String DB = "lookup_tests"
    static final String COL = "test_data"

    @Before
    void setup() {
        lookupService = new ArangoDBLookupService()
        ArangoDBClientService clientService = new ArangoDBClientServiceImpl()
        runner = TestRunners.newTestRunner(MockProcessor.class)
        runner.addControllerService("lookupService", lookupService)
        runner.addControllerService("clientService", clientService)
        runner.setProperty(clientService, ArangoDBClientServiceImpl.HOSTS, "localhost:8529")
        runner.setProperty(clientService, ArangoDBClientServiceImpl.USERNAME, "root")
        runner.setProperty(clientService, ArangoDBClientServiceImpl.PASSWORD, "testing1234")
        runner.setProperty(clientService, ArangoDBClientServiceImpl.LOAD_BALANCING_STRATEGY, ArangoDBClientServiceImpl.LOAD_BALANCE_NONE)
        runner.setProperty(lookupService, ArangoDBLookupService.CLIENT_SERVICE, "clientService")
        runner.setProperty(lookupService, ArangoDBLookupService.DATABASE_NAME, DB)
        runner.setProperty(MockProcessor.LOOKUP_SERVICE, "lookupService")
        runner.enableControllerService(clientService)

        connection = clientService.getConnection()
        connection.db(DB).create()
        connection.db(DB).createCollection(COL)
    }

    @After
    void tearDown() {
        connection.db(DB).drop()
        connection.shutdown()
    }

    @Test
    void testSimpleNamedParameter() {
        def db = connection.db(DB)
        db.query("""
            INSERT {
                from: "e.goldstein",
                to: "w.smith",
                message: "My book is attached."
            } IN ${COL}
        """, Object.class)
        runner.setProperty(lookupService, ArangoDBLookupService.QUERY, """
            FOR message IN ${COL}
                FILTER message.from == @is_from
            RETURN message
        """)
        runner.setProperty(lookupService, lookupService.getPropertyDescriptor(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY.name), SchemaAccessUtils.INFER_SCHEMA)
        runner.enableControllerService(lookupService)
        def record = lookupService.lookup([ is_from: "e.goldstein" ])
        assert record?.isPresent()
    }
}
