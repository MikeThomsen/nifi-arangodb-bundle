package org.apache.nifi.processor

import groovy.json.JsonSlurper
import org.apache.nifi.flowfile.FlowFile
import org.junit.Before
import org.junit.Test

class QueryArangoDBIT extends AbstractArangoDBIT {
    @Before
    void setup() {
        super.setup(QueryArangoDB.class)
        runner.enableControllerService(clientService)
        super.setupTestDocuments()
    }

    @Test
    void testCountQuery() {
        runner.setProperty(QueryArangoDB.QUERY, "FOR message IN messages COLLECT WITH COUNT INTO cnt RETURN cnt")
        runner.assertValid()
        runner.run()

        runner.assertTransferCount(QueryArangoDB.REL_SUCCESS, 1)
        runner.assertTransferCount(QueryArangoDB.REL_ORIGINAL, 0)

        def ff = runner.getFlowFilesForRelationship(QueryArangoDB.REL_SUCCESS)[0]
        def raw = runner.getContentAsByteArray(ff)
        def str = new String(raw)
        def parsed = new JsonSlurper().parseText(str)
        assert parsed?.size() == 1
        assert parsed["result"] == 2
    }

    @Test
    void testMassInsert() {
        arangoDB.db("nifi").createCollection("users")
        runner.setProperty(QueryArangoDB.QUERY, '${query}')
        runner.enqueue("", ["query": """
            FOR i IN 1..1000
              INSERT {
                id: 100000 + i,
                age: 18 + FLOOR(RAND() * 25),
                name: CONCAT('test', TO_STRING(i)),
                active: false,
                gender: i % 2 == 0 ? 'male' : 'female'
              } IN users
        """])
        runner.run()
        runner.assertTransferCount(QueryArangoDB.REL_FAILURE, 0)
        runner.assertTransferCount(QueryArangoDB.REL_SUCCESS, 0)
        runner.assertTransferCount(QueryArangoDB.REL_ORIGINAL, 1)
    }

    @Test
    void testRetrieve() {
        runner.setProperty(QueryArangoDB.QUERY, """
            FOR message IN messages RETURN message
        """)
        runner.run()
        runner.assertTransferCount(QueryArangoDB.REL_FAILURE, 0)
        runner.assertTransferCount(QueryArangoDB.REL_SUCCESS, 2)
        runner.assertTransferCount(QueryArangoDB.REL_ORIGINAL, 0)

        for (FlowFile ff : runner.getFlowFilesForRelationship(QueryArangoDB.REL_SUCCESS)) {
            byte[] raw = runner.getContentAsByteArray(ff)
            String str = new String(raw)
            def parsed = new JsonSlurper().parseText(str)
            assert parsed?.size() >= 4
            assert parsed["to"]
            assert parsed["from"]
            assert parsed["message"]
        }
    }

    @Test
    void testBadQuery() {
        runner.setProperty(QueryArangoDB.QUERY, """
            DO nothing NOW
        """)
        runner.enqueue("")
        runner.run()
        runner.assertTransferCount(QueryArangoDB.REL_FAILURE, 1)
        runner.assertTransferCount(QueryArangoDB.REL_SUCCESS, 0)
        runner.assertTransferCount(QueryArangoDB.REL_ORIGINAL, 0)
    }

    @Test
    void testDelete() {
        arangoDB.db("nifi").createCollection("users")
        arangoDB.db("nifi").query("""
            INSERT { username: "john.smith" } IN users
        """, Object.class)
        runner.setProperty(QueryArangoDB.QUERY, """
            FOR user IN users REMOVE user IN users
        """)
        runner.enqueue("")
        runner.run()
        runner.assertTransferCount(QueryArangoDB.REL_FAILURE, 0)
        runner.assertTransferCount(QueryArangoDB.REL_SUCCESS, 0)
        runner.assertTransferCount(QueryArangoDB.REL_ORIGINAL, 1)
    }
}
