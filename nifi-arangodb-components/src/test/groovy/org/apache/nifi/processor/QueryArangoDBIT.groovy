package org.apache.nifi.processor

import groovy.json.JsonSlurper
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
}
