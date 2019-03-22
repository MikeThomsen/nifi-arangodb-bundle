package org.apache.nifi.processor

import org.junit.Before
import org.junit.Test

class QueryArangoDBIT extends AbstractArangoDBIT {
    @Before
    void setup() {
        super.setup(QueryArangoDB.class)
    }

    @Test
    void testCountQuery() {

    }
}
