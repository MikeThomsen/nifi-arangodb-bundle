package org.apache.nifi.controller;

import com.arangodb.ArangoDB;

public interface ArangoDBClientService extends ControllerService {
    ArangoDB getConnection();
}
