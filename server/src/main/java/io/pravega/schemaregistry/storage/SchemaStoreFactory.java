/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage;

import io.pravega.client.ClientConfig;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.storage.client.TableStore;
import io.pravega.schemaregistry.storage.impl.SchemaStoreImpl;
import io.pravega.schemaregistry.storage.impl.groups.InMemoryGroups;
import io.pravega.schemaregistry.storage.impl.groups.PravegaKVGroups;
import org.apache.commons.lang3.NotImplementedException;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Factory for creating schema store of different types. 
 */
public class SchemaStoreFactory {
    public static SchemaStore createInMemoryStore(ScheduledExecutorService executor) {
        return new SchemaStoreImpl<>(new InMemoryGroups(executor));
    }
    
    public static SchemaStore createPravegaStore(ServiceConfig serviceConfig, ClientConfig clientConfig, ScheduledExecutorService executor) {
        TableStore tableStore = new TableStore(clientConfig, () -> retrieveMasterToken(serviceConfig), executor);
        return new SchemaStoreImpl<>(new PravegaKVGroups(tableStore, executor));
    }

    private static String retrieveMasterToken(ServiceConfig config) {
        if (config.isAuthEnabled()) {
            throw new NotImplementedException("Auth");
        } else {
            return "";
        }
    }

}
