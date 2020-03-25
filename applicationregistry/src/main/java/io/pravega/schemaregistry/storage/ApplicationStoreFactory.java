/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage;

import io.pravega.client.ClientConfig;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.schemaregistry.storage.client.TableStore;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Factory for creating schema store of different types.
 */
public class ApplicationStoreFactory {
    public static ApplicationStore createInMemoryStore(ScheduledExecutorService executor) {
        return new InmemoryApplicationStore();
    }

    public static ApplicationStore createPravegaStore(ClientConfig clientConfig, ScheduledExecutorService executor) {
        TableStore tableStore = new TableStore(clientConfig, GrpcAuthHelper.getDisabledAuthHelper(), executor);
        return new PravegaApplicationStore(tableStore);
    }
}
