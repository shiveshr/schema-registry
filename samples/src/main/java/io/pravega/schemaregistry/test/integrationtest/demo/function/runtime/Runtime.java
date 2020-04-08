/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.test.integrationtest.demo.function.runtime;

import com.google.common.util.concurrent.AbstractIdleService;
import io.pravega.client.ClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClient;

import java.util.List;
import java.util.stream.Collectors;

public class Runtime<I, O> extends AbstractIdleService {
    private final ClientConfig clientConfig;
    private final SchemaRegistryClient client;
    private final Pipeline<I, O> processPipeline;
    private List<StreamProcessRuntime> executionFlow;

    public Runtime(ClientConfig clientConfig, SchemaRegistryClient client, Pipeline<I, O> pipeline) {
        this.clientConfig = clientConfig;
        this.client = client;
        this.processPipeline = pipeline;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected final void startUp() {
        executionFlow = processPipeline.getPipeline().stream()
                                       .map(x -> {
                                           StreamProcessRuntime streamProcessRuntime = new StreamProcessRuntime(clientConfig, client, x);
                                           streamProcessRuntime.startAsync();
                                           streamProcessRuntime.awaitRunning();
                                           return streamProcessRuntime;
                                       })
                                       .collect(Collectors.toList());
    }

    @Override
    protected void shutDown() {
        executionFlow.forEach(x -> {
            x.stopAsync();
            x.awaitTerminated();
        });
    }
}