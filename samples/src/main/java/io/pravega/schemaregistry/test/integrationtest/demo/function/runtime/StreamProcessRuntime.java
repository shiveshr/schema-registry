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

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.AbstractIdleService;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.GroupIdGenerator;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.serializers.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.schemaregistry.test.integrationtest.demo.function.interfaces.SerDe;
import io.pravega.shared.NameUtils;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

class StreamProcessRuntime<I, O> extends AbstractIdleService {
    private final ClientConfig clientConfig;
    private final SchemaRegistryClient client;
    private final String inputScope;
    private final String outputStreamScope;
    private final String inputStream;
    private final String outputStream;
    private final String inputGroupId;
    private final String outputGroupId;
    private final StreamProcess<I, O> streamProcess;
    private final List<Cell> cells;
    
    StreamProcessRuntime(ClientConfig clientConfig, SchemaRegistryClient client, StreamProcess<I, O> streamProcess) {
        this.clientConfig = clientConfig;
        this.client = client;
        this.inputScope = streamProcess.getInputStream().getScope();
        this.inputStream = streamProcess.getInputStream().getStream();
        this.outputStreamScope = streamProcess.getOutputStream().getScope();
        this.outputStream = streamProcess.getOutputStream().getStream();
        this.inputGroupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, inputScope, inputStream);
        this.outputGroupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, outputStreamScope, outputStream);
        this.streamProcess = streamProcess;
        this.cells = new ArrayList<>(streamProcess.getParallelism());
    }

    @Override
    protected final void startUp() {
        String readerGroupName = "rg" + inputStream + System.currentTimeMillis();
        createReaderGroup(readerGroupName);
        IntStream.of(streamProcess.getParallelism()).parallel().forEach(x -> {
            Cell cell = new Cell(createReader(readerGroupName, Integer.toString(x)), createWriter());
            cells.add(cell);
            cell.startAsync();
            cell.awaitRunning();
        });
    }

    @Override
    protected void shutDown() {
        cells.forEach(cell -> {
            cell.stopAsync();
            cell.awaitTerminated();
        });
    }

    private class Cell extends AbstractExecutionThreadService implements StreamProcess.Context {
        private final EventStreamReader<I> inputReader;
        private final EventStreamWriter<O> outputWriter;

        private Cell(EventStreamReader<I> inputReader, EventStreamWriter<O> outputWriter) {
            this.inputReader = inputReader;
            this.outputWriter = outputWriter;
        }

        @Override
        protected void run() {
            EventRead<I> event;
            while (isRunning()) {
                event = inputReader.readNextEvent(1000);
                if (event.getEvent() != null) {
                    O output = streamProcess.process(event.getEvent(), this);

                    if (output != null) {
                        if (streamProcess.getRoutingKeyFunction() == null) {
                            outputWriter.writeEvent(output).join();    
                        } else {
                            String routingKey = streamProcess.getRoutingKeyFunction().apply(output);
                            outputWriter.writeEvent(routingKey, output).join();
                        }
                    }
                }
            }
        }
    }

    @SneakyThrows
    private EventStreamWriter<O> createWriter() {
        SerializerConfig config = SerializerConfig.builder()
                                                  .groupId(outputGroupId)
                                                  .autoRegisterSchema(true)
                                                  .registryConfigOrClient(Either.right(client))
                                                  .build();
        SerDe<O> serDe = streamProcess.getOutputStream().getSerDe();
        Serializer<O> serializer = SerializerFactory.customSerializer(config, serDe.getSchema(), serDe.getSerializer());

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(outputStreamScope, clientConfig);
        return clientFactory.createEventWriter(outputStream, serializer, EventWriterConfig.builder().build());
    }

    private void createReaderGroup(String readerGroupName) {
        try (ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(inputScope, clientConfig, new ConnectionFactoryImpl(clientConfig))) {
            readerGroupManager.createReaderGroup(readerGroupName,
                    ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(inputScope, inputStream)).disableAutomaticCheckpoints().build());
        }
    }
    
    @SuppressWarnings("unchecked")
    @SneakyThrows
    private EventStreamReader<I> createReader(String readerGroupName, String readerName) {
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(inputGroupId)
                                                            .registryConfigOrClient(Either.right(client))
                                                            .build();

        SerDe<I> serDe = streamProcess.getInputStream().getSerDe();
        Serializer<I> deserializer = SerializerFactory.customDeserializer(serializerConfig, serDe.getSchema(), serDe.getDeserializer());

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(inputScope, clientConfig);

        return clientFactory.createReader(readerName, readerGroupName, deserializer, ReaderConfig.builder().build());
    }
}
