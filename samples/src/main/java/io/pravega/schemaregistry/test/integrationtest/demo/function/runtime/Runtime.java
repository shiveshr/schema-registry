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

public class Runtime<I, O> extends AbstractExecutionThreadService {
    private final ClientConfig clientConfig;
    private final SchemaRegistryClient client;
    private final String inputStreamScope;
    private final String outputStreamScope;
    private final String inputStream;
    private final String outputStream;
    private final String inputGroupId;
    private final String outputGroupId;
    private final StreamProcess<I, O> streamProcess;
    private EventStreamReader<I> inputReader;
    private EventStreamWriter<O> outputWriter;

    public Runtime(ClientConfig clientConfig, SchemaRegistryClient client, StreamProcess<I, O> streamProcess) {
        this.clientConfig = clientConfig;
        this.client = client;
        this.inputStreamScope = streamProcess.getInputStream().getScope();
        this.inputStream = streamProcess.getInputStream().getStream();
        this.outputStreamScope = streamProcess.getOutputStream().getScope();
        this.outputStream = streamProcess.getOutputStream().getStream();
        this.inputGroupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, inputStreamScope, inputStream);
        this.outputGroupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, outputStreamScope, outputStream);
        this.streamProcess = streamProcess;
    }

    @Override
    protected final void startUp() {
        inputReader = createReader();
        outputWriter = createWriter();
    }

    @Override
    protected void run() {
        EventRead<I> event;
        while (isRunning()) {
            event = inputReader.readNextEvent(1000);
            if (event.getEvent() != null) {
                O output = streamProcess.process(event.getEvent());

                if (output != null) {
                    outputWriter.writeEvent(output).join();
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

    @SuppressWarnings("unchecked")
    @SneakyThrows
    private EventStreamReader<I> createReader() {
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(inputGroupId)
                                                            .registryConfigOrClient(Either.right(client))
                                                            .build();

        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(inputStreamScope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String rg = "rg" + inputStream + System.currentTimeMillis();
        readerGroupManager.createReaderGroup(rg,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(inputStreamScope, inputStream)).disableAutomaticCheckpoints().build());
        SerDe<I> serDe = streamProcess.getInputStream().getSerDe();
        Serializer<I> deserializer = SerializerFactory.customDeserializer(serializerConfig, serDe.getSchema(), serDe.getDeserializer());

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(inputStreamScope, clientConfig);

        return clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());
    }
}
