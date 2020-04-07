/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.test.integrationtest.demo.function;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.concurrent.Futures;
import io.pravega.schemaregistry.GroupIdGenerator;
import io.pravega.schemaregistry.client.RegistryClientFactory;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.serializers.PravegaDeserializer;
import io.pravega.schemaregistry.serializers.PravegaSerializer;
import io.pravega.schemaregistry.serializers.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.schemaregistry.test.integrationtest.demo.serde.MyPojo;
import io.pravega.schemaregistry.test.integrationtest.demo.serde.MySerializer;
import io.pravega.schemaregistry.test.integrationtest.demo.util.LoaderUtil;
import io.pravega.shared.NameUtils;
import lombok.SneakyThrows;

import java.net.URI;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class FunctionRuntime {
    private final ClientConfig clientConfig;
    private final SchemaRegistryClient client;
    private final String scope;
    private final String inputStream;
    private final String outputStream;
    private final String inputGroupId;
    private final String outputGroupId;

    private FunctionRuntime() {
        clientConfig = ClientConfig.builder().controllerURI(URI.create("tcp://localhost:9090")).build();
        SchemaRegistryClientConfig config = new SchemaRegistryClientConfig(URI.create("http://localhost:9092"));
        client = RegistryClientFactory.createRegistryClient(config);
        this.scope = UUID.randomUUID().toString();
        this.inputStream = UUID.randomUUID().toString();
        this.outputStream = UUID.randomUUID().toString();
        this.inputGroupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, inputStream);
        this.outputGroupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, outputStream);
        createScopeAndStream(scope, inputStream, inputGroupId);
        createScopeAndStream(scope, outputStream, outputGroupId);
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    public static void main(String[] args) {
        String functionName = "io.pravega.schemaregistry.test.integrationtest.demo.function.MyFunction";
        String inputDeserializer = "io.pravega.schemaregistry.test.integrationtest.demo.serde.MyDeserializer";
        String outputSerializer = "io.pravega.schemaregistry.test.integrationtest.demo.function.StringSerializer";

        URL funcFilepath = Paths.get("/home/shivesh/function/function.jar").toUri().toURL(); 
        URL serDeFilepath = Paths.get("/home/shivesh/serde.jar").toUri().toURL(); 
        // background thread to write some data into input stream
        Function func = LoaderUtil.getInstance(functionName, funcFilepath, Function.class);
        PravegaDeserializer deserializer = LoaderUtil.getInstance(inputDeserializer, serDeFilepath, PravegaDeserializer.class);
        PravegaSerializer serializer = LoaderUtil.getInstance(outputSerializer, funcFilepath, PravegaSerializer.class);
        
        FunctionRuntime runtime = new FunctionRuntime();
        MySerializer mySerializer = new MySerializer();
        EventStreamWriter<MyPojo> writer = runtime.createWriter(runtime.inputGroupId, runtime.inputStream, mySerializer);
        AtomicInteger counter = new AtomicInteger();
        Futures.loop(() -> true, () -> {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                
            }
            return writer.writeEvent(new MyPojo(Integer.toString(counter.incrementAndGet())));
        }, Executors.newSingleThreadExecutor());
        
        // loop --> read from inputstream
        EventStreamReader<Object> reader = runtime.createReader(runtime.inputGroupId, runtime.inputStream, deserializer);
        EventStreamWriter writer2 = runtime.createWriter(runtime.outputGroupId, runtime.outputStream, serializer);
        EventRead<Object> event;
        do {
            event = reader.readNextEvent(1000);
            if (event.getEvent() != null) {
                Object output = func.apply(event.getEvent());
                writer2.writeEvent(output).join();
            }
        } while(event.getEvent() != null);
    }

    private void createScopeAndStream(String scope, String stream, String groupId) {
        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        SchemaType schemaType = SchemaType.custom("serDe");
        client.addGroup(groupId, schemaType,
                SchemaValidationRules.of(Compatibility.denyAll()),
                false, Collections.singletonMap(SerializerFactory.ENCODE, Boolean.toString(false)));
    }

    @SneakyThrows
    private <T> EventStreamWriter<T> createWriter(String groupId, String stream, PravegaSerializer<T> mySerializer) {
        SerializerConfig config = SerializerConfig.builder()
                                                  .groupId(groupId)
                                                  .autoRegisterSchema(true)
                                                  .registryConfigOrClient(Either.right(client))
                                                  .build();
        SchemaType schemaType = SchemaType.custom("serDe");

        SchemaInfo schemaInfo = new SchemaInfo("serde", schemaType, new byte[0], Collections.emptyMap());

        Serializer<T> serializer = SerializerFactory.customSerializer(config, () -> schemaInfo, mySerializer);

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        return clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    private <T> EventStreamReader<T> createReader(String groupId, String stream, PravegaDeserializer<T> myDeserializer) {
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .registryConfigOrClient(Either.right(client))
                                                            .build();

        // region read into specific schema
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String rg = "rg" + stream + System.currentTimeMillis();
        readerGroupManager.createReaderGroup(rg,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());
        
        Serializer<T> deserializer = SerializerFactory.customDeserializer(serializerConfig, null, myDeserializer);

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        return clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());
        // endregion
    }

}
