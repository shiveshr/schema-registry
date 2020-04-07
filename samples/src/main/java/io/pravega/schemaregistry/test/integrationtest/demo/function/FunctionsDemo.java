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

import com.google.common.collect.Lists;
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
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.serializers.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.schemaregistry.test.integrationtest.demo.function.test.MyInput;
import io.pravega.schemaregistry.test.integrationtest.demo.function.test.MySerDe;
import io.pravega.schemaregistry.test.integrationtest.demo.function.test.ToLowerFunction;
import io.pravega.schemaregistry.test.integrationtest.demo.function.test.WordCount;
import io.pravega.schemaregistry.test.integrationtest.demo.function.test.WordCountSerDe;
import io.pravega.schemaregistry.test.integrationtest.demo.function.runtime.Processing;
import io.pravega.schemaregistry.test.integrationtest.demo.function.runtime.Runtime;
import io.pravega.shared.NameUtils;
import lombok.SneakyThrows;

import java.net.URI;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class FunctionsDemo {
    private static final String SAMPLE1 = "To handle this, applications would want to use serialization systems that support schemas and allow for evolution of schemas. With schemas, they can define the structure of the data in an event while ensuring that both upstream and downstream applications use the correct structure. And over time, evolve this schema to incorporate new business requirements. Without a schema evolution support, if the upstream writers were updated to publish newer structures of data, it may break downstream readers if they were running with an older version of the data.";
    private static final String SAMPLE2 = "Pravega streams store sequences of bytes and pravega does not validate the information in these events. However, applications typically require encoding this information in a structured format. And as the business needs change, this structure may also need to change/evolve to handle new requirements.";
    private static final String SAMPLE3 = "When schemas are allowed to change in compatible fashion then downstream applications continue to consume data without worrying about breaking because of unexpected changes to structures.";
    private static final String SAMPLE4 = "In the absence of schema management support from streaming storage layer, applications have to build an out of band coordination mechanism for potentially exchanging and evolving schemas for the data in the stream. Given that schema management is a common requirement of applications working with streams, it is desirable to perform it at the Streaming Storage layer.";

    @SuppressWarnings("unchecked")
    @SneakyThrows
    public static void main(String[] args) {
        String toLowerFunc = ToLowerFunction.class.getName();
        String wordCountFunc = WordCount.class.getName();
        String inputSerDe = MySerDe.class.getName();
        String outputSerDe = WordCountSerDe.class.getName();

        URL funcFilepath = Paths.get("/home/shivesh/function/function.jar").toUri().toURL();
        URL serDeFilepath = Paths.get("/home/shivesh/function/serDe.jar").toUri().toURL();
        // background thread to write some data into input stream

        // region create stream and write data into it
        String scope = UUID.randomUUID().toString();
        String inputStream = UUID.randomUUID().toString();
        String outputStream = UUID.randomUUID().toString();
        String inputGroupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, inputStream);
        String outputGroupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, outputStream);

        ClientConfig clientConfig = ClientConfig.builder().controllerURI(URI.create("tcp://localhost:9090")).build();
        SchemaRegistryClient srClient = RegistryClientFactory.createRegistryClient(new SchemaRegistryClientConfig(URI.create("http://localhost:9092")));

        createScopeAndStream(clientConfig, srClient, scope, inputStream, inputGroupId, SchemaType.custom("myPojo"));
        createScopeAndStream(clientConfig, srClient, scope, outputStream, outputGroupId, SchemaType.custom("string"));

        generateTestDataIntoInputStream(inputGroupId, scope, inputStream, clientConfig, srClient, 20,
                Lists.newArrayList(SAMPLE1, SAMPLE2, SAMPLE3, SAMPLE4));
        // endregion
        
        // region processing
        Processing<MyInput, Map<String, Integer>> process = new Processing.ProcessingBuilder<MyInput, Map<String, Integer>>()
                .inputStream(scope, inputStream, inputSerDe, serDeFilepath)
                .map(x -> ((MyInput) x).getText())
                .map(toLowerFunc, funcFilepath)
                .map(x -> ((String) x).split("\\W+"))
                .windowedMap(wordCountFunc, funcFilepath, 2)
                .outputStream(scope, outputStream, outputSerDe, serDeFilepath)
                .build();
        
        Runtime runtime = new Runtime(clientConfig, srClient, process);
        runtime.startAsync();
        runtime.awaitRunning();
        // endregion
        
        // region read events
        WordCountSerDe wordCountSerDe = new WordCountSerDe();
        SerializerConfig config = SerializerConfig.builder()
                                                  .groupId(outputGroupId)
                                                  .registryConfigOrClient(Either.right(srClient))
                                                  .build();
        Serializer<Map<String, Integer>> deserializer = SerializerFactory.customDeserializer(config, null, wordCountSerDe.getDeserializer());

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String rg = "rg" + outputStream + System.currentTimeMillis();
        readerGroupManager.createReaderGroup(rg,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, outputStream)).disableAutomaticCheckpoints().build());

        EventStreamReader<Map<String, Integer>> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());
        List<Map<String, Integer>> result = new LinkedList<>();
        EventRead<Map<String, Integer>> event = reader.readNextEvent(1000);
        int counter = 1;
        while (event.getEvent() != null) {
            System.out.println("\n\n " + counter++ + " Window output \n\n" + event.getEvent());
            result.add(event.getEvent());
            event = reader.readNextEvent(1000);
        }
        assert result.size() == 10;
        generateTestDataIntoInputStream(inputGroupId, scope, inputStream, clientConfig, srClient, 5, Lists.newArrayList("one"));
        event = reader.readNextEvent(1000);
        while (event.getEvent() != null) {
            System.out.println("\n\n " + counter++ + " Window output \n\n" + event.getEvent());
            result.add(event.getEvent());
            event = reader.readNextEvent(1000);
        }
        assert result.size() == 12;
        
        runtime.stopAsync();
        runtime.awaitTerminated();
        System.exit(0);
        // endregion
    }

    private static void createScopeAndStream(ClientConfig clientConfig, SchemaRegistryClient srClient, String scope, String stream, String groupId, SchemaType schemaType) {
        try (StreamManager streamManager = new StreamManagerImpl(clientConfig)) {
            streamManager.createScope(scope);
            streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

            srClient.addGroup(groupId, schemaType,
                    SchemaValidationRules.of(Compatibility.denyAll()),
                    false, Collections.singletonMap(SerializerFactory.ENCODE, Boolean.toString(false)));
        }
    }

    private static void generateTestDataIntoInputStream(String groupId, String scope, String stream,
                                                        ClientConfig clientConfig, SchemaRegistryClient client, int numberOfEvents, List<String> text) {
        MySerDe mySerDe = new MySerDe();
        SerializerConfig config = SerializerConfig.builder()
                                                  .groupId(groupId)
                                                  .autoRegisterSchema(true)
                                                  .registryConfigOrClient(Either.right(client))
                                                  .build();
        Serializer<MyInput> serializer = SerializerFactory.customSerializer(config, mySerDe.getSchema(), mySerDe.getSerializer());

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        EventStreamWriter<MyInput> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());

        AtomicInteger counter = new AtomicInteger();
        Futures.loop(() -> counter.getAndIncrement() < numberOfEvents, () -> {
            MyInput event = new MyInput(text.get(counter.get() % text.size()));
            return writer.writeEvent(event);
        }, Executors.newSingleThreadExecutor()).join();
    }
}
