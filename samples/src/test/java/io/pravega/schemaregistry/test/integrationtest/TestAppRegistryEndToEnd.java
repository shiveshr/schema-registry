/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.test.integrationtest;

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.schemaregistry.GroupIdGenerator;
import io.pravega.schemaregistry.client.RegistryClient;
import io.pravega.schemaregistry.codec.Codec;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.exceptions.CodecMismatchException;
import io.pravega.schemaregistry.schemas.AvroSchema;
import io.pravega.schemaregistry.serializers.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.schemaregistry.service.ApplicationRegistryService;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.schemaregistry.storage.ApplicationStore;
import io.pravega.schemaregistry.storage.ApplicationStoreFactory;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.schemaregistry.storage.SchemaStoreFactory;
import io.pravega.schemaregistry.test.integrationtest.generated.Type1;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Test;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertTrue;

@Slf4j
public class TestAppRegistryEndToEnd implements AutoCloseable {
    private static final String MYCOMPRESSION = "mycompression";
    private static final Codec MY_CODEC = new Codec() {
        @Override
        public CodecType getCodecType() {
            return CodecType.custom(MYCOMPRESSION, Collections.emptyMap());
        }

        @Override
        public ByteBuffer encode(ByteBuffer data) {
            // left rotate by 1 byte
            byte[] array = new byte[data.remaining()];
            data.get(array);

            int i;
            byte temp = array[0];
            for (i = 0; i < array.length - 1; i++) {
                array[i] = array[i + 1];
            }
            array[array.length - 1] = temp;
            return ByteBuffer.wrap(array);
        }

        @Override
        public ByteBuffer decode(ByteBuffer data) {
            byte[] array = new byte[data.remaining()];
            data.get(array);

            int i;
            byte temp = array[array.length - 1];
            for (i = array.length - 1; i > 0; i--) {
                array[i] = array[i - 1];
            }
            array[0] = temp;
            return ByteBuffer.wrap(array);
        }
    };

    private final ClientConfig clientConfig;

    private final SchemaStore schemaStore;
    private final ScheduledExecutorService executor;
    private final SchemaRegistryService service;
    private final ApplicationRegistryService appService;
    private final RegistryClient client;
    private final PravegaStandaloneUtils pravegaStandaloneUtils;
    private final ApplicationStore appStore;
    private Random random;

    public TestAppRegistryEndToEnd() throws Exception {
        pravegaStandaloneUtils = PravegaStandaloneUtils.startPravega();
        executor = Executors.newScheduledThreadPool(10);

        clientConfig = ClientConfig.builder().controllerURI(URI.create(pravegaStandaloneUtils.getControllerURI())).build();

        schemaStore = SchemaStoreFactory.createPravegaStore(clientConfig, executor);
        appStore = ApplicationStoreFactory.createPravegaStore(clientConfig, executor);
        service = new SchemaRegistryService(schemaStore, executor);
        appService = new ApplicationRegistryService(appStore);
        client = new PassthruRegistryClient(service, appService);
        random = new Random();
    }
    
    @Override
    @After
    public void close() throws Exception {
        executor.shutdownNow();
    }
    
    @Test
    public void testCodec() {
        // create stream
        String scope = "appscope";
        String stream = "appcodec" + System.currentTimeMillis();
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);

        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        streamManager.close();
        SchemaType schemaType = SchemaType.Avro;
        client.addGroup(groupId, schemaType,
                SchemaValidationRules.of(Compatibility.backward()),
                true, Collections.singletonMap(SerializerFactory.ENCODE, Boolean.toString(true)));
        client.addApplication("app", Collections.emptyMap());
        client.addApplication("app2", Collections.emptyMap());

        // 1. add a writer with custom codec 
        // this should fail
        // 2. add codec
        // 3. add a writer with custom codec
        AvroSchema<Type1> schema1 = AvroSchema.of(Type1.class);

        SerializerConfig serializerConfig = SerializerConfig.builder()
                                           .groupId(groupId)
                                           .autoRegisterSchema(true)
                                           .autoRegisterCodec(true)
                                           .registryConfigOrClient(Either.right(client))
                                           .application("app")
                                           .codec(MY_CODEC)
                                           .build();

        SerializerFactory.avroSerializer(serializerConfig, schema1);

        SerializerConfig serializerConfig2 = SerializerConfig.builder()
                                                             .groupId(groupId)
                                                             .registryConfigOrClient(Either.right(client))
                                                             .application("app2")
                                                             .build();

        boolean exceptionPredicate = false;
        try {
            SerializerFactory.avroDeserializer(serializerConfig2, AvroSchema.of(Type1.class));
        } catch (Exception e) {
            exceptionPredicate = Exceptions.unwrap(e) instanceof CodecMismatchException;
        }
        assertTrue(exceptionPredicate);

        serializerConfig2 = SerializerConfig.builder()
                                            .groupId(groupId)
                                            .registryConfigOrClient(Either.right(client))
                                            .application("app2")
                                            .addDecoder(MY_CODEC.getCodecType(), MY_CODEC::decode)
                                            .build();
        SerializerFactory.avroDeserializer(serializerConfig2, AvroSchema.of(Type1.class));
    }
}

