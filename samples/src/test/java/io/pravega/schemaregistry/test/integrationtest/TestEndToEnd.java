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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.pravega.common.Exceptions;
import io.pravega.schemaregistry.client.RegistryClient;
import io.pravega.schemaregistry.contract.data.Application;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.SchemaEvolution;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.contract.exceptions.IncompatibleSchemaException;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.schemaregistry.service.ApplicationRegistryService;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.schemaregistry.storage.ApplicationStore;
import io.pravega.schemaregistry.storage.SchemaStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.curator.shaded.com.google.common.base.Charsets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public abstract class TestEndToEnd {
    protected ScheduledExecutorService executor;

    private final Schema schema1 = SchemaBuilder
            .record("MyTest")
            .fields()
            .name("a")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .endRecord();

    private final Schema schema2 = SchemaBuilder
            .record("MyTest")
            .fields()
            .name("a")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("b")
            .type(Schema.create(Schema.Type.STRING))
            .withDefault("backward compatible with schema1")
            .endRecord();

    private final Schema schema3 = SchemaBuilder
            .record("MyTest")
            .fields()
            .name("a")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("b")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .endRecord();
    
    private final Schema schema3Compatible = SchemaBuilder
            .record("MyTest")
            .fields()
            .name("a")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("b")
            .type(Schema.create(Schema.Type.STRING))
            .withDefault("default")
            .endRecord();

    private final Schema schemaTest2 = SchemaBuilder
            .record("MyTest2")
            .fields()
            .name("a")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("b")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .endRecord();
    
    @Before
    public void setUp() {
        executor = Executors.newScheduledThreadPool(10);    
    }
    
    @After
    public void tearDown() {
        executor.shutdownNow();
    }
    
    @Test
    public void testEndToEnd() {
        SchemaStore store = getStore();
        ApplicationStore appStore = getAppStore();
        SchemaRegistryService service = new SchemaRegistryService(store, executor);
        ApplicationRegistryService appService = new ApplicationRegistryService(appStore);
        RegistryClient client = new PassthruRegistryClient(service, appService);
        
        String group = "group";

        int groupsCount = client.listGroups().size();
        
        client.addGroup(group, SchemaType.Avro,  
                SchemaValidationRules.of(Compatibility.backward()), 
                true, Collections.singletonMap(SerializerFactory.ENCODE, Boolean.toString(true)));
        assertEquals(client.listGroups().size(), groupsCount + 1);

        String myTest = "MyTest";
        SchemaInfo schemaInfo = new SchemaInfo(myTest, SchemaType.Avro, 
                schema1.toString().getBytes(Charsets.UTF_8), ImmutableMap.of());

        client.addSchemaIfAbsent(group, schemaInfo);

        // attempt to add an existing schema
        VersionInfo version1 = client.addSchemaIfAbsent(group, schemaInfo);

        SchemaInfo schemaInfo2 = new SchemaInfo(myTest, SchemaType.Avro,
                schema2.toString().getBytes(Charsets.UTF_8), ImmutableMap.of());
        VersionInfo version2 = client.addSchemaIfAbsent(group, schemaInfo2);

        client.updateSchemaValidationRules(group, SchemaValidationRules.of(Compatibility.fullTransitive()));

        SchemaInfo schemaInfo3 = new SchemaInfo(myTest, SchemaType.Avro,
                schema3.toString().getBytes(Charsets.UTF_8), ImmutableMap.of());

        AtomicReference<Throwable> exceptionRef = new AtomicReference<>();
        CompletableFuture.supplyAsync(() -> client.addSchemaIfAbsent(group, schemaInfo3))
                         .exceptionally(e -> {
                             exceptionRef.set(Exceptions.unwrap(e));
                             return null;
                         }).join();
        
        assertTrue(exceptionRef.get() instanceof IncompatibleSchemaException);
        
        String myTest2 = "MyTest2";
        SchemaInfo schemaInfo4 = new SchemaInfo(myTest2, SchemaType.Avro,
                schemaTest2.toString().getBytes(Charsets.UTF_8), ImmutableMap.of());
        VersionInfo version4 = client.addSchemaIfAbsent(group, schemaInfo4);

        List<String> objectTypes = client.getObjectTypes(group);
        assertEquals(objectTypes.size(), 2);
        assertTrue(objectTypes.contains(myTest));
        assertTrue(objectTypes.contains(myTest2));
        List<SchemaEvolution> groupEvolutionHistory = client.getGroupEvolutionHistory(group, null);
        assertEquals(groupEvolutionHistory.size(), 3);
        List<SchemaEvolution> myTestHistory = client.getGroupEvolutionHistory(group, myTest);
        assertEquals(myTestHistory.size(), 2);
        List<SchemaEvolution> myTest2History = client.getGroupEvolutionHistory(group, myTest2);
        assertEquals(myTest2History.size(), 1);

        client.updateSchemaValidationRules(group, SchemaValidationRules.of(Compatibility.backward()));

        client.addApplication("myapp", Collections.emptyMap());
        
        client.addWriter("myapp", group, new Application.Writer("writer1", Collections.singletonList(version1), CodecType.None));
        client.addWriter("myapp", group, new Application.Writer("writer2", Collections.singletonList(version2), CodecType.None));
        // we should not be allowed to add a reader older than latest writer.
        List<CodecType> codecs = Lists.newArrayList(CodecType.None, CodecType.Snappy, CodecType.GZip);
        try {
            client.addReader("myapp", group, new Application.Reader("reader1", Collections.singletonList(version1), codecs));
        } catch (Exception e) {
            Throwable unwrap = Exceptions.unwrap(e);
            assertTrue(unwrap instanceof IncompatibleSchemaException);
        }
        client.addReader("myapp", group, new Application.Reader("reader1", Collections.singletonList(version2), codecs));

        SchemaInfo schemaInfo3Compatible = new SchemaInfo(myTest, SchemaType.Avro,
                schema3Compatible.toString().getBytes(Charsets.UTF_8), ImmutableMap.of());
        VersionInfo version3 = client.addSchemaIfAbsent(group, schemaInfo3Compatible);

        // this should fail as no reader has moved to v3 yet
        try {
            client.addWriter("myapp", group, new Application.Writer("writer2", Collections.singletonList(version3), CodecType.None));
        } catch (Exception e) {
            Throwable unwrap = Exceptions.unwrap(e);
            assertTrue(unwrap instanceof IncompatibleSchemaException);
        }
        
        // this should be allowed as this is on a different object.
        client.addWriter("myapp", group, new Application.Writer("writer2", Collections.singletonList(version4), CodecType.None));
        
        // TODO: add more tests for codecs and reader ahead of writer for forward compatibility etc
    }

    abstract SchemaStore getStore();

    abstract ApplicationStore getAppStore();
}

