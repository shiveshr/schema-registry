/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.test.samples;

import com.google.common.collect.ImmutableMap;
import io.pravega.common.Exceptions;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.exceptions.IncompatibleSchemaException;
import io.pravega.schemaregistry.service.SchemaRegistryService;
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
        SchemaRegistryService service = new SchemaRegistryService(store, executor);
        SchemaRegistryClient client = new PassthruRegistryClient(service);
        
        String group = "group";

        int groupsCount = client.listGroups().size();
        
        client.addGroup(group, SchemaType.Avro,  
                SchemaValidationRules.of(Compatibility.backward()), 
                true, Collections.emptyMap());
        assertEquals(client.listGroups().size(), groupsCount + 1);

        String myTest = "MyTest";
        SchemaInfo schemaInfo = new SchemaInfo(myTest, SchemaType.Avro, 
                schema1.toString().getBytes(Charsets.UTF_8), ImmutableMap.of());

        client.addSchema(group, schemaInfo);

        // attempt to add an existing schema
        client.addSchema(group, schemaInfo);

        SchemaInfo schemaInfo2 = new SchemaInfo(myTest, SchemaType.Avro,
                schema2.toString().getBytes(Charsets.UTF_8), ImmutableMap.of());
        client.addSchema(group, schemaInfo2);

        client.updateSchemaValidationRules(group, SchemaValidationRules.of(Compatibility.fullTransitive()));

        SchemaInfo schemaInfo3 = new SchemaInfo(myTest, SchemaType.Avro,
                schema3.toString().getBytes(Charsets.UTF_8), ImmutableMap.of());

        AtomicReference<Throwable> exceptionRef = new AtomicReference<>();
        CompletableFuture.supplyAsync(() -> client.addSchema(group, schemaInfo3))
                         .exceptionally(e -> {
                             exceptionRef.set(Exceptions.unwrap(e));
                             return null;
                         }).join();
        
        assertTrue(exceptionRef.get() instanceof IncompatibleSchemaException);
        
        String myTest2 = "MyTest2";
        SchemaInfo schemaInfo4 = new SchemaInfo(myTest2, SchemaType.Avro,
                schemaTest2.toString().getBytes(Charsets.UTF_8), ImmutableMap.of());
        client.addSchema(group, schemaInfo4);

        List<String> objectTypes = client.getSchemaNames(group);
        assertEquals(objectTypes.size(), 2);
        assertTrue(objectTypes.contains(myTest));
        assertTrue(objectTypes.contains(myTest2));
        List<GroupHistoryRecord> groupEvolutionHistory = client.getGroupHistory(group);
        assertEquals(groupEvolutionHistory.size(), 3);
        List<SchemaWithVersion> myTestHistory = client.getSchemaVersions(group, myTest);
        assertEquals(myTestHistory.size(), 2);
        List<SchemaWithVersion> myTest2History = client.getSchemaVersions(group, myTest2);
        assertEquals(myTest2History.size(), 1);
    }

    abstract SchemaStore getStore();

}

