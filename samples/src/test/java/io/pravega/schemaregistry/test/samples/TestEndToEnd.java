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
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.contract.exceptions.IncompatibleSchemaException;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.test.common.AssertExtensions;
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
        SchemaRegistryClient client = new PassthruSchemaRegistryClient(service);
        
        String group = "group";

        int groupsCount = client.listGroups(group).size();
        
        client.addGroup(group, group, SchemaType.Avro,  
                SchemaValidationRules.of(Compatibility.backward()), 
                true, Collections.emptyMap());
        assertEquals(client.listGroups(group).size(), groupsCount + 1);

        String myTest = "MyTest";
        SchemaInfo schemaInfo = new SchemaInfo(myTest, SchemaType.Avro, 
                schema1.toString().getBytes(Charsets.UTF_8), ImmutableMap.of());

        VersionInfo version1 = client.addSchema(group, group, schemaInfo);
        assertEquals(version1.getVersion(), 0);
        assertEquals(version1.getOrdinal(), 0);
        assertEquals(version1.getSchemaName(), myTest);
        // attempt to add an existing schema
        version1 = client.addSchema(group, group, schemaInfo);
        assertEquals(version1.getVersion(), 0);
        assertEquals(version1.getOrdinal(), 0);
        assertEquals(version1.getSchemaName(), myTest);

        SchemaInfo schemaInfo2 = new SchemaInfo(myTest, SchemaType.Avro,
                schema2.toString().getBytes(Charsets.UTF_8), ImmutableMap.of());
        VersionInfo version2 = client.addSchema(group, group, schemaInfo2);
        assertEquals(version2.getVersion(), 1);
        assertEquals(version2.getOrdinal(), 1);
        assertEquals(version2.getSchemaName(), myTest);

        client.updateSchemaValidationRules(group, group, SchemaValidationRules.of(Compatibility.fullTransitive()));

        SchemaInfo schemaInfo3 = new SchemaInfo(myTest, SchemaType.Avro,
                schema3.toString().getBytes(Charsets.UTF_8), ImmutableMap.of());

        AtomicReference<Throwable> exceptionRef = new AtomicReference<>();
        CompletableFuture.supplyAsync(() -> client.addSchema(group, group, schemaInfo3))
                         .exceptionally(e -> {
                             exceptionRef.set(Exceptions.unwrap(e));
                             return null;
                         }).join();
        
        assertTrue(exceptionRef.get() instanceof IncompatibleSchemaException);
        
        String myTest2 = "MyTest2";
        SchemaInfo schemaInfo4 = new SchemaInfo(myTest2, SchemaType.Avro,
                schemaTest2.toString().getBytes(Charsets.UTF_8), ImmutableMap.of());
        VersionInfo version3 = client.addSchema(group, group, schemaInfo4);
        assertEquals(version3.getVersion(), 0);
        assertEquals(version3.getOrdinal(), 2);
        assertEquals(version3.getSchemaName(), myTest2);

        List<String> schemaNames = client.getSchemaNames(group, group);
        assertEquals(schemaNames.size(), 2);
        assertTrue(schemaNames.contains(myTest));
        assertTrue(schemaNames.contains(myTest2));
        List<GroupHistoryRecord> groupEvolutionHistory = client.getGroupHistory(group, group);
        assertEquals(group, groupEvolutionHistory.size(), 3);
        List<SchemaWithVersion> myTestHistory = client.getSchemaVersions(group, group, myTest);
        assertEquals(myTestHistory.size(), 2);
        List<SchemaWithVersion> myTest2History = client.getSchemaVersions(group, group, myTest2);
        assertEquals(myTest2History.size(), 1);
        
        // delete schemainfo2
        EncodingId encodingId = client.getEncodingId(group, group, version2, CodecType.None);
        assertEquals(encodingId.getId(), 0);
        client.deleteSchemaVersion(group, group, version2);
        SchemaInfo schema = client.getSchemaForVersion(group, group, version2);
        assertEquals(schema, schemaInfo2);
        AssertExtensions.assertThrows("", () -> client.getVersionForSchema(group, group, schemaInfo2), 
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException);
        encodingId = client.getEncodingId(group, group, version2, CodecType.None);
        assertEquals(encodingId.getId(), 0);
        AssertExtensions.assertThrows("", () -> client.getEncodingId(group, group, version2, CodecType.GZip),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException);
        
        groupEvolutionHistory = client.getGroupHistory(group, group);
        assertEquals(group, groupEvolutionHistory.size(), 2);

        myTestHistory = client.getSchemaVersions(group, group, myTest);
        assertEquals(myTestHistory.size(), 1);
        SchemaWithVersion schemaWithVersion = client.getLatestSchemaVersion(group, group, myTest);
        assertEquals(schemaWithVersion.getVersion(), version1);
        
        schemaWithVersion = client.getLatestSchemaVersion(group, group, null);
        assertEquals(schemaWithVersion.getVersion(), version3);
        
        // add the schema again. it should get a new version
        VersionInfo version4 = client.addSchema(group, group, schemaInfo2);
        assertEquals(version4.getOrdinal(), 3);
        assertEquals(version4.getVersion(), 2);
    }

    abstract SchemaStore getStore();

}

