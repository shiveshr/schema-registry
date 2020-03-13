/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl.group;

import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.Version;
import io.pravega.schemaregistry.storage.Position;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.client.TableStore;
import io.pravega.schemaregistry.storage.records.IndexKeySerializer;
import io.pravega.schemaregistry.storage.records.TableRecords;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Pravega tables based index implementation. 
 */
public class PravegaTableTable implements Table<Version> {
    public static final String TABLE_NAME_FORMAT = "table-%s/index/0";
    private static final IndexKeySerializer INDEX_KEY_SERIALIZER = new IndexKeySerializer();
    
    private final TableStore tablesStore;
    private final String tableName;

    public PravegaTableTable(String groupName, String id, TableStore tablesStore) {
        this.tablesStore = tablesStore;
        this.tableName = getIndexName(groupName, id); 
    }

    private static String getIndexName(String groupName, String id) {
        return String.format(TABLE_NAME_FORMAT, id);
    }
    
    public CompletableFuture<Void> create() {
        // create new table
        return tablesStore.createTable(tableName);
    }
    
    public CompletableFuture<Void> delete() {
        // delete the table
        return tablesStore.deleteTable(tableName, false);
    }
    
    @Override
    public CompletableFuture<Collection<TableRecords.Key>> getAllKeys() {
        return tablesStore.getAllKeys(tableName)
                          .thenApply(list -> list.stream().map(INDEX_KEY_SERIALIZER::fromString).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<List<Entry>> getAllEntries() {
        return getAllEntries(x -> true);
    }

    @Override
    public CompletableFuture<List<Entry>> getAllEntries(Predicate<TableRecords.Key> filterKeys) {

        return tablesStore.getAllEntries(tableName, x -> x)
                          .thenApply(entries -> entries.stream().map( 
                                  x -> {
                                      TableRecords.Key key = INDEX_KEY_SERIALIZER.fromString(x.getKey());
                                      TableRecords.Record record = TableRecords.fromBytes(key.getClass(), x.getValue().getObject());
                                      return new Entry(key, record);
                                  }).filter(x -> filterKeys.test(x.getKey())).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Void> addEntry(TableRecords.Key key, TableRecords.Record value) {
        return Futures.toVoid(tablesStore.addNewEntryIfAbsent(tableName, INDEX_KEY_SERIALIZER.toKeyString(key), value.toBytes()));
    }

    @Override
    public CompletableFuture<Void> updateEntry(TableRecords.Key key, TableRecords.Record value, Version version) {
        String keyStr = INDEX_KEY_SERIALIZER.toKeyString(key);
         return Futures.toVoid(tablesStore.updateEntry(tableName, keyStr, value.toBytes(), version));
    }

    @Override
    public <T extends TableRecords.Record> CompletableFuture<T> getRecord(TableRecords.Key key, Class<T> tClass) {
        return Futures.exceptionallyExpecting(
                tablesStore.getEntry(tableName, INDEX_KEY_SERIALIZER.toKeyString(key), x -> TableRecords.fromBytes(key.getClass(), x))
                          .thenApply(entry -> getTypedRecord(tClass, entry.getObject())), 
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException, 
                null);
    }

    @SuppressWarnings("unchecked")
    private <T extends TableRecords.Record> T getTypedRecord(Class<T> tClass, TableRecords.Record value) {
        if (tClass.isAssignableFrom(value.getClass())) {
            return (T) value;
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public <T extends TableRecords.Record> CompletableFuture<Value<T, Version>> getRecordWithVersion(TableRecords.Key key, Class<T> tClass) {
        return Futures.exceptionallyExpecting(tablesStore.getEntry(tableName, INDEX_KEY_SERIALIZER.toKeyString(key), x -> TableRecords.fromBytes(key.getClass(), x))
                   .thenApply(entry -> {
                       T t = getTypedRecord(tClass, entry.getObject());
                       return new Value<>(t, entry.getVersion());
                   }), 
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException, 
                null);
    }
}
