/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl.group;

import io.pravega.schemaregistry.storage.Position;
import io.pravega.schemaregistry.storage.records.TableRecords;
import lombok.Data;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

/**
 * Table for all records stored for a group. The index can be in memory or backed by a key value table.
 * The {@link Log} is the source of truth about the state of {@link Group} and index is a convenience to optimize query 
 * responses. The index can lag behind the log but will eventually catch up. 
 * @param <V> Version of index
 */
public interface Table<V> {
    CompletableFuture<Collection<TableRecords.Key>> getAllKeys();

    CompletableFuture<List<Entry>> getAllEntries();

    CompletableFuture<List<Entry>> getAllEntries(Predicate<TableRecords.Key> filterKeys);

    CompletableFuture<Void> addEntry(TableRecords.Key key, TableRecords.Record value);

    CompletableFuture<Void> updateEntry(TableRecords.Key key, TableRecords.Record value, V version);
    
    CompletableFuture<Void> updateEntries(List<Map.Entry<TableRecords.Key, Value>> value);

    <T extends TableRecords.Record> CompletableFuture<T> getRecord(TableRecords.Key key, Class<T> tClass);
    
    <T extends TableRecords.Record> CompletableFuture<List<T>> getRecords(List<TableRecords.Key> keys, Class<T> tClass);

    <T extends TableRecords.Record> CompletableFuture<Value<T, V>> getRecordWithVersion(TableRecords.Key key, Class<T> tClass);

    CompletableFuture<Position> getCurrentEtag();

    @Data
    class Value<T extends TableRecords.Record, V> {
        private final T value;
        private final V version;
    }

    @Data
    class Entry {
        private final TableRecords.Key key;
        private final TableRecords.Record value;
    }
}
