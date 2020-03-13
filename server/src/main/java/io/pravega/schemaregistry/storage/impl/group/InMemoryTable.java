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

import io.pravega.common.concurrent.Futures;
import io.pravega.schemaregistry.storage.Position;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.records.TableRecords;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Synchronized;

import javax.annotation.concurrent.GuardedBy;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * In memory implementation of records. 
 */
public class InMemoryTable implements Table<Integer> {
    @GuardedBy("$lock")
    @Getter(AccessLevel.NONE)
    private final Map<TableRecords.Key, Value<TableRecords.Record, Integer>> records = new HashMap<>();

    @Override
    @Synchronized
    public CompletableFuture<Collection<TableRecords.Key>> getAllKeys() {
        return CompletableFuture.completedFuture(records.keySet());
    }

    @Override
    @Synchronized
    public CompletableFuture<List<Entry>> getAllEntries() {
        return CompletableFuture.completedFuture(
                records.entrySet().stream().map(x -> new Entry(x.getKey(), x.getValue().getValue())).collect(Collectors.toList()));
    }

    @Override
    @Synchronized
    public CompletableFuture<List<Entry>> getAllEntries(Predicate<TableRecords.Key> filterKeys) {
        return CompletableFuture.completedFuture(records.entrySet().stream().filter(x -> filterKeys.test(x.getKey()))
                                                        .map(x -> new Entry(x.getKey(), x.getValue().getValue())).collect(Collectors.toList()));
    }

    @Override
    @Synchronized
    public CompletableFuture<Void> addEntry(TableRecords.Key key, TableRecords.Record value) {
        records.putIfAbsent(key, new Value<>(value, 0));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    @Synchronized
    public CompletableFuture<Void> updateEntry(TableRecords.Key key, TableRecords.Record value, Integer version) {
        int currentVersion = records.containsKey(key) ? records.get(key).getVersion() : 0;
        if (currentVersion != version) {
            throw StoreExceptions.create(StoreExceptions.Type.WRITE_CONFLICT, key.getClass().toString());
        } else {
            records.put(key, new Value<>(value, version + 1));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> updateEntries(List<Map.Entry<TableRecords.Key, Value>> value) {
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    @Synchronized
    public <T extends TableRecords.Record> CompletableFuture<T> getRecord(TableRecords.Key key, Class<T> tClass) {
        if (!records.containsKey(key)) {
            return CompletableFuture.completedFuture(null);
        }

        Value<TableRecords.Record, Integer> value = records.get(key);
        if (tClass.isAssignableFrom(value.getValue().getClass())) {
            return CompletableFuture.completedFuture((T) value.getValue());
        } else {
            return Futures.failedFuture(new IllegalArgumentException());
        }
    }

    @Override
    public <T extends TableRecords.Record> CompletableFuture<List<T>> getRecords(List<TableRecords.Key> keys, Class<T> tClass) {
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    @Synchronized
    public <T extends TableRecords.Record> CompletableFuture<Value<T, Integer>> getRecordWithVersion(TableRecords.Key key, Class<T> tClass) {
        if (!records.containsKey(key)) {
            return CompletableFuture.completedFuture(null);
        }

        Value<? extends TableRecords.Record, Integer> value = records.get(key);
        if (tClass.isAssignableFrom(value.getValue().getClass())) {
            return CompletableFuture.completedFuture(new Value<>((T) value.getValue(), value.getVersion()));
        } else {
            return Futures.failedFuture(new IllegalArgumentException());
        }
    }

    @Override
    @Synchronized
    public Position versionToPosition(Integer version) {
        return new InMemoryPosition(version);
    }
}
