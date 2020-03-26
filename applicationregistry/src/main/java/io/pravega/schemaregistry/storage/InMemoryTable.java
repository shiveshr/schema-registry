/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Synchronized;

import javax.annotation.concurrent.GuardedBy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * In memory implementation of {@link Table} for storing resource metadata. 
 */
public class InMemoryTable implements Table<Integer> {
    @GuardedBy("$lock")
    @Getter(AccessLevel.NONE)
    private final Map<TableKey, ValueWithVersion<TableValue, Integer>> table = new HashMap<>();
    
    @Synchronized
    @Override
    public CompletableFuture<Map<TableKey, TableValue>> getAllRecords() {
        return CompletableFuture.completedFuture(
                table.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, x -> x.getValue().getValue())));
    }

    @Synchronized
    private CompletableFuture<Void> updateEntry(TableKey key, TableValue value, Integer version) {
        CompletableFuture<Void> ret = new CompletableFuture<>();
        ValueWithVersion<TableValue, Integer> val = table.get(key);
        if ((val == null && version == null) || (val != null && version.equals(val.getVersion()))) {
            int newVersion = version == null ? 0 : version + 1;
            table.put(key, new ValueWithVersion<>(value, newVersion));
            ret.complete(null);
        } else {
            ret.completeExceptionally(StoreExceptions.create(StoreExceptions.Type.WRITE_CONFLICT, "key"));
        }
        return ret;
    }

    @Synchronized
    @Override
    public CompletableFuture<Void> updateEntries(Map<TableKey, ValueWithVersion<TableValue, Integer>> updates) {
        CompletableFuture<Void> ret = new CompletableFuture<>();

        boolean isValid = updates.entrySet().stream().allMatch(update -> {
            TableKey key = update.getKey();
            Integer version = update.getValue().getVersion();
            ValueWithVersion<TableValue, Integer> val = table.get(key);
            return (val == null && version == null) || (val != null && version.equals(val.getVersion()));
        });
        
        if (isValid) {
            updates.forEach((x, y) -> {
                updateEntry(x, y.getValue(), y.getVersion()).join();
            });
            ret.complete(null);            
        } else {
            ret.completeExceptionally(StoreExceptions.create(StoreExceptions.Type.WRITE_CONFLICT, "key"));
        }
        return ret;
    }

    @Synchronized
    @Override
    public <T extends TableValue> CompletableFuture<T> getRecord(TableKey key, Class<T> tClass) {
        return getRecordWithVersion(key, tClass).thenApply(ValueWithVersion::getValue);
    }

    @Synchronized
    @Override
    public CompletableFuture<Void> addRecord(TableKey key, TableValue value) {
        return updateEntry(key, value, null); 
    }

    @Synchronized
    @Override
    public CompletableFuture<Void> deleteRecord(TableKey key) {
        table.remove(key);
        return CompletableFuture.completedFuture(null);
    }

    @Synchronized
    @Override
    @SuppressWarnings("unchecked")
    public <T extends TableValue> CompletableFuture<ValueWithVersion<T, Integer>> getRecordWithVersion(
            TableKey key, Class<T> tClass) {
        ValueWithVersion<TableValue, Integer> val = table.get(key);
        if (val == null) {
            return CompletableFuture.completedFuture(null);
        } else if (val.getValue().getClass().isAssignableFrom(tClass)) {
            return CompletableFuture.completedFuture(new ValueWithVersion<>((T) val.getValue(), val.getVersion()));
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public Etag toEtag(Integer version) {
        return () -> version;
    }

    @Override
    public Integer fromEtag(Etag etag) {
        return (Integer) etag.etag();
    }

}
