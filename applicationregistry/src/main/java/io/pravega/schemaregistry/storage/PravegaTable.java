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

import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.Version;
import io.pravega.controller.store.stream.VersionedMetadata;
import io.pravega.schemaregistry.storage.client.TableStore;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.pravega.schemaregistry.storage.client.TableStore.EMPTY;

/**
 * Pravega tables based table implementation. 
 */
public class PravegaTable implements Table<Version> {
    private static final TableKeySerializer KEY_SERIALIZER = new TableKeySerializer();

    private final TableStore tablesStore;
    private final String tableName;

    public PravegaTable(String tableName, TableStore tablesStore) {
        this.tablesStore = tablesStore;
        this.tableName = tableName;
    }

    @Override
    public Etag<Version> toEtag(Version version) {
        return () -> version;
    }

    @Override
    public Version fromEtag(Etag etag) {
        return (Version) etag.etag();
    }

    @Override
    public CompletableFuture<Map<TableKey, TableValue>> getAllRecords() {
        return tablesStore.getAllEntries(tableName, x -> x)
                          .thenApply(entries -> entries.stream().map(x -> {
                              TableKey key = KEY_SERIALIZER.fromString(x.getKey());
                              TableValue value = Table.fromBytes(key.getClass(), x.getValue().getObject(), TableValue.class);
                              return new ImmutablePair<>(key, value);
                          }).collect(Collectors.toMap(ImmutablePair::getLeft, ImmutablePair::getRight)));
    }

    private CompletableFuture<Void> addEntry(TableKey key, TableValue value) {
        return Futures.toVoid(tablesStore.addNewEntryIfAbsent(tableName, KEY_SERIALIZER.toKeyString(key), value.toBytes()));
    }

    private CompletableFuture<Void> updateEntry(TableKey key, TableValue value, Version version) {
        String keyStr = KEY_SERIALIZER.toKeyString(key);
        return Futures.toVoid(tablesStore.updateEntry(tableName, keyStr, value.toBytes(), version));
    }

    @Override
    public CompletableFuture<Void> updateEntries(Map<TableKey, ValueWithVersion<TableValue, Version>> entries) {
        Map<String, Map.Entry<byte[], Version>> batch = entries.entrySet().stream().collect(Collectors.toMap(
                x -> KEY_SERIALIZER.toKeyString(x.getKey()),
                x -> {
                    ValueWithVersion<TableValue, Version> valueWithVersion = x.getValue();
                    return new AbstractMap.SimpleEntry<>(valueWithVersion.getValue().toBytes(), valueWithVersion.getVersion());
                }));
        return Futures.toVoid(tablesStore.updateEntries(tableName, batch));
    }
    
    @Override
    public <T extends TableValue> CompletableFuture<T> getRecord(TableKey key, Class<T> tClass) {
        return Futures.exceptionallyExpecting(
                tablesStore.getEntry(tableName, KEY_SERIALIZER.toKeyString(key), x -> Table.fromBytes(key.getClass(), x, tClass))
                           .thenApply(VersionedMetadata::getObject),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException,
                null);
    }

    @Override
    public <T extends TableValue> CompletableFuture<List<ValueWithVersion<T, Version>>> getRecords(List<? extends TableKey> keys, Class<T> tClass) {
        return tablesStore.getEntries(tableName,
                keys.stream().map(KEY_SERIALIZER::toKeyString).collect(Collectors.toList()))
                          .thenApply(values -> {
                              List<ValueWithVersion<T, Version>> result = new ArrayList<>(keys.size());
                              for (int i = 0; i < keys.size(); i++) {
                                  TableKey key = keys.get(i);
                                  T value;
                                  Version version;
                                  VersionedMetadata<byte[]> versionedMetadata = values.get(i);
                                  if (!versionedMetadata.equals(EMPTY)) {
                                      value = Table.fromBytes(key.getClass(), versionedMetadata.getObject(), tClass);
                                      version = versionedMetadata.getVersion();
                                  } else {
                                      value = null;
                                      version = null;
                                  }
                                  result.add(new ValueWithVersion<>(value, version));
                              }
                              return result;
                          });
    }

    @Override
    public CompletableFuture<Void> addRecord(TableKey key, TableValue value) {
        return tablesStore.addNewEntryIfAbsent(tableName, KEY_SERIALIZER.toKeyString(key), value.toBytes());
    }

    @Override
    public CompletableFuture<Void> deleteRecord(TableKey key) {
        return tablesStore.removeEntry(tableName, KEY_SERIALIZER.toKeyString(key));
    }

    @Override
    public <T extends TableValue> CompletableFuture<ValueWithVersion<T, Version>> getRecordWithVersion(TableKey key, Class<T> tClass) {
        return Futures.exceptionallyExpecting(tablesStore.getEntry(tableName, KEY_SERIALIZER.toKeyString(key),
                x -> Table.fromBytes(key.getClass(), x, tClass))
                                                         .thenApply(entry -> new ValueWithVersion<>(entry.getObject(), entry.getVersion())),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException,
                new ValueWithVersion<>(null, null));
    }
}
