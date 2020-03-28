/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage;

import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.Version;
import io.pravega.schemaregistry.contract.data.Application;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.storage.client.TableStore;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class PravegaApplicationStore implements ApplicationStore {
    private static final String APPLICATIONS = "registryService/applications/0";
    private static final String GROUP_TABLE_NAME_FORMAT = "applications-%s/table/0";

    private final TableStore tableStore;

    public PravegaApplicationStore(TableStore tableStore) {
        this.tableStore = tableStore;
    } 

    @Override
    public CompletableFuture<Void> addApplication(String appId, Map<String, String> properties) {
        // add application to the applications table
        ApplicationRecord.ApplicationValue applicationValue = new ApplicationRecord.ApplicationValue(Collections.emptyList(), Collections.emptyList(), 
                properties);
        return withCreateTableIfAbsent(APPLICATIONS, () -> tableStore.addNewEntryIfAbsent(APPLICATIONS, appId, applicationValue.toBytes()));
    }

    @Override
    public CompletableFuture<Application> getApplication(String appId) {
        return withCreateTableIfAbsent(APPLICATIONS, () -> tableStore.getEntry(APPLICATIONS, appId, ApplicationRecord.ApplicationValue::fromBytes)
                         .thenCompose(x -> {
                             ApplicationRecord.ApplicationValue appValue = x.getObject();
                             // go to the group table and fetch the key against app id

                             CompletableFuture<Map<String, List<Application.Reader>>> readersFuture = Futures.allOfWithResults(
                                     appValue.getReadingFrom().stream().collect(Collectors.toMap(group -> group,
                                             group -> {
                                                 Group<Version> grp = getGroupObj(group);
                                                 return grp.getReaderSchemasForApp(appId);
                                             })));
                             
                             CompletableFuture<Map<String, List<Application.Writer>>> writersFuture = Futures.allOfWithResults(
                                     appValue.getReadingFrom().stream().collect(Collectors.toMap(group -> group,
                                             group -> {
                                                 Group<Version> grp = getGroupObj(group);
                                                 return grp.getWriterSchemasForApp(appId);
                                             })));
                             
                             return CompletableFuture.allOf(readersFuture, writersFuture)
                                .thenApply(v -> {
                                    Map<String, List<Application.Reader>> readers = readersFuture.join();
                                    Map<String, List<Application.Writer>> writers = writersFuture.join();
                                    return new Application(appId, writers, readers, appValue.getProperties());
                                });
                         }));
    }

    @Override
    public CompletableFuture<ReadersInGroupWithEtag> getReaderApps(String groupId) {
        Group<Version> group = getGroupObj(groupId);
        return withCreateTableIfAbsent(getGroupTableName(groupId), group::getReaderApps);
    }

    @Override
    public CompletableFuture<WritersInGroupWithEtag> getWriterApps(String groupId) {
        Group<Version> group = getGroupObj(groupId);
        return withCreateTableIfAbsent(getGroupTableName(groupId), group::getWriterApps);
    }

    @Override
    public CompletableFuture<Void> addWriter(String appId, String groupId, VersionInfo schemaVersion, CodecType codecType, Etag etag) {
        return tableStore.getEntry(APPLICATIONS, appId, ApplicationRecord.ApplicationValue::fromBytes)
                .thenCompose(entry -> {
                    if (entry == null) {
                        throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, String.format("%s not found", appId));
                    }
                    ApplicationRecord.ApplicationValue existing = entry.getObject();
                    if (existing.getWritingTo().contains(groupId)) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        List<String> writingTo = new LinkedList<>();
                        writingTo.addAll(existing.getWritingTo());
                        writingTo.add(groupId);
                        ApplicationRecord.ApplicationValue newVal = new ApplicationRecord.ApplicationValue(writingTo, 
                                existing.getReadingFrom(), existing.getProperties());
                        return Futures.toVoid(tableStore.updateEntry(APPLICATIONS, appId, newVal.toBytes(), entry.getVersion()));
                    }
                })
                .thenCompose(v -> {
                    // create group table if not exist
                    Group<Version> group = getGroupObj(groupId);
                    return withCreateTableIfAbsent(getGroupTableName(groupId), () -> group.addWriter(appId, schemaVersion, codecType, etag));
                });
    }

    @Override
    public CompletableFuture<Void> addReader(String appId, String groupId, VersionInfo schemaVersion, List<CodecType> codecs, Etag etag) {
        return tableStore.getEntry(APPLICATIONS, appId, ApplicationRecord.ApplicationValue::fromBytes)
                         .thenCompose(entry -> {
                             if (entry == null) {
                                 throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, String.format("%s not found", appId));
                             }
                             ApplicationRecord.ApplicationValue existing = entry.getObject();
                             if (existing.getWritingTo().contains(groupId)) {
                                 return CompletableFuture.completedFuture(null);
                             } else {
                                 List<String> readingFrom = new LinkedList<>();
                                 readingFrom.addAll(existing.getReadingFrom());
                                 readingFrom.add(groupId);
                                 ApplicationRecord.ApplicationValue newVal = new ApplicationRecord.ApplicationValue(
                                         existing.getWritingTo(), readingFrom, existing.getProperties());
                                 return Futures.toVoid(tableStore.updateEntry(APPLICATIONS, appId, newVal.toBytes(), entry.getVersion()));
                             }
                         })
                         .thenCompose(v -> {
                             // create group table if not exist
                             Group<Version> group = getGroupObj(groupId);
                             return withCreateTableIfAbsent(getGroupTableName(groupId), () -> group.addReader(appId, schemaVersion, codecs, etag));
                         });
    }

    @Override
    public CompletableFuture<Void> removeWriter(String appId, String groupId) {
        Group<Version> group = getGroupObj(groupId);
        return withCreateTableIfAbsent(getGroupTableName(groupId), () -> group.removeWriter(appId));
    }

    @Override
    public CompletableFuture<Void> removeReader(String appId, String groupId) {
        Group<Version> group = getGroupObj(groupId);
        return withCreateTableIfAbsent(getGroupTableName(groupId), () -> group.removeReader(appId));
    }

    private Group<Version> getGroupObj(String groupId) {
        String tableName = getGroupTableName(groupId);
        PravegaTable table = new PravegaTable(tableName, tableStore);
        return new Group<>(table);
    }

    private String getGroupTableName(String groupId) {
        return String.format(GROUP_TABLE_NAME_FORMAT, groupId);
    }

    private <T> CompletableFuture<T> withCreateTableIfAbsent(String tableName, Supplier<CompletableFuture<T>> supplier) {
        return Futures.exceptionallyComposeExpecting(supplier.get(),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataContainerNotFoundException,
                () -> tableStore.createTable(tableName).thenCompose(v -> supplier.get()));
    }
}
