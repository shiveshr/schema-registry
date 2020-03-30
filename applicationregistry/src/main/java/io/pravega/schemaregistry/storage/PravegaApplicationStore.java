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
import io.pravega.schemaregistry.storage.client.TableStore;
import lombok.val;

import java.util.Collections;
import java.util.HashMap;
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
        ApplicationRecord.ApplicationValue applicationValue = new ApplicationRecord.ApplicationValue(Collections.emptyMap(), Collections.emptyMap(), 
                properties);
        return withCreateTableIfAbsent(APPLICATIONS, () -> tableStore.addNewEntryIfAbsent(APPLICATIONS, appId, applicationValue.toBytes()));
    }

    @Override
    public CompletableFuture<Application> getApplication(String appId) {
        return withCreateTableIfAbsent(APPLICATIONS, () -> tableStore.getEntry(APPLICATIONS, appId, ApplicationRecord.ApplicationValue::fromBytes)
                         .thenCompose(x -> {
                             ApplicationRecord.ApplicationValue appValue = x.getObject();
                             // go to the group table and fetch the key against app id
                             // writerId -> groupId ==> groupId -> List<Map.Entry<WriterId, GroupId>>
                             val writerIdByGroup = appValue.getWritingTo().entrySet().stream().collect(Collectors.groupingBy(Map.Entry::getValue));
                             
                             CompletableFuture<Map<String, List<Application.Writer>>> writersFuture = Futures.allOfWithResults(
                                     writerIdByGroup.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                                             entry -> {
                                                 Group<Version> grp = getGroupObj(entry.getKey());
                                                 List<String> writerIds = entry.getValue().stream().map(Map.Entry::getKey).collect(Collectors.toList());
                                                 return grp.getWritersForApp(appId, writerIds);
                                             })));

                             val readerIdByGroup = appValue.getReadingFrom().entrySet().stream().collect(Collectors.groupingBy(Map.Entry::getValue));
                             
                             CompletableFuture<Map<String, List<Application.Reader>>> readersFuture = Futures.allOfWithResults(
                                     readerIdByGroup .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                                             entry -> {
                                                 Group<Version> grp = getGroupObj(entry.getKey());
                                                 List<String> readerIds = entry.getValue().stream().map(Map.Entry::getKey).collect(Collectors.toList());
                                                 return grp.getReadersForApp(appId, readerIds);
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
    public CompletableFuture<Void> addWriter(String appId, String groupId, Application.Writer writer, Etag etag) {
        return tableStore.getEntry(APPLICATIONS, appId, ApplicationRecord.ApplicationValue::fromBytes)
                .thenCompose(entry -> {
                    if (entry == null) {
                        throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, String.format("%s not found", appId));
                    }
                    ApplicationRecord.ApplicationValue existing = entry.getObject();
                    HashMap<String, String> writingTo = new HashMap<>(existing.getWritingTo());
                    writingTo.put(writer.getWriterId(), groupId);
                    ApplicationRecord.ApplicationValue newVal = new ApplicationRecord.ApplicationValue(writingTo,
                            existing.getReadingFrom(), existing.getProperties());
                    return Futures.toVoid(tableStore.updateEntry(APPLICATIONS, appId, newVal.toBytes(), entry.getVersion()));
                })
                .thenCompose(v -> {
                    // create group table if not exist
                    Group<Version> group = getGroupObj(groupId);
                    return withCreateTableIfAbsent(getGroupTableName(groupId), () -> group.addWriter(appId, writer.getWriterId(), 
                            writer.getVersionInfos(), writer.getCodecType(), etag));
                });
    }

    @Override
    public CompletableFuture<Void> addReader(String appId, String groupId, Application.Reader reader, Etag etag) {
        return tableStore.getEntry(APPLICATIONS, appId, ApplicationRecord.ApplicationValue::fromBytes)
                         .thenCompose(entry -> {
                             if (entry == null) {
                                 throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, String.format("%s not found", appId));
                             }
                             ApplicationRecord.ApplicationValue existing = entry.getObject();
                             HashMap<String, String> readingFrom = new HashMap<>(existing.getReadingFrom());
                             readingFrom.put(reader.getReaderId(), groupId);
                             ApplicationRecord.ApplicationValue newVal = new ApplicationRecord.ApplicationValue(
                                     existing.getWritingTo(), readingFrom, existing.getProperties());
                             return Futures.toVoid(tableStore.updateEntry(APPLICATIONS, appId, newVal.toBytes(), entry.getVersion()));
                         })
                         .thenCompose(v -> {
                             // create group table if not exist
                             Group<Version> group = getGroupObj(groupId);
                             return withCreateTableIfAbsent(getGroupTableName(groupId), () -> group.addReader(appId, 
                                     reader.getReaderId(), reader.getVersionInfos(), reader.getCodecs(), etag));
                         });
    }

    @Override
    public CompletableFuture<Void> removeWriter(String appId, String writerId) {
        // get app --> get group for writer --> remove writer from group --> remove writer from app. 
        // if we die before we are able to remove writer from app, no problem, it will just be a stale entry that is 
        // harmless. getApplication call will gc such stale writers/readers
        return withCreateTableIfAbsent(APPLICATIONS, () -> tableStore.getEntry(APPLICATIONS, appId, ApplicationRecord.ApplicationValue::fromBytes))
                .thenCompose(app -> {
                    String groupId = app.getObject().getWritingTo().get(writerId);
                    
                    if (groupId != null) {
                        Group<Version> group = getGroupObj(groupId);

                        return withCreateTableIfAbsent(getGroupTableName(groupId), () -> group.removeWriter(appId, writerId))
                                .whenComplete((r, e) -> {
                                    // we will do this asynchronously and even if we fail to remove the writer from app its harmless.
                                    if (e != null) {
                                        Map<String, String> writingTo = new HashMap<>(app.getObject().getWritingTo());
                                        writingTo.remove(writerId);
                                        ApplicationRecord.ApplicationValue applicationValue = new ApplicationRecord.ApplicationValue(
                                                writingTo, app.getObject().getReadingFrom(), app.getObject().getProperties());
                                        tableStore.updateEntry(APPLICATIONS, appId, applicationValue.toBytes(), app.getVersion());
                                    }
                                });
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    @Override
    public CompletableFuture<Void> removeReader(String appId, String readerId) {
        // get app --> get group for reader --> remove reader from group --> remove reader from app. 
        // if we die before we are able to remove writer from app, no problem, it will just be a stale entry that is 
        // harmless. getApplication call will gc such stale writers/readers
        return withCreateTableIfAbsent(APPLICATIONS, () -> tableStore.getEntry(APPLICATIONS, appId, ApplicationRecord.ApplicationValue::fromBytes))
                .thenCompose(app -> {
                    String groupId = app.getObject().getReadingFrom().get(readerId);

                    if (groupId != null) {
                        Group<Version> group = getGroupObj(groupId);

                        return withCreateTableIfAbsent(getGroupTableName(groupId), () -> group.removeReader(appId, readerId))
                                .whenComplete((r, e) -> {
                                    // we will do this asynchronously and even if we fail to remove the writer from app its harmless.
                                    if (e != null) {
                                        Map<String, String> readingFrom = new HashMap<>(app.getObject().getReadingFrom());
                                        readingFrom.remove(readerId);
                                        ApplicationRecord.ApplicationValue applicationValue = new ApplicationRecord.ApplicationValue(
                                                app.getObject().getWritingTo(), readingFrom, app.getObject().getProperties());
                                        tableStore.updateEntry(APPLICATIONS, appId, applicationValue.toBytes(), app.getVersion());
                                    }
                                });
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });    
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
