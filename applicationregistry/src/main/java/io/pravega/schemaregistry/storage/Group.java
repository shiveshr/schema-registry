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

import com.google.common.collect.Lists;
import io.pravega.schemaregistry.contract.data.VersionInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class Group<V> {
    private static final Table.TableEtag ETAG = new Table.TableEtag();
    
    private final Table<V> table;

    Group(Table<V> table) {
        this.table = table;
    }

    private CompletableFuture<Etag> getGroupEtag() {
        return table.getRecordWithVersion(ETAG, Table.TableEtag.class)
                .thenCompose(etagWithVersion -> {
                    if (etagWithVersion == null) {
                        return table.addRecord(ETAG, ETAG).thenCompose(v -> getGroupEtag());       
                    } else {
                        return CompletableFuture.completedFuture(table.toEtag(etagWithVersion.getVersion()));
                    }
                });
    }

    CompletableFuture<AppsInGroupWithEtag> getReaderApps() {
        Map<String, List<VersionInfo>> readers = new HashMap<>();
        return getGroupEtag()
                .thenCompose(etag -> table.getAllRecords()
                                          .thenApply(records -> {
                                              records.forEach((x, y) -> {
                                                  if (x instanceof Table.Reader) {
                                                      String appId = ((Table.Reader) x).getAppId();
                                                      Table.SchemaVersions value = (Table.SchemaVersions) y;
                                                      readers.put(appId, Lists.newArrayList(value.getVersions().values()));
                                                  }
                                              });
                                              return new AppsInGroupWithEtag(readers, etag);
                                          }));
    }
    
    CompletableFuture<AppsInGroupWithEtag> getWriterApps() {
        Map<String, List<VersionInfo>> writers = new HashMap<>();
        return getGroupEtag()
                .thenCompose(etag -> table.getAllRecords()
                                          .thenApply(records -> {
                                              records.forEach((x, y) -> {
                                                  if (x instanceof Table.Writer) {
                                                      String appId = ((Table.Writer) x).getAppId();
                                                      Table.SchemaVersions value = (Table.SchemaVersions) y;
                                                      writers.put(appId, Lists.newArrayList(value.getVersions().values()));
                                                  }
                                              });
                                              return new AppsInGroupWithEtag(writers, etag);
                                          }));

    }

    CompletableFuture<List<VersionInfo>> getReaderSchemasForApp(String applicationId) {
        return table.getRecord(new Table.Reader(applicationId), Table.SchemaVersions.class)
                    .thenApply(record -> {
                        if (record == null) {
                            return Collections.emptyList();
                        } else {
                            return Lists.newArrayList(record.getVersions().values());
                        }
                    });
    }

    CompletableFuture<List<VersionInfo>> getWriterSchemasForApp(String applicationId) {
        return table.getRecord(new Table.Reader(applicationId), Table.SchemaVersions.class)
                    .thenApply(record -> {
                        if (record == null) {
                            return Collections.emptyList();
                        } else {
                            return Lists.newArrayList(record.getVersions().values());
                        }
                    });
    }
    
    CompletableFuture<Void> addWriter(String appId, VersionInfo schemaVersion, Etag etag) {
        Table.Writer key = new Table.Writer(appId);
        return table.getRecordWithVersion(key, Table.SchemaVersions.class)
                .thenCompose(recordWithVersion -> {
                    Table.SchemaVersions next;
                    V nextVersion = recordWithVersion == null ? null : recordWithVersion.getVersion();
                    if (recordWithVersion == null) {
                        // add a new record
                        next = new Table.SchemaVersions(Collections.singletonMap(schemaVersion.getSchemaName(), schemaVersion));    
                    } else {
                        Table.SchemaVersions record = recordWithVersion.getValue();
                        VersionInfo existing = record.getVersions().get(schemaVersion.getSchemaName());
                        if (existing != null && existing.equals(schemaVersion)) {
                            return CompletableFuture.completedFuture(null);
                        } else {
                            Map<String, VersionInfo> map = new HashMap<>(record.getVersions());
                            map.put(schemaVersion.getSchemaName(), schemaVersion);
                            next = new Table.SchemaVersions(map);
                        }
                    }
                    Map<Table.TableKey, Table.ValueWithVersion<Table.TableValue, V>> entries = new HashMap<>();

                    entries.put(key, new Table.ValueWithVersion<>(next, nextVersion));
                    entries.put(ETAG, new Table.ValueWithVersion<>(ETAG, table.fromEtag(etag)));

                    return table.updateEntries(entries);
                });
    }

    CompletableFuture<Void> addReader(String appId, VersionInfo schemaVersion, Etag etag) {
        Table.Reader key = new Table.Reader(appId);
        return table.getRecordWithVersion(key, Table.SchemaVersions.class)
                    .thenCompose(recordWithVersion -> {
                        Table.SchemaVersions next;
                        V nextVersion = recordWithVersion == null ? null : recordWithVersion.getVersion();
                        if (recordWithVersion == null) {
                            // add a new record
                            next = new Table.SchemaVersions(Collections.singletonMap(schemaVersion.getSchemaName(), schemaVersion));
                        } else {
                            Table.SchemaVersions record = recordWithVersion.getValue();
                            VersionInfo existing = record.getVersions().get(schemaVersion.getSchemaName());
                            if (existing != null && existing.equals(schemaVersion)) {
                                return CompletableFuture.completedFuture(null);
                            } else {
                                Map<String, VersionInfo> map = new HashMap<>(record.getVersions());
                                map.put(schemaVersion.getSchemaName(), schemaVersion);
                                next = new Table.SchemaVersions(map);
                            }
                        }
                        Map<Table.TableKey, Table.ValueWithVersion<Table.TableValue, V>> entries = new HashMap<>();

                        entries.put(key, new Table.ValueWithVersion<>(next, nextVersion));
                        entries.put(ETAG, new Table.ValueWithVersion<>(ETAG, table.fromEtag(etag)));

                        return table.updateEntries(entries);
                    });

    }

    CompletableFuture<Void> removeWriter(String appId) {
        Table.Writer key = new Table.Writer(appId);
        return table.deleteRecord(key);
    }

    CompletableFuture<Void> removeReader(String appId) {
        Table.Reader key = new Table.Reader(appId);
        return table.deleteRecord(key);
    }
}