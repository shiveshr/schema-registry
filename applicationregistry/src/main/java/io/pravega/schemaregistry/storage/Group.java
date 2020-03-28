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
import io.pravega.schemaregistry.contract.data.Application;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.VersionInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

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

    CompletableFuture<ReadersInGroupWithEtag> getReaderApps() {
        Map<String, List<Application.Reader>> readers = new HashMap<>();
        return getGroupEtag()
                .thenCompose(etag -> table.getAllRecords()
                                          .thenApply(records -> {
                                              records.forEach((x, y) -> {
                                                  if (x instanceof Table.Reader) {
                                                      String appId = ((Table.Reader) x).getAppId();
                                                      Table.ReaderValue value = (Table.ReaderValue) y;
                                                      readers.put(appId, Lists.newArrayList(value.getVersions().values()));
                                                  }
                                              });
                                              return new ReadersInGroupWithEtag(readers, etag);
                                          }));
    }
    
    CompletableFuture<WritersInGroupWithEtag> getWriterApps() {
        Map<String, List<VersionInfo>> writers = new HashMap<>();
        return getGroupEtag()
                .thenCompose(etag -> table.getAllRecords()
                                          .thenApply(records -> {
                                              records.forEach((x, y) -> {
                                                  if (x instanceof Table.Writer) {
                                                      String appId = ((Table.Writer) x).getAppId();
                                                      Table.ReaderValue value = (Table.ReaderValue) y;
                                                      writers.put(appId, Lists.newArrayList(value.getVersions().values()));
                                                  }
                                              });
                                              return new AppsInGroupWithEtag(writers, etag);
                                          }));

    }

    CompletableFuture<List<Application.Reader>> getReaderSchemasForApp(String applicationId) {
        return table.getRecord(new Table.Reader(applicationId), Table.ReaderValue.class)
                    .thenApply(record -> {
                        if (record == null) {
                            return Collections.emptyList();
                        } else {
                            Map<VersionInfo, Application.Reader> map = new HashMap<>();
                            record.getVersions().forEach((encoding, version) -> map.compute(version, (a, b) -> {
                                if (b == null) {
                                    LinkedList<CodecType> codecTypes = new LinkedList<>();
                                    codecTypes.add(encoding.getCodecType());
                                    return new Application.Reader(a, codecTypes);
                                } else {
                                    LinkedList<CodecType> codecTypes = new LinkedList<>(b.getCodecs());
                                    codecTypes.add(encoding.getCodecType());
                                    return new Application.Reader(a, codecTypes);
                                }
                            }));
                            return Lists.newArrayList(map.values());
                        }
                    });
    }

    CompletableFuture<List<Application.Writer>> getWriterSchemasForApp(String applicationId) {
        return table.getRecord(new Table.Writer(applicationId), Table.WriterValue.class)
                    .thenApply(record -> {
                        if (record == null) {
                            return Collections.emptyList();
                        } else {
                            Map<VersionInfo, Application.Writer> map = new HashMap<>();
                            record.getVersions().forEach((encoding, version) -> map.compute(version, (a, b) -> {
                                if (b == null) {
                                    LinkedList<CodecType> codecTypes = new LinkedList<>();
                                    codecTypes.add(encoding.getCodecType());
                                    return new Application.Reader(a, codecTypes);
                                } else {
                                    LinkedList<CodecType> codecTypes = b.getCodecType());
                                    codecTypes.add(encoding.getCodecType());
                                    return new Application.Reader(a, codecTypes);
                                }
                            }));
                            return Lists.newArrayList(map.values());
                        }
                    });
    }
    
    CompletableFuture<Void> addWriter(String appId, VersionInfo schemaVersion, CodecType codecType, Etag etag) {
        Table.Writer key = new Table.Writer(appId);
        return table.getRecordWithVersion(key, Table.ReaderValue.class)
                .thenCompose(recordWithVersion -> {
                    Table.ReaderValue next;
                    V nextVersion = recordWithVersion == null ? null : recordWithVersion.getVersion();
                    Table.Encoding typeCodecPair = new Table.Encoding(schemaVersion.getSchemaName(), codecType);
                    if (recordWithVersion == null) {
                        // add a new record
                        next = new Table.ReaderValue(Collections.singletonMap(
                                typeCodecPair, schemaVersion));    
                    } else {
                        Table.ReaderValue record = recordWithVersion.getValue();
                        VersionInfo existing = record.getVersions().get(typeCodecPair);
                        if (existing != null && existing.equals(schemaVersion)) {
                            return CompletableFuture.completedFuture(null);
                        } else {
                            Map<Table.Encoding, VersionInfo> map = new HashMap<>(record.getVersions());
                            map.put(typeCodecPair, schemaVersion);
                            next = new Table.ReaderValue(map);
                        }
                    }
                    Map<Table.TableKey, Table.ValueWithVersion<Table.TableValue, V>> entries = new HashMap<>();

                    entries.put(key, new Table.ValueWithVersion<>(next, nextVersion));
                    entries.put(ETAG, new Table.ValueWithVersion<>(ETAG, table.fromEtag(etag)));

                    return table.updateEntries(entries);
                });
    }

    CompletableFuture<Void> addReader(String appId, VersionInfo schemaVersion, List<CodecType> codecs, Etag etag) {
        Table.Reader key = new Table.Reader(appId);
        return table.getRecordWithVersion(key, Table.ReaderValue.class)
                    .thenCompose(recordWithVersion -> {
                        Table.ReaderValue next;
                        V nextVersion = recordWithVersion == null ? null : recordWithVersion.getVersion();
                        List<Table.Encoding> encodings = codecs.stream().map(x -> new Table.Encoding(schemaVersion, x)).collect(Collectors.toList());
                        if (recordWithVersion == null) {
                            // add a new record
                            next = new Table.ReaderValue(encodings);
                        } else {
                            Table.ReaderValue record = recordWithVersion.getValue();
                            // check if reader exists. 
//                            VersionInfo existing = record.getVersions().get(typeCodecPair);
                            if (existing != null && existing.equals(schemaVersion)) {
                                return CompletableFuture.completedFuture(null);
                            } else {
                                Map<Table.Encoding, VersionInfo> map = new HashMap<>(record.getVersions());
                                map.put(typeCodecPair, schemaVersion);
                                next = new Table.ReaderValue(map);
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