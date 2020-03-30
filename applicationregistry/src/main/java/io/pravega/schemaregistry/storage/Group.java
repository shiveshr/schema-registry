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

import io.pravega.schemaregistry.contract.data.Application;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.VersionInfo;

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
                                                  if (x instanceof Table.ReaderKey) {
                                                      String appId = ((Table.ReaderKey) x).getAppId();
                                                      String readerId = ((Table.ReaderKey) x).getReaderId();
                                                      Table.ReaderValue value = (Table.ReaderValue) y;
                                                      Application.Reader reader = new Application.Reader(readerId, 
                                                              value.getVersions(), value.getDecoders());
                                                      readers.compute(appId, (a, b) -> {
                                                          List<Application.Reader> list;
                                                          if (b == null) {
                                                              list = new LinkedList<>();
                                                          } else {
                                                              list = b;
                                                          }
                                                          list.add(reader);
                                                          return list;
                                                      });
                                                  }
                                              });
                                              return new ReadersInGroupWithEtag(readers, etag);
                                          }));
    }
    
    CompletableFuture<WritersInGroupWithEtag> getWriterApps() {
        Map<String, List<Application.Writer>> writers = new HashMap<>();
        return getGroupEtag()
                .thenCompose(etag -> table.getAllRecords()
                                          .thenApply(records -> {
                                              records.forEach((x, y) -> {
                                                  if (x instanceof Table.WriterKey) {
                                                      String appId = ((Table.WriterKey) x).getAppId();
                                                      String writerId = ((Table.WriterKey) x).getWriterId();
                                                      Table.WriterValue value = (Table.WriterValue) y;
                                                      Application.Writer writer = new Application.Writer(writerId,
                                                              value.getVersions(), value.getCodec());
                                                      writers.compute(appId, (a, b) -> {
                                                          List<Application.Writer> list;
                                                          if (b == null) {
                                                              list = new LinkedList<>();
                                                          } else {
                                                              list = b;
                                                          }
                                                          list.add(writer);
                                                          return list;
                                                      });
                                                  }
                                              });
                                              return new WritersInGroupWithEtag(writers, etag);
                                          }));

    }

    CompletableFuture<List<Application.Reader>> getReadersForApp(String applicationId, List<String> readerIds) {
        List<Table.ReaderKey> readerKeys = readerIds.stream().map(x -> new Table.ReaderKey(applicationId, x)).collect(Collectors.toList());
        return table.getRecords(readerKeys, Table.ReaderValue.class)
                    .thenApply(records -> {
                        // remove all null values
                        List<Application.Reader> readers = new LinkedList<>();
                        for (int i = 0; i < readerIds.size(); i++) {
                            Table.ReaderValue value = records.get(i).getValue();
                            if (value != null) {
                                String readerId = readerIds.get(i);
                                readers.add(new Application.Reader(readerId, value.getVersions(), value.getDecoders()));
                            }
                        }
                        return readers;
                    });
    }

    CompletableFuture<List<Application.Writer>> getWritersForApp(String applicationId, List<String> writerIds) {
        List<Table.WriterKey> writerKeys = writerIds.stream().map(x -> new Table.WriterKey(applicationId, x)).collect(Collectors.toList());
        return table.getRecords(writerKeys, Table.WriterValue.class)
                    .thenApply(records -> {
                        // remove all null values
                        List<Application.Writer> writers = new LinkedList<>();
                        for (int i = 0; i < writerIds.size(); i++) {
                            Table.WriterValue value = records.get(i).getValue();
                            if (value != null) {
                                String writerId = writerIds.get(i);
                                writers.add(new Application.Writer(writerId, value.getVersions(), value.getCodec()));
                            }
                        }
                        return writers;
                    });
    }
    
    CompletableFuture<Void> addWriter(String appId, String writerId, List<VersionInfo> versions, CodecType codecType, Etag etag) {
        Table.WriterKey key = new Table.WriterKey(appId, writerId);
        return table.getRecordWithVersion(key, Table.WriterValue.class)
                    .thenCompose(recordWithVersion -> {
                        Table.WriterValue next;
                        V nextVersion = recordWithVersion == null ? null : recordWithVersion.getVersion();
                        next = new Table.WriterValue(versions, codecType);
                        Map<Table.TableKey, Table.ValueWithVersion<Table.TableValue, V>> entries = new HashMap<>();

                        entries.put(key, new Table.ValueWithVersion<>(next, nextVersion));
                        entries.put(ETAG, new Table.ValueWithVersion<>(ETAG, table.fromEtag(etag)));

                        return table.updateEntries(entries);
                    });
    }

    CompletableFuture<Void> addReader(String appId, String readerId, List<VersionInfo> versions, List<CodecType> decoders, Etag etag) {
        Table.ReaderKey key = new Table.ReaderKey(appId, readerId);
        return table.getRecordWithVersion(key, Table.ReaderValue.class)
                    .thenCompose(recordWithVersion -> {
                        Table.ReaderValue next;
                        V nextVersion = recordWithVersion == null ? null : recordWithVersion.getVersion();
                        next = new Table.ReaderValue(versions, decoders);
                        Map<Table.TableKey, Table.ValueWithVersion<Table.TableValue, V>> entries = new HashMap<>();

                        entries.put(key, new Table.ValueWithVersion<>(next, nextVersion));
                        entries.put(ETAG, new Table.ValueWithVersion<>(ETAG, table.fromEtag(etag)));

                        return table.updateEntries(entries);
                    });
    }

    CompletableFuture<Void> removeWriter(String appId, String writerId) {
        Table.WriterKey key = new Table.WriterKey(appId, writerId);
        return table.deleteRecord(key);
    }

    CompletableFuture<Void> removeReader(String appId, String readerId) {
        Table.ReaderKey key = new Table.ReaderKey(appId, readerId);
        return table.deleteRecord(key);
    }
}