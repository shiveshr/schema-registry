/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.service;

import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import io.pravega.schemaregistry.contract.data.Application;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaEvolution;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.contract.exceptions.IncompatibleSchemaException;
import io.pravega.schemaregistry.storage.ApplicationStore;
import io.pravega.schemaregistry.storage.AppsInGroupWithEtag;
import io.pravega.schemaregistry.storage.Etag;
import io.pravega.schemaregistry.storage.StoreExceptions;

import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Schema registry service backend. 
 */
public class ApplicationRegistryService {
    private static final Retry.RetryAndThrowConditionally RETRY = Retry.withExpBackoff(1, 2, Integer.MAX_VALUE, 100)
                                                                       .retryWhen(x -> Exceptions.unwrap(x) instanceof StoreExceptions.WriteConflictException);

    private final ApplicationStore store;

    public ApplicationRegistryService(ApplicationStore store) {
        this.store = store;
    }
    
    public CompletableFuture<Void> addApplication(String appId, Map<String, String> properties) {
        // add application to the store then app each writer and reader
        return store.addApplication(appId, properties);
    }

    public CompletableFuture<Application> getApplication(String appId, BiFunction<String, VersionInfo, CompletableFuture<SchemaInfo>> getSchemaFromVersion) {
        // add application to the store then app each writer and reader
        return store.getApplication(appId)
                .thenCompose(app -> {
                    CompletableFuture<Map<String, List<SchemaInfo>>> readersFuture = Futures.allOfWithResults(
                            app.getReaders().entrySet().stream()
                               .collect(Collectors.toMap(Map.Entry::getKey, x -> {
                                   List<VersionInfo> list = x.getValue();
                                   return Futures.allOfWithResults(list.stream().map(y -> getSchemaFromVersion.apply(x.getKey(), y))
                                                                       .collect(Collectors.toList()));
                               })));
                    CompletableFuture<Map<String, List<SchemaInfo>>> writersFuture = Futures.allOfWithResults(
                            app.getWriters().entrySet().stream()
                               .collect(Collectors.toMap(Map.Entry::getKey, x -> {
                                   List<VersionInfo> list = x.getValue();
                                   return Futures.allOfWithResults(list.stream().map(y -> getSchemaFromVersion.apply(x.getKey(), y))
                                                                       .collect(Collectors.toList()));
                               })));
                    return CompletableFuture.allOf(readersFuture, writersFuture)
                            .thenApply(v -> {
                                Map<String, List<SchemaInfo>> readers = readersFuture.join();
                                Map<String, List<SchemaInfo>> writers = writersFuture.join();
                                return new Application(appId, writers, readers, app.getProperties());
                            });
                });
    }

    public CompletableFuture<Void> addWriter(String appId, String groupId, VersionInfo schemaVersion,
                                             Function<String, CompletableFuture<GroupProperties>> groupProperties,
                                             Function<String, CompletableFuture<List<SchemaEvolution>>> groupHistory) {
        CompletableFuture<GroupProperties> grpProp = groupProperties.apply(groupId);
        CompletableFuture<AppsInGroupWithEtag> readers = store.getReaderApps(groupId);
        CompletableFuture<List<SchemaEvolution>> grpHistory = groupHistory.apply(groupId);
        StringBuilder cause = new StringBuilder();

        return CompletableFuture.allOf(grpProp, readers, grpHistory)
                                .thenCompose(v -> {
                                    GroupProperties prop = grpProp.join();
                                    List<SchemaEvolution> history = grpHistory.join();
                                    
                                    List<VersionInfo> readerApps = readers
                                            .join().getAppIdWithSchemaVersions().entrySet().stream()
                                            .flatMap(x -> x.getValue().stream()).collect(Collectors.toList());
                                    if (prop.isValidateByObjectType()) {
                                        readerApps = readerApps.stream().filter(x -> x.getSchemaName().equals(schemaVersion.getSchemaName()))
                                                  .collect(Collectors.toList());
                                        history = history.stream().filter(x -> x.getSchema().getName().equals(schemaVersion.getSchemaName()))
                                                .collect(Collectors.toList());
                                    }
                                    Etag etag = readers.join().getEtag();

                                    IntSummaryStatistics stats = readerApps.stream().mapToInt(VersionInfo::getVersion).summaryStatistics();
                                    int maxReaderVersion = stats.getMax();
                                    int minReaderVersion = stats.getMin();

                                    IntSummaryStatistics historyStats = history.stream().mapToInt(x -> x.getVersion().getVersion()).summaryStatistics();
                                    int latestSchemaVersion = historyStats.getMax();
                                    
                                    Compatibility compatibility = (Compatibility) prop
                                            .getSchemaValidationRules().getRules().get(Compatibility.class.getSimpleName());
                                    boolean isValid;
                                    int atLeast;
                                    int atMost;
                                    int forwardTill = getTillVersion(compatibility.getForwardTill(), history, prop, schemaVersion);
                                    int backwardTill = getTillVersion(compatibility.getBackwardTill(), history, prop, schemaVersion);
                                    switch (compatibility.getCompatibility()) {
                                        case ForwardTransitive:
                                            // writer should be greater than equal to highest reader.
                                            atLeast = Integer.max(0, maxReaderVersion);
                                            isValid = schemaVersion.getVersion() >= atLeast;
                                            if (!(schemaVersion.getVersion() >= atLeast)) {
                                                cause.append(" Schema version should be at least ").append(atLeast).append(".");
                                            }
                                            break;
                                        case ForwardTill:
                                            atLeast = Integer.max(forwardTill, maxReaderVersion);
                                            // if there is a reader behind forwardTill, fail the check until the reader is updated 
                                            // or removed.
                                            isValid = schemaVersion.getVersion() >= atLeast && 
                                                    minReaderVersion >= forwardTill;
                                            if (!isValid) {
                                                if (!(minReaderVersion >= forwardTill)) {
                                                    cause.append("All readers should be ahead of ").append(forwardTill).append(".");
                                                }
                                                if (!(schemaVersion.getVersion() >= atLeast)) {
                                                    cause.append(" Schema version should be at least ").append(atLeast).append(".");
                                                }
                                            }
                                            break;
                                        case Forward:
                                            atLeast = Integer.max(latestSchemaVersion - 1, maxReaderVersion);
                                            // if there is a reader behind latestSchema - 1, fail the check until the reader is 
                                            // updated or removed. 
                                            isValid = schemaVersion.getVersion() >= atLeast &&
                                                    Integer.min(minReaderVersion, latestSchemaVersion - 1) >= atLeast;
                                            if (!isValid) {
                                                if (!(Integer.min(minReaderVersion, latestSchemaVersion - 1) >= atLeast)) {
                                                    cause.append("All readers should be ahead of ").append(atLeast).append(".");
                                                }
                                                if (!(schemaVersion.getVersion() >= atLeast)) {
                                                    cause.append(" Schema version should be at least ").append(atLeast).append(".");
                                                }
                                            }
                                            break;
                                        case Backward:
                                            // one level behind or at par with lowest reader version. 
                                            atMost = Integer.min(minReaderVersion, latestSchemaVersion);
                                            atLeast = latestSchemaVersion - 1;
                                            isValid = schemaVersion.getVersion() >= atLeast && schemaVersion.getVersion() <= atMost;
                                            if (!isValid) {
                                                if (!(schemaVersion.getVersion() >= atLeast)) {
                                                    cause.append("Schema version should be at least ").append(atLeast).append(".");
                                                }
                                                if (!(schemaVersion.getVersion() <= atMost)) {
                                                    cause.append(" Schema version should be at most ").append(atMost).append(".");
                                                }
                                            }
                                            break;
                                        case BackwardTill:
                                            // all readers should be ahead or equal to this writer version
                                            // the writer should be ahead of backwardTill
                                            atMost = Integer.min(minReaderVersion, latestSchemaVersion);
                                            atLeast = backwardTill;
                                            isValid = schemaVersion.getVersion() >= atLeast && schemaVersion.getVersion() <= atMost;
                                            if (!isValid) {
                                                if (!(schemaVersion.getVersion() >= atLeast)) {
                                                    cause.append("Schema version should be at least ").append(atLeast).append(".");
                                                }
                                                if (!(schemaVersion.getVersion() <= atMost)) {
                                                    cause.append(" Schema version should be at most ").append(atMost).append(".");
                                                }
                                            }
                                            break;
                                        case BackwardTransitive:
                                            // all readers should be ahead of this writer version.
                                            atMost = Integer.min(minReaderVersion, latestSchemaVersion);
                                            isValid = schemaVersion.getVersion() <= atMost;
                                            if (!(schemaVersion.getVersion() <= atMost)) {
                                                cause.append(" Schema version should be at most ").append(atMost).append(".");
                                            }

                                            break;
                                        case BackwardAndForwardTill:
                                            // there shouldnt be any reader older than compatibility.getForwardTill().getVersion()
                                            // if there are, dont allow this writer until the reader is upgraded or removed. 
                                            atLeast = backwardTill; 
                                            isValid = schemaVersion.getVersion() >= atLeast && 
                                                    minReaderVersion >= forwardTill;
                                            if (!isValid) {
                                                if (!(schemaVersion.getVersion() >= atLeast)) {
                                                    cause.append("Schema version should be at least ").append(atLeast).append(".");
                                                }
                                                if (!(minReaderVersion >= forwardTill)) {
                                                    cause.append(" All readers should be ahead of ").append(forwardTill).append(".");
                                                }
                                            }
                                            break;
                                        case Full:
                                            // if there is a reader older than latestSchemaVersion - 1 then disallow this writer. 
                                            atLeast = latestSchemaVersion - 1;
                                            isValid = schemaVersion.getVersion() >= atLeast && minReaderVersion >= atLeast;
                                            if (!isValid) {
                                                if (!(schemaVersion.getVersion() >= atLeast)) {
                                                    cause.append("Schema version should be at least ").append(atLeast).append(".");
                                                }
                                                if (!(minReaderVersion >= atLeast)) {
                                                    cause.append(" All readers should be ahead of ").append(atLeast).append(".");
                                                }
                                            }
                                            break;
                                        case FullTransitive:
                                            isValid = true;
                                            break;
                                        case AllowAny:
                                            isValid = true;
                                            break;
                                        case DenyAll:
                                            isValid = schemaVersion.getVersion() == latestSchemaVersion;
                                            if (!isValid) {
                                                cause.append(" Schema version should be ").append(latestSchemaVersion).append(".");
                                            }
                                            break;
                                        default:
                                            isValid = false;
                                            cause.append(" Unknown policy.");
                                    }
                                    
                                    if (isValid) {
                                        return store.addWriter(appId, groupId, schemaVersion, etag);
                                    } else {
                                        throw new IncompatibleSchemaException(cause.toString());
                                    }
                });
    }

    private int getTillVersion(VersionInfo till, List<SchemaEvolution> history, GroupProperties prop, VersionInfo schemaToCheck) {
        if (till == null) {
            return Integer.MIN_VALUE;
        }
        if (prop.isValidateByObjectType()) {
            boolean found = false;
            int schemaVersion = Integer.MIN_VALUE;
            for (SchemaEvolution schema : history) {
                if (!found) {
                    found = schema.getVersion().equals(till);
                }
                if (found && schema.getVersion().getSchemaName().equals(schemaToCheck.getSchemaName())) {
                    schemaVersion = schema.getVersion().getVersion();
                    break;
                }
            }
            return schemaVersion;
        } else {
            return till.getVersion();
        }
    }

    public CompletableFuture<Void> addReader(String appId, String groupId, VersionInfo schemaVersion,
                                             Function<String, CompletableFuture<GroupProperties>> groupProperties,
                                             Function<String, CompletableFuture<List<SchemaEvolution>>> groupHistory) {
        CompletableFuture<GroupProperties> grpProp = groupProperties.apply(groupId);
        CompletableFuture<AppsInGroupWithEtag> writers = store.getWriterApps(groupId);
        CompletableFuture<List<SchemaEvolution>> grpHistory = groupHistory.apply(groupId);
        StringBuilder cause = new StringBuilder();
        return CompletableFuture.allOf(grpProp, writers, grpHistory)
                                .thenCompose(v -> {
                                    GroupProperties prop = grpProp.join();
                                    List<SchemaEvolution> history = grpHistory.join();
                                    List<VersionInfo> writerApps = writers.join().getAppIdWithSchemaVersions()
                                            .entrySet().stream().flatMap(x -> x.getValue().stream()).collect(Collectors.toList());
                                    
                                    if (prop.isValidateByObjectType()) {
                                        writerApps = writerApps.stream().filter(x -> x.getSchemaName().equals(schemaVersion.getSchemaName()))
                                                               .collect(Collectors.toList());
                                        history = history.stream().filter(x -> x.getSchema().getName().equals(schemaVersion.getSchemaName()))
                                                         .collect(Collectors.toList());
                                    }
                                    
                                    Etag etag = writers.join().getEtag();

                                    IntSummaryStatistics stats = writerApps.stream().mapToInt(VersionInfo::getVersion).summaryStatistics();
                                    int maxWriterVersion = stats.getMax();
                                    int minWriterVersion = stats.getMin();

                                    IntSummaryStatistics historyStats = history.stream().mapToInt(x -> x.getVersion().getVersion()).summaryStatistics();
                                    int latestSchemaVersion = historyStats.getMax();

                                    Compatibility compatibility = (Compatibility) prop
                                            .getSchemaValidationRules().getRules().get(Compatibility.class.getSimpleName());
                                    boolean isValid;
                                    int atLeast;
                                    int atMost;
                                    int forwardTill = getTillVersion(compatibility.getForwardTill(), history, prop, schemaVersion);
                                    int backwardTill = getTillVersion(compatibility.getBackwardTill(), history, prop, schemaVersion);

                                    switch (compatibility.getCompatibility()) {
                                        case ForwardTransitive:
                                            // reader should be less than equal to lowest writer.
                                            atMost = Integer.min(latestSchemaVersion, minWriterVersion);
                                            isValid = schemaVersion.getVersion() <= atMost;
                                            if (!isValid) {
                                                cause.append("Schema version should be at most ").append(atMost).append(".");
                                            }
                                            break;
                                        case ForwardTill:
                                            // less than max writer version while ahead of forward till
                                            atMost = Integer.min(Integer.max(0, maxWriterVersion), latestSchemaVersion);
                                            atLeast = forwardTill;
                                            isValid = schemaVersion.getVersion() <= atMost && schemaVersion.getVersion() >= atLeast;
                                            if (!isValid) {
                                                if (!(schemaVersion.getVersion() <= atMost)) {
                                                    cause.append("Schema version should be at most ").append(atMost).append(".");
                                                }
                                                if (!(schemaVersion.getVersion() >= atLeast)) {
                                                    cause.append(" Schema version should be at least ").append(atLeast).append(".");
                                                }
                                            }
                                            break;
                                        case Forward:
                                            // less than min writer while ahead of latest - 1
                                            atMost = Integer.min(latestSchemaVersion, minWriterVersion);
                                            atLeast = latestSchemaVersion - 1;
                                            isValid = schemaVersion.getVersion() <= atMost && schemaVersion.getVersion() >= atLeast;
                                            if (!isValid) {
                                                if (!(schemaVersion.getVersion() <= atMost)) {
                                                    cause.append("Schema version should be at most ").append(atMost).append(".");
                                                }
                                                if (!(schemaVersion.getVersion() >= atLeast)) {
                                                    cause.append(" Schema version should be at least ").append(atLeast).append(".");
                                                }
                                            }
                                            break;
                                        case Backward:
                                            // ahead of max writer with no writer behind latestSchema - 1
                                            atLeast = Integer.max(latestSchemaVersion - 1, maxWriterVersion);
                                            isValid = schemaVersion.getVersion() >= atLeast 
                                                    && minWriterVersion >= latestSchemaVersion - 1;
                                            if (!isValid) {
                                                if (!(schemaVersion.getVersion() >= atLeast)) {
                                                    cause.append(" Schema version should be at least ").append(atLeast).append(".");
                                                }
                                                if (!(minWriterVersion >= latestSchemaVersion - 1)) {
                                                    cause.append(" No writer should be behind version ").append(latestSchemaVersion - 1).append(".");
                                                }
                                            }
                                            break;
                                        case BackwardTill:
                                            // reader schema should be ahead of max writer
                                            // all writers should be ahead of backward till.
                                            atLeast = Integer.max(maxWriterVersion, backwardTill);
                                            isValid = schemaVersion.getVersion() >= atLeast &&
                                                    minWriterVersion >= backwardTill;
                                            if (!isValid) {
                                                if (!(schemaVersion.getVersion() >= atLeast)) {
                                                    cause.append(" Schema version should be at least ").append(atLeast).append(".");
                                                }
                                                if (!(minWriterVersion >= backwardTill)) {
                                                    cause.append(" No writer should be behind version ").append(backwardTill).append(".");
                                                }
                                            }
                                            break;
                                        case BackwardTransitive:
                                            // reader should be ahead of max writer
                                            atLeast = Integer.max(maxWriterVersion, 0);
                                            isValid = schemaVersion.getVersion() >= atLeast;
                                            if (!isValid) {
                                                if (!(schemaVersion.getVersion() >= atLeast)) {
                                                    cause.append(" Schema version should be at least ").append(atLeast).append(".");
                                                }
                                            }
                                            break;
                                        case BackwardAndForwardTill:
                                            // reader should be ahead of forwardTill
                                            // there shouldnt be any writer older than compatibility.getBackwardTill().getVersion()
                                            // if there are, dont allow this writer until the reader is also upgraded or removed. 
                                            atLeast = forwardTill;
                                            isValid = schemaVersion.getVersion() >= atLeast &&
                                                    minWriterVersion >= backwardTill;
                                            if (!isValid) {
                                                if (!(schemaVersion.getVersion() >= atLeast)) {
                                                    cause.append(" Schema version should be at least ").append(atLeast).append(".");
                                                }
                                                if (!(minWriterVersion >= backwardTill)) {
                                                    cause.append(" No writer should be behind version ").append(backwardTill).append(".");
                                                }
                                            }
                                            break;
                                        case Full:
                                            // if there is a writer older than latestSchemaVersion - 1 then disallow this reader. 
                                            atLeast = latestSchemaVersion - 1;
                                            isValid = schemaVersion.getVersion() >= atLeast && minWriterVersion >= atLeast;
                                            if (!isValid) {
                                                if (!(schemaVersion.getVersion() >= atLeast)) {
                                                    cause.append(" Schema version should be at least ").append(atLeast).append(".");
                                                }
                                                if (!(minWriterVersion >= atLeast)) {
                                                    cause.append(" No writer should be behind version ").append(atLeast).append(".");
                                                }
                                            }
                                            break;
                                        case FullTransitive:
                                            isValid = true;
                                            break;
                                        case AllowAny:
                                            isValid = true;
                                            break;
                                        case DenyAll:
                                            isValid = schemaVersion.getVersion() == latestSchemaVersion;
                                            if (!isValid) {
                                                if (schemaVersion.getVersion() != latestSchemaVersion) {
                                                    cause.append(" Schema version should be ").append(latestSchemaVersion).append(".");
                                                }
                                            }
                                            break;
                                        default:
                                            isValid = false;
                                            cause.append("Unknown policy.");

                                    }

                                    if (isValid) {
                                        return store.addReader(appId, groupId, schemaVersion, etag);
                                    } else {
                                        throw new IncompatibleSchemaException(cause.toString());
                                    }
                                });
    }

    public CompletableFuture<Void> removeWriter(String appId, String groupId) {
        return store.removeWriter(appId, groupId);
    }

    public CompletableFuture<Void> removeReader(String appId, String groupId) {
        return store.removeReader(appId, groupId);
    }

    public CompletableFuture<Map<String, List<VersionInfo>>> listWriterAppsInGroup(String groupId) {
        return store.getWriterApps(groupId).thenApply(AppsInGroupWithEtag::getAppIdWithSchemaVersions);
    }

    public CompletableFuture<Map<String, List<VersionInfo>>> listReaderAppsInGroup(String groupId) {
        return store.getReaderApps(groupId).thenApply(AppsInGroupWithEtag::getAppIdWithSchemaVersions);
    }
}
