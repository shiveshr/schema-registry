/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.service;

import io.pravega.common.Exceptions;
import io.pravega.common.util.Retry;
import io.pravega.schemaregistry.contract.data.Application;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaEvolution;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.contract.exceptions.CodecMismatchException;
import io.pravega.schemaregistry.contract.exceptions.IncompatibleSchemaException;
import io.pravega.schemaregistry.storage.ApplicationStore;
import io.pravega.schemaregistry.storage.Etag;
import io.pravega.schemaregistry.storage.ReadersInGroupWithEtag;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.WritersInGroupWithEtag;

import java.util.Collection;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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

    public CompletableFuture<Application> getApplication(String appId) {
        // add application to the store then app each writer and reader
        return store.getApplication(appId);
    }

    public CompletableFuture<Void> addWriter(String appId, String groupId, Application.Writer writer,
                                             Function<String, CompletableFuture<GroupProperties>> groupProperties,
                                             Function<String, CompletableFuture<List<SchemaEvolution>>> groupHistory) {
        CompletableFuture<GroupProperties> grpProp = groupProperties.apply(groupId);
        CompletableFuture<ReadersInGroupWithEtag> readersWithEtag = store.getReaderApps(groupId);
        CompletableFuture<List<SchemaEvolution>> grpHistory = groupHistory.apply(groupId);

        // for all writer schemas, readers should be able to read them. 
        return CompletableFuture.allOf(grpProp, readersWithEtag, grpHistory)
                                .thenCompose(v -> {
                                    GroupProperties prop = grpProp.join();
                                    List<SchemaEvolution> history = grpHistory.join();
                                    ReadersInGroupWithEtag readers = readersWithEtag.join();
                                    Etag etag = readersWithEtag.join().getEtag();
                                    List<Application.Reader> readerApps = readers.getAppReaders().values().stream().flatMap(Collection::stream)
                                            .collect(Collectors.toList());
                                    writer.getVersionInfos().forEach(version -> checkWriterSchema(prop, readerApps, history,
                                            version));

                                    boolean encodingMatch = readerApps.stream().allMatch(x -> x.getCodecs().contains(writer.getCodecType()));
                                    if (!encodingMatch) {
                                        throw new CodecMismatchException("Not all readers have the supplied codec.");
                                    }

                                    return store.addWriter(appId, groupId, writer, etag);
                                });
    }

    private void checkWriterSchema(GroupProperties prop, List<Application.Reader> readers, List<SchemaEvolution> schemaEvolutions,
                                   VersionInfo version) {
        StringBuilder cause = new StringBuilder();

        List<VersionInfo> readersVersions;
        List<SchemaEvolution> history = schemaEvolutions;
        if (prop.isValidateByObjectType()) {
            // filter all readerAps that are using the object type
            readersVersions = readers.stream().filter(x -> x.getVersionInfos().stream().anyMatch(y -> y.getSchemaName().equals(version.getSchemaName())))
                                     .flatMap(x -> x.getVersionInfos().stream())
                                     .collect(Collectors.toList());
            history = schemaEvolutions.stream().filter(x -> x.getSchema().getName().equals(version.getSchemaName()))
                             .collect(Collectors.toList());
        } else {
            readersVersions = readers.stream().flatMap(x -> x.getVersionInfos().stream()).collect(Collectors.toList());
        }
        boolean isCompatible = isCompatibleWithReaders(version, cause, prop, history, readersVersions);

        if (!isCompatible) {
            throw new IncompatibleSchemaException(cause.toString());
        }
    }

    private boolean isCompatibleWithReaders(VersionInfo schemaVersion, StringBuilder cause, GroupProperties prop,
                                            List<SchemaEvolution> history, List<VersionInfo> readerVersions) {
        boolean isCompatible;
        IntSummaryStatistics stats = readerVersions.stream().mapToInt(VersionInfo::getVersion).summaryStatistics();
        int maxReaderVersion = stats.getMax();
        int minReaderVersion = stats.getMin();

        IntSummaryStatistics historyStats = history.stream().mapToInt(x -> x.getVersion().getVersion()).summaryStatistics();
        int latestSchemaVersion = historyStats.getMax();

        Compatibility compatibility = (Compatibility) prop
                .getSchemaValidationRules().getRules().get(Compatibility.class.getSimpleName());
        int atLeast;
        int atMost;
        int forwardTill = getTillVersion(compatibility.getForwardTill(), history, prop, schemaVersion);
        int backwardTill = getTillVersion(compatibility.getBackwardTill(), history, prop, schemaVersion);
        switch (compatibility.getCompatibility()) {
            case ForwardTransitive:
                // writer should be greater than equal to highest reader.
                atLeast = Integer.max(0, maxReaderVersion);
                isCompatible = schemaVersion.getVersion() >= atLeast;
                if (!(schemaVersion.getVersion() >= atLeast)) {
                    cause.append(" Schema version should be at least ").append(atLeast).append(".");
                }
                break;
            case ForwardTill:
                atLeast = Integer.max(forwardTill, maxReaderVersion);
                // if there is a reader behind forwardTill, fail the check until the reader is updated 
                // or removed.
                isCompatible = schemaVersion.getVersion() >= atLeast &&
                        minReaderVersion >= forwardTill;
                if (!isCompatible) {
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
                isCompatible = schemaVersion.getVersion() >= atLeast &&
                        Integer.min(minReaderVersion, latestSchemaVersion - 1) >= atLeast;
                if (!isCompatible) {
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
                isCompatible = schemaVersion.getVersion() >= atLeast && schemaVersion.getVersion() <= atMost;
                if (!isCompatible) {
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
                isCompatible = schemaVersion.getVersion() >= atLeast && schemaVersion.getVersion() <= atMost;
                if (!isCompatible) {
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
                isCompatible = schemaVersion.getVersion() <= atMost;
                if (!(schemaVersion.getVersion() <= atMost)) {
                    cause.append(" Schema version should be at most ").append(atMost).append(".");
                }

                break;
            case BackwardAndForwardTill:
                // there shouldnt be any reader older than compatibility.getForwardTill().getVersion()
                // if there are, dont allow this writer until the reader is upgraded or removed. 
                atLeast = backwardTill;
                isCompatible = schemaVersion.getVersion() >= atLeast &&
                        minReaderVersion >= forwardTill;
                if (!isCompatible) {
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
                isCompatible = schemaVersion.getVersion() >= atLeast && minReaderVersion >= atLeast;
                if (!isCompatible) {
                    if (!(schemaVersion.getVersion() >= atLeast)) {
                        cause.append("Schema version should be at least ").append(atLeast).append(".");
                    }
                    if (!(minReaderVersion >= atLeast)) {
                        cause.append(" All readers should be ahead of ").append(atLeast).append(".");
                    }
                }
                break;
            case FullTransitive:
                isCompatible = true;
                break;
            case AllowAny:
                isCompatible = true;
                break;
            case DenyAll:
                isCompatible = schemaVersion.getVersion() == latestSchemaVersion;
                if (!isCompatible) {
                    cause.append(" Schema version should be ").append(latestSchemaVersion).append(".");
                }
                break;
            default:
                isCompatible = false;
                cause.append(" Unknown policy.");
        }
        return isCompatible;
    }
    
    public CompletableFuture<Void> addReader(String appId, String groupId, Application.Reader reader,
                                             Function<String, CompletableFuture<GroupProperties>> groupProperties,
                                             Function<String, CompletableFuture<List<SchemaEvolution>>> groupHistory) {
        CompletableFuture<GroupProperties> grpProp = groupProperties.apply(groupId);
        CompletableFuture<WritersInGroupWithEtag> writersWithEtag = store.getWriterApps(groupId);
        CompletableFuture<List<SchemaEvolution>> grpHistory = groupHistory.apply(groupId);
        StringBuilder cause = new StringBuilder();
        return CompletableFuture.allOf(grpProp, writersWithEtag, grpHistory)
                                .thenCompose(v -> {
                                    GroupProperties prop = grpProp.join();
                                    List<SchemaEvolution> history = grpHistory.join();
                                    List<Application.Writer> writerApps = writersWithEtag
                                            .join().getAppWriters()
                                            .entrySet().stream().flatMap(x -> x.getValue().stream()).collect(Collectors.toList());

                                    Etag etag = writersWithEtag.join().getEtag();
                                    reader.getVersionInfos().forEach(version -> checkReaderSchema(prop, writerApps, history,
                                            version));

                                    boolean encodingMatch = writerApps.stream().allMatch(x -> reader.getCodecs().contains(x.getCodecType()));
                                    if (!encodingMatch) {
                                        throw new CodecMismatchException("Not all readers have the supplied codec.");
                                    }

                                    return store.addReader(appId, groupId, reader, etag);
                                });
    }

    private void checkReaderSchema(GroupProperties prop, List<Application.Writer> writers, List<SchemaEvolution> schemaEvolutions,
                                   VersionInfo version) {
        StringBuilder cause = new StringBuilder();

        List<VersionInfo> writerVersions;
        List<SchemaEvolution> history = schemaEvolutions;
        if (prop.isValidateByObjectType()) {
            // filter all readerAps that are using the object type
            writerVersions = writers.stream().filter(x -> x.getVersionInfos().stream().anyMatch(y -> y.getSchemaName().equals(version.getSchemaName())))
                                     .flatMap(x -> x.getVersionInfos().stream())
                                     .collect(Collectors.toList());
            history = schemaEvolutions.stream().filter(x -> x.getSchema().getName().equals(version.getSchemaName()))
                                      .collect(Collectors.toList());
        } else {
            writerVersions = writers.stream().flatMap(x -> x.getVersionInfos().stream()).collect(Collectors.toList());
        }

        boolean isCompatible;

        isCompatible = isCompatibleWithWriters(version, cause, prop, history, writerVersions);

        if (!isCompatible) {
            throw new IncompatibleSchemaException(cause.toString());
        }
    }

    private boolean isCompatibleWithWriters(VersionInfo schemaVersion, StringBuilder cause, GroupProperties prop,
                                            List<SchemaEvolution> history, List<VersionInfo> writerVersions) {
        boolean isCompatible;
        IntSummaryStatistics stats = writerVersions.stream().mapToInt(VersionInfo::getVersion).summaryStatistics();
        int maxWriterVersion = stats.getMax();
        int minWriterVersion = stats.getMin();

        IntSummaryStatistics historyStats = history.stream().mapToInt(x -> x.getVersion().getVersion()).summaryStatistics();
        int latestSchemaVersion = historyStats.getMax();

        Compatibility compatibility = (Compatibility) prop
                .getSchemaValidationRules().getRules().get(Compatibility.class.getSimpleName());
        int atLeast;
        int atMost;
        int forwardTill = getTillVersion(compatibility.getForwardTill(), history, prop, schemaVersion);
        int backwardTill = getTillVersion(compatibility.getBackwardTill(), history, prop, schemaVersion);

        switch (compatibility.getCompatibility()) {
            case ForwardTransitive:
                // reader should be less than equal to lowest writer.
                atMost = Integer.min(latestSchemaVersion, minWriterVersion);
                isCompatible = schemaVersion.getVersion() <= atMost;
                if (!isCompatible) {
                    cause.append("Schema version should be at most ").append(atMost).append(".");
                }
                break;
            case ForwardTill:
                // less than max writer version while ahead of forward till
                atMost = Integer.min(Integer.max(0, maxWriterVersion), latestSchemaVersion);
                atLeast = forwardTill;
                isCompatible = schemaVersion.getVersion() <= atMost && schemaVersion.getVersion() >= atLeast;
                if (!isCompatible) {
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
                isCompatible = schemaVersion.getVersion() <= atMost && schemaVersion.getVersion() >= atLeast;
                if (!isCompatible) {
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
                isCompatible = schemaVersion.getVersion() >= atLeast
                        && minWriterVersion >= latestSchemaVersion - 1;
                if (!isCompatible) {
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
                isCompatible = schemaVersion.getVersion() >= atLeast &&
                        minWriterVersion >= backwardTill;
                if (!isCompatible) {
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
                isCompatible = schemaVersion.getVersion() >= atLeast;
                if (!isCompatible) {
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
                isCompatible = schemaVersion.getVersion() >= atLeast &&
                        minWriterVersion >= backwardTill;
                if (!isCompatible) {
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
                isCompatible = schemaVersion.getVersion() >= atLeast && minWriterVersion >= atLeast;
                if (!isCompatible) {
                    if (!(schemaVersion.getVersion() >= atLeast)) {
                        cause.append(" Schema version should be at least ").append(atLeast).append(".");
                    }
                    if (!(minWriterVersion >= atLeast)) {
                        cause.append(" No writer should be behind version ").append(atLeast).append(".");
                    }
                }
                break;
            case FullTransitive:
                isCompatible = true;
                break;
            case AllowAny:
                isCompatible = true;
                break;
            case DenyAll:
                isCompatible = schemaVersion.getVersion() == latestSchemaVersion;
                if (!isCompatible) {
                    if (schemaVersion.getVersion() != latestSchemaVersion) {
                        cause.append(" Schema version should be ").append(latestSchemaVersion).append(".");
                    }
                }
                break;
            default:
                isCompatible = false;
                cause.append("Unknown policy.");

        }
        return isCompatible;
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

    public CompletableFuture<Void> removeWriter(String appId, String writerId) {
        return store.removeWriter(appId, writerId);
    }

    public CompletableFuture<Void> removeReader(String appId, String readerId) {
        return store.removeReader(appId, readerId);
    }

    public CompletableFuture<Map<String, List<Application.Writer>>> listWriterAppsInGroup(String groupId) {
        return store.getWriterApps(groupId).thenApply(WritersInGroupWithEtag::getAppWriters);
    }

    public CompletableFuture<Map<String, List<Application.Reader>>> listReaderAppsInGroup(String groupId) {
        return store.getReaderApps(groupId).thenApply(ReadersInGroupWithEtag::getAppReaders);
    }
}
