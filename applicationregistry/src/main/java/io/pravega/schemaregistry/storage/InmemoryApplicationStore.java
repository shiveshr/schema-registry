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
import lombok.Synchronized;

import javax.annotation.concurrent.GuardedBy;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Schema Store interface for storing and retrieving and querying schemas. 
 */
public class InmemoryApplicationStore implements ApplicationStore {
    @GuardedBy("$lock")
    private final Map<String, ApplicationRecord.ApplicationValue> applications = new HashMap<>();
    @GuardedBy("$lock")
    private final Map<String, Group<Integer>> groups = new HashMap<>();

    @Synchronized
    @Override
    public CompletableFuture<Void> addApplication(String appId, Map<String, String> properties) {
        applications.putIfAbsent(appId, new ApplicationRecord.ApplicationValue(Collections.emptyMap(), Collections.emptyMap(), properties));
        return CompletableFuture.completedFuture(null);
    }

    @Synchronized
    @Override
    public CompletableFuture<Application> getApplication(String appId) {
        ApplicationRecord.ApplicationValue app = applications.get(appId);
        Map<String, List<Map.Entry<String, String>>> writerIdByGroup = app.getWritingTo().entrySet().stream().collect(Collectors.groupingBy(Map.Entry::getValue));
        Map<String, List<Application.Writer>> writers =
                writerIdByGroup.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, x -> {
                    Group<Integer> group = groups.get(x.getKey());
                    List<Map.Entry<String, String>> entry = writerIdByGroup.get(x.getKey());
                    List<String> writerIds = entry.stream().map(Map.Entry::getKey).collect(Collectors.toList());

                    return group.getWritersForApp(appId, writerIds).join();
                }));
        Map<String, List<Map.Entry<String, String>>> readerIdByGroup = app.getReadingFrom().entrySet().stream().collect(Collectors.groupingBy(Map.Entry::getValue));
        Map<String, List<Application.Reader>> readers =
                writerIdByGroup.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, x -> {
                    Group<Integer> group = groups.get(x.getKey());
                    List<Map.Entry<String, String>> entry = writerIdByGroup.get(x.getKey());
                    List<String> readerIds = entry.stream().map(Map.Entry::getKey).collect(Collectors.toList());

                    return group.getReadersForApp(appId, readerIds).join();
                }));
        Application application = new Application(appId, writers, readers, app.getProperties());
        return CompletableFuture.completedFuture(application);
    }

    @Synchronized
    @Override
    public CompletableFuture<ReadersInGroupWithEtag> getReaderApps(String groupId) {
        Group<Integer> group = groups.get(groupId);
        if (group == null) {
            group = new Group<>(new InMemoryTable());
            groups.put(groupId, group);
        }
        return group.getReaderApps();
    }

    @Synchronized
    @Override
    public CompletableFuture<WritersInGroupWithEtag> getWriterApps(String groupId) {
        Group<Integer> group = groups.get(groupId);
        if (group == null) {
            group = new Group<>(new InMemoryTable());
            groups.put(groupId, group);
        }
        return group.getWriterApps();
    }

    @Synchronized
    @Override
    public CompletableFuture<Void> addWriter(String appId, String groupId, Application.Writer writer, Etag etag) {
        ApplicationRecord.ApplicationValue app = applications.get(appId);
        
        Map<String, String> writingTo = new HashMap<>(app.getWritingTo());
        writingTo.put(writer.getWriterId(), groupId);
        applications.put(appId, new ApplicationRecord.ApplicationValue(writingTo, app.getReadingFrom(), app.getProperties()));
        Group<Integer> group = groups.get(groupId);

        return group.addWriter(appId, writer.getWriterId(), writer.getVersionInfos(), writer.getCodecType(), etag);
    }

    @Synchronized
    @Override
    public CompletableFuture<Void> addReader(String appId, String groupId, Application.Reader reader, Etag etag) {
        ApplicationRecord.ApplicationValue app = applications.get(appId);
        
        Map<String, String> readingFrom = new HashMap<>(app.getReadingFrom());
        readingFrom.put(reader.getReaderId(), groupId);
        applications.put(appId, new ApplicationRecord.ApplicationValue(app.getWritingTo(), readingFrom, app.getProperties()));
        Group<Integer> group = groups.get(groupId);

        return group.addReader(appId, reader.getReaderId(), reader.getVersionInfos(), reader.getCodecs(), etag);
    }
    
    @Synchronized
    @Override
    public CompletableFuture<Void> removeWriter(String appId, String writerId) {
        ApplicationRecord.ApplicationValue app = applications.get(appId);
        String groupId = app.getWritingTo().get(writerId);
        if (groupId != null) {
            Group<Integer> group = groups.get(groupId);
            group.removeWriter(appId, writerId).join();
            app.getWritingTo().remove(writerId);
        } 
        return CompletableFuture.completedFuture(null);
    }

    @Synchronized
    @Override
    public CompletableFuture<Void> removeReader(String appId, String readerId) {
        ApplicationRecord.ApplicationValue app = applications.get(appId);
        String groupId = app.getReadingFrom().get(readerId);
        if (groupId != null) {
            Group<Integer> group = groups.get(groupId);
            group.removeReader(appId, readerId).join();
            app.getReadingFrom().remove(readerId);
        }
        return CompletableFuture.completedFuture(null);
    }
}
