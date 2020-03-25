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
import io.pravega.schemaregistry.service.AppsInGroupList;
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
        applications.putIfAbsent(appId, new ApplicationRecord.ApplicationValue(Collections.emptyList(), Collections.emptyList(), properties));
        return CompletableFuture.completedFuture(null);
    }

    @Synchronized
    @Override
    public CompletableFuture<ApplicationRecord.Application> getApplication(String appId) {
        ApplicationRecord.ApplicationValue app = applications.get(appId);
        Map<String, List<VersionInfo>> writers = app.getWritingTo().stream().collect(Collectors.toMap(x -> x, grp -> {
            Group<Integer> group = groups.get(grp);
            return group.getWriterSchemasForApp(appId).join();
        }));
        Map<String, List<VersionInfo>> readers = app.getWritingTo().stream().collect(Collectors.toMap(x -> x, grp -> {
            Group<Integer> group = groups.get(grp);
            return group.getReaderSchemasForApp(appId).join();
        }));
        ApplicationRecord.Application application = new ApplicationRecord.Application(appId, writers, readers, app.getProperties());
        return CompletableFuture.completedFuture(application);
    }

    @Synchronized
    @Override
    public CompletableFuture<AppsInGroupList> getReaderApps(String groupId) {
        Group<Integer> group = groups.get(groupId);
        if (group == null) {
            group = new Group<>(new InMemoryTable());
            groups.put(groupId, group);
        }
        return group.getReaderApps();
    }

    @Synchronized
    @Override
    public CompletableFuture<AppsInGroupList> getWriterApps(String groupId) {
        Group<Integer> group = groups.get(groupId);
        if (group == null) {
            group = new Group<>(new InMemoryTable());
            groups.put(groupId, group);
        }
        return group.getReaderApps();
    }

    @Synchronized
    @Override
    public CompletableFuture<Void> addWriter(String appId, String groupId, VersionInfo schemaVersion, Etag etag) {
        ApplicationRecord.ApplicationValue app = applications.get(appId);
        List<String> writingTo = Lists.newArrayList(app.getWritingTo());
        if (!writingTo.contains(groupId)) {
            writingTo.add(groupId);
        }
        Group<Integer> group = groups.get(groupId);
        
        return group.addWriter(appId, schemaVersion, etag);
    }

    @Synchronized
    @Override
    public CompletableFuture<Void> addReader(String appId, String groupId, VersionInfo schemaVersion, Etag etag) {
        ApplicationRecord.ApplicationValue app = applications.get(appId);
        List<String> writingTo = Lists.newArrayList(app.getWritingTo());
        if (!writingTo.contains(groupId)) {
            writingTo.add(groupId);
        }
        Group<Integer> group = groups.get(groupId);

        return group.addWriter(appId, schemaVersion, etag);
    }
    
    @Synchronized
    @Override
    public CompletableFuture<Void> removeWriter(String appId, String groupId) {
        Group<Integer> group = groups.get(groupId);
        return group.removeWriter(appId);
    }

    @Synchronized
    @Override
    public CompletableFuture<Void> removeReader(String appId, String groupId) {
        Group<Integer> group = groups.get(groupId);
        return group.removeReader(appId);
    }
}
