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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Schema Store interface for storing and retrieving and querying schemas. 
 */
public interface ApplicationStore {
    CompletableFuture<Void> addApplication(String appId, Map<String, String> properties);
    
    CompletableFuture<Application> getApplication(String appId);

    CompletableFuture<ReadersInGroupWithEtag> getReaderApps(String groupId);
    
    CompletableFuture<WritersInGroupWithEtag> getWriterApps(String groupId);

    CompletableFuture<Void> addWriter(String appId, String groupId, VersionInfo schemaVersion, CodecType codecType, Etag etag);
    
    CompletableFuture<Void> addReader(String appId, String groupId, VersionInfo schemaVersion, List<CodecType> codecType, Etag etag);
    
    CompletableFuture<Void> removeWriter(String appId, String groupId);
    
    CompletableFuture<Void> removeReader(String appId, String groupId);
}
