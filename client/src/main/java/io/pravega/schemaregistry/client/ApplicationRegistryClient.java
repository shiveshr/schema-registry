/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.client;

import io.pravega.schemaregistry.contract.data.Application;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.VersionInfo;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ApplicationRegistryClient {
    void addApplication(String appId, Map<String, String> properties);

    Application getApplication(String appId);

    void addWriter(String appId, String groupId, VersionInfo schemaVersion, CodecType codecType);

    void addReader(String appId, String groupId, VersionInfo schemaVersion, Set<CodecType> codecs);

    void removeWriter(String appId, String groupId);

    void removeReader(String appId, String groupId);

    Map<String, List<VersionInfo>> listWriterAppsInGroup(String groupId);

    Map<String, List<VersionInfo>> listReaderAppsInGroup(String groupId);
}
