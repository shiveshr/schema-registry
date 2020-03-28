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
import io.pravega.schemaregistry.contract.data.VersionInfo;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class ReadersInGroupWithEtag {
    // appid to list of readers
    private final Map<String, List<Application.Reader>> appIdWithSchemaVersions;
    private final Etag etag;
}
