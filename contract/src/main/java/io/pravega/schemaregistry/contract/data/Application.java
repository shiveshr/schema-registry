/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.contract.data;

import io.pravega.common.ObjectBuilder;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * A class that represents an application which is reading from (/writing to) one or more data source(/sink)
 * and using one or more schemas and codecs. 
 */
@Data
public class Application {
    /**
     * Unique name for the specific application.
     */
    private final String name;
    /**
     * Group to app's writers map.
     */
    private final Map<String, List<Writer>> writers;
    /**
     * Group to app's readers map.
     */
    private final Map<String, List<Reader>> readers;
    /**
     * A properties map to store any meaningful key value pairs.
     */
    private final Map<String, String> properties;
    
    @Data
    @Builder
    @AllArgsConstructor
    public static class Writer {
        private final String writerId;
        private final List<VersionInfo> versionInfos;
        private final CodecType codecType;
        
        public static class WriterBuilder implements ObjectBuilder<Writer> {
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    public static class Reader {
        private final String readerId;
        private final List<VersionInfo> versionInfos;
        private final List<CodecType> codecs;

        public static class ReaderBuilder implements ObjectBuilder<Reader> {
        }
    }
}
