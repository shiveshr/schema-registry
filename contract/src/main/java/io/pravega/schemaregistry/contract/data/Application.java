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

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class Application {
    private final String name;
    /**
     * Group to list of schema map.
     */
    private final Map<String, List<SchemaInfo>> writingToUsing;
    /**
     * Group to list of schema map.
     */
    private final Map<String, List<SchemaInfo>> readingFromUsing;
    private final Map<String, String> properties;
}
