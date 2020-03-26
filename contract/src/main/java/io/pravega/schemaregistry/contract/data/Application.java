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

/**
 * A class that represents an application instance which is writing to and reading from potentially more than one 
 * data source and using one or ore schemas. 
 */
@Data
public class Application {
    /**
     * Unique name for the specific application instance.
     */
    private final String name;
    /**
     * Group to list of schema map.
     */
    private final Map<String, List<SchemaInfo>> writingToUsing;
    /**
     * Group to list of schema map.
     */
    private final Map<String, List<SchemaInfo>> readingFromUsing;
    /**
     * A properties map to store any meaningful key value pairs.
     */
    private final Map<String, String> properties;
}
