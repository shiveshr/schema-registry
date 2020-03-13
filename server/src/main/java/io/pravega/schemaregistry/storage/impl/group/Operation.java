/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl.group;

import io.pravega.schemaregistry.storage.records.TableRecords;
import lombok.Data;

import java.util.function.Predicate;

/**
 * Represents different operations on the Table. 
 */
interface Operation {
    class Noop implements Operation {
    }

    /**
     * Add new entry to the index. 
     */
    @Data
    class Add implements Operation {
        private final TableRecords.Key key;
        private final TableRecords.Record value;
    }

    /**
     * Add entry to an existing list value. 
     */
    @Data
    class AddToList implements Operation {
        private final TableRecords.Key key;
        private final TableRecords.Record value;
    }

    /**
     * Get and update operation if existing value satisfies the {@link #condition}. 
     */
    @Data
    class GetAndSet implements Operation {
        private final TableRecords.Key key;
        private final TableRecords.Record value;
        private final Predicate<TableRecords.Record> condition;
    }
}
