/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl;

import io.pravega.schemaregistry.storage.Position;
import io.pravega.schemaregistry.storage.records.Record;
import io.pravega.schemaregistry.storage.records.RecordWithPosition;

import java.util.List;

public interface Log {
    Position getCurrentEtag();

    Position writeToLog(Record record, Position etag);

    <T extends Record> T readAt(Position position, Class<T> tClass);

    List<RecordWithPosition> readFrom(Position position);
}
