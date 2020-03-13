/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.records;

import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;

public class IndexKeySerializer extends VersionedSerializer.MultiType<TableRecords.Key> {

    @Override
    protected void declareSerializers(Builder builder) {
        // Unused values (Do not repurpose!):
        // - 0: Unsupported Serializer.
        builder.serializer(TableRecords.VersionInfoKey.class, 1, new TableRecords.VersionInfoKey.Serializer())
               .serializer(TableRecords.SchemaFingerprintKey.class, 2, new TableRecords.SchemaFingerprintKey.Serializer())
               .serializer(TableRecords.LatestSchemaVersionKey.class, 3, new TableRecords.LatestSchemaVersionKey.Serializer())
               .serializer(TableRecords.GroupPropertyKey.class, 4, new TableRecords.GroupPropertyKey.Serializer())
               .serializer(TableRecords.ValidationRulesKey.class, 5, new TableRecords.ValidationRulesKey.Serializer())
               .serializer(TableRecords.EncodingId.class, 6, new TableRecords.EncodingId.Serializer())
               .serializer(TableRecords.EncodingInfo.class, 7, new TableRecords.EncodingInfo.Serializer())
               .serializer(TableRecords.LatestEncodingIdKey.class, 8, new TableRecords.LatestEncodingIdKey.Serializer())
               .serializer(TableRecords.Etag.class, 9, new TableRecords.Etag.Serializer());
    }
    
    /**
     * Serializes the given {@link TableRecords.Key} to a {@link ByteBuffer}.
     *
     * @param value The {@link TableRecords.Key} to serialize.
     * @return A base 64 encoding of wrapping an array that contains the serialization.
     */
    @SneakyThrows(IOException.class)
    public String toKeyString(TableRecords.Key value) {
        ByteArraySegment s = serialize(value);
        return value.getClass().getSimpleName() + "_" + Base64.getEncoder().encodeToString(s.getCopy());
    }
    
    /**
     * Deserializes the given base 64 encoded key string into a {@link TableRecords.Key} instance.
     *
     * @param string string to deserialize into key.
     * @return A new {@link TableRecords.Key} instance from the given serialization.
     */
    @SneakyThrows(IOException.class)
    public TableRecords.Key fromString(String string) {
        String[] tokens = string.split("_");
        
        byte[] buffer = Base64.getDecoder().decode(tokens[1]);
        return deserialize(new ByteArraySegment(buffer));
    }
}
