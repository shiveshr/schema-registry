/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage;

import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;

/**
 * Serializer for all table keys for {@link Table.TableKey}.
 */
public class TableKeySerializer extends VersionedSerializer.MultiType<Table.TableKey> {

    @Override
    protected void declareSerializers(Builder builder) {
        // Unused values (Do not repurpose!):
        // - 0: Unsupported Serializer.
        builder.serializer(Table.WriterKey.class, 1, new Table.WriterKey.Serializer())
               .serializer(Table.ReaderKey.class, 2, new Table.ReaderKey.Serializer())
               .serializer(Table.TableEtag.class, 3, new Table.TableEtag.Serializer());
    }

    /**
     * Serializes the given {@link Table.TableKey} to a {@link ByteBuffer}.
     *
     * @param value The {@link Table.TableKey} to serialize.
     * @return A base 64 encoding of wrapping an array that contains the serialization.
     */
    @SneakyThrows(IOException.class)
    public String toKeyString(Table.TableKey value) {
        ByteArraySegment s = serialize(value);
        return value.getClass().getSimpleName() + "_" + Base64.getEncoder().encodeToString(s.getCopy());
    }
    
    /**
     * Deserializes the given base 64 encoded key string into a {@link Table.TableKey} instance.
     *
     * @param string string to deserialize into key.
     * @return A new {@link Table.TableKey} instance from the given serialization.
     */
    @SneakyThrows(IOException.class)
    public Table.TableKey fromString(String string) {
        String[] tokens = string.split("_");
        
        byte[] buffer = Base64.getDecoder().decode(tokens[1]);
        return deserialize(new ByteArraySegment(buffer));
    }
}
