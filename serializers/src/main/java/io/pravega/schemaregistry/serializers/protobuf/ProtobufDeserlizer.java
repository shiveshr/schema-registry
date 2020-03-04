/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.serializers.protobuf;

import com.google.protobuf.Message;
import io.pravega.schemaregistry.cache.EncodingCache;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.compression.Compressor;
import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.schemas.ProtobufSchema;
import io.pravega.schemaregistry.serializers.AbstractPravegaDeserializer;
import lombok.SneakyThrows;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Map;

public class ProtobufDeserlizer<T extends Message> extends AbstractPravegaDeserializer<T> {
    ProtobufSchema<T> protobufSchema;
    public ProtobufDeserlizer(String groupId, SchemaRegistryClient client,
                              @Nullable ProtobufSchema<T> schema, Map<CompressionType, Compressor> compressorMap,
                              EncodingCache encodingCache) {
        super(groupId, client, schema, true, compressorMap, encodingCache);
        this.protobufSchema = schema;
    }

    @SneakyThrows
    @Override
    protected T deserialize(ByteBuffer buffer, SchemaInfo writerSchemaInfo, SchemaInfo readerSchemaInfo) {
        return protobufSchema.getParser().parseFrom(buffer);
    }
}
