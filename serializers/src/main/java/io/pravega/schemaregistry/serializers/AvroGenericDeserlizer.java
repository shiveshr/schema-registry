/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.serializers;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.pravega.schemaregistry.cache.EncodingCache;
import io.pravega.schemaregistry.client.RegistryClient;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.schemas.AvroSchema;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

class AvroGenericDeserlizer extends AbstractPravegaDeserializer<GenericRecord> {
    private final LoadingCache<byte[], Schema> knownSchemas;

    AvroGenericDeserlizer(String groupId, RegistryClient client, @Nullable AvroSchema<GenericRecord> schema,
                          SerializerConfig.Decoder decoder, EncodingCache encodingCache) {
        super(groupId, client, schema, false, decoder, encodingCache);
        this.knownSchemas = CacheBuilder.newBuilder().build(new CacheLoader<byte[], Schema>() {
            @Override
            public Schema load(byte[] schemaData) throws Exception {
                String schemaString = new String(schemaData, Charsets.UTF_8);
                return new Schema.Parser().parse(schemaString);
            }
        });
    }

    @SneakyThrows
    @Override
    protected GenericRecord deserialize(ByteBuffer buffer, SchemaInfo writerSchemaInfo, SchemaInfo readerSchemaInfo) {
        Preconditions.checkNotNull(writerSchemaInfo);
        Schema writerSchema = knownSchemas.get(writerSchemaInfo.getSchemaData());
        Schema readerSchema = knownSchemas.get(readerSchemaInfo.getSchemaData());
        
        GenericDatumReader<GenericRecord> genericDatumReader = new GenericDatumReader<>(writerSchema, readerSchema);
        byte[] array = new byte[buffer.remaining()];
        buffer.get(array);

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(array, null);
        return genericDatumReader.read(null, decoder);
    }
}
