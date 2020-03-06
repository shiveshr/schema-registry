/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.schemas;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import lombok.Data;
import lombok.Getter;
import lombok.SneakyThrows;

@Data
public class ProtobufSchema<T extends Message> implements SchemaData<T> {
    @Getter
    private final Parser<T> parser;
    @Getter
    private final DescriptorProtos.FileDescriptorSet descriptorProto;
    
    private final SchemaInfo schemaInfo;

    private ProtobufSchema(String name, Parser<T> parser, DescriptorProtos.FileDescriptorSet fileDescriptorSet) {
        this.parser = parser;
        this.descriptorProto = fileDescriptorSet;
        this.schemaInfo = new SchemaInfo(name, SchemaType.Protobuf, 
                getSchemaBytes(), ImmutableMap.of());
    }
    
    private byte[] getSchemaBytes() {
        return descriptorProto.toByteArray();
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }
    
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public static <T extends GeneratedMessageV3> ProtobufSchema<T> of(Class<T> tClass, DescriptorProtos.FileDescriptorSet fileDescriptorSet) {
        T defaultInstance = (T) tClass.getMethod("getDefaultInstance").invoke(null);
        Parser<T> tParser = (Parser<T>) defaultInstance.getParserForType();
        return new ProtobufSchema<>(tClass.getSimpleName(), tParser, fileDescriptorSet);
    }
    
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public static <T extends GeneratedMessageV3> ProtobufSchema<GeneratedMessageV3> of(String name, Class<T> tClass, DescriptorProtos.FileDescriptorSet fileDescriptorSet) {
        T defaultInstance = (T) tClass.getMethod("getDefaultInstance").invoke(null);
        Parser<GeneratedMessageV3> tParser = (Parser<GeneratedMessageV3>) defaultInstance.getParserForType();

        return new ProtobufSchema<>(name, tParser, fileDescriptorSet);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public static ProtobufSchema<DynamicMessage> of(String name, DescriptorProtos.FileDescriptorSet fileDescriptorSet) {
        return new ProtobufSchema<>(name, null, fileDescriptorSet);
    }
}

