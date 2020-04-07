/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.test.integrationtest.demo.function.test;

import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.schemas.SchemaContainer;
import io.pravega.schemaregistry.serializers.PravegaDeserializer;
import io.pravega.schemaregistry.serializers.PravegaSerializer;
import io.pravega.schemaregistry.test.integrationtest.demo.function.interfaces.SerDe;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.Map;

public class WordCountSerDe implements SerDe<Map<String, Integer>> {
    @Override
    public PravegaSerializer<Map<String, Integer>> getSerializer() {
        return (var, schema, outputStream) -> {
            ObjectOutputStream out;
            try {
                out = new ObjectOutputStream(outputStream);
                out.writeObject(var);
                out.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public PravegaDeserializer<Map<String, Integer>> getDeserializer() {
        return new PravegaDeserializer<Map<String, Integer>>() {
            @Override
            @SuppressWarnings("unchecked")
            public Map<String, Integer> deserialize(InputStream inputStream, SchemaInfo writerSchema, SchemaInfo readerSchema) {
                ObjectInputStream oin;
                try {
                    oin = new ObjectInputStream(inputStream);
                    return (Map<String, Integer>) oin.readObject();
                } catch (IOException | ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }

            }
        };
    }

    @Override
    public SchemaContainer<Map<String, Integer>> getSchema() {
        return () -> new SchemaInfo("myOutput", SchemaType.custom("myOutput"), new byte[0], Collections.emptyMap());
    }
}
