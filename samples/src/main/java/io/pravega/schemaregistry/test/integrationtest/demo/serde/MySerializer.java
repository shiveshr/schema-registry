/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.test.integrationtest.demo.serde;

import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.serializers.PravegaSerializer;
import lombok.SneakyThrows;

import java.io.OutputStream;

public class MySerializer implements PravegaSerializer<MyPojo> {
    private final JavaSerializer<MyPojo> javaSerializer = new JavaSerializer<>();
    @SneakyThrows
    @Override
    public void serialize(MyPojo var, SchemaInfo schema, OutputStream outputStream) {
        outputStream.write(javaSerializer.serialize(var).array());
    }
}
