/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.test.integrationtest.demo.function;

import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.serializers.PravegaSerializer;
import lombok.SneakyThrows;
import org.apache.curator.shaded.com.google.common.base.Charsets;

import java.io.OutputStream;

public class StringSerializerX implements PravegaSerializer<String> {
    @SneakyThrows
    @Override
    public void serialize(String var, SchemaInfo schema, OutputStream outputStream) {
        outputStream.write(var.getBytes(Charsets.UTF_8));
    }
}
