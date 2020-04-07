/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.test.integrationtest.demo.function.interfaces;

import io.pravega.schemaregistry.schemas.SchemaContainer;
import io.pravega.schemaregistry.serializers.PravegaDeserializer;
import io.pravega.schemaregistry.serializers.PravegaSerializer;

public interface SerDe<T> {
    PravegaSerializer<T> getSerializer();
    
    PravegaDeserializer<T> getDeserializer();

    SchemaContainer<T> getSchema();
}
