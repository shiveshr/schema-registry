package io.pravega.schemaregistry.test.integrationtest.demo.function;

import io.pravega.schemaregistry.schemas.SchemaContainer;

public interface InputSchema<Input> {
    SchemaContainer<Input> getSchema();
}
