package io.pravega.schemaregistry.test.integrationtest.demo.function;

import io.pravega.schemaregistry.schemas.SchemaContainer;
import io.pravega.schemaregistry.serializers.PravegaDeserializer;
import io.pravega.schemaregistry.serializers.PravegaSerializer;

public interface SerDe<T> {
    PravegaSerializer<T> getSerializer();
    
    PravegaDeserializer<T> getDeserializer();
}
