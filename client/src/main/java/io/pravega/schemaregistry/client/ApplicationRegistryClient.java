package io.pravega.schemaregistry.client;

import io.pravega.schemaregistry.contract.data.Application;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaEvolution;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.VersionInfo;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public interface ApplicationRegistryClient {
    CompletableFuture<Void> addApplication(String appId, Map<String, String> properties);

    CompletableFuture<Application> getApplication(String appId, Function<VersionInfo, CompletableFuture<SchemaInfo>> getSchemaFromVersion);

    CompletableFuture<Void> addWriter(String appId, String groupId, VersionInfo schemaVersion,
                                             Function<String, CompletableFuture<GroupProperties>> groupProperties,
                                             Function<String, CompletableFuture<List<SchemaEvolution>>> groupHistory);

    CompletableFuture<Void> addReader(String appId, String groupId, VersionInfo schemaVersion,
                                             Function<String, CompletableFuture<GroupProperties>> groupProperties,
                                             Function<String, CompletableFuture<List<SchemaEvolution>>> groupHistory);

    CompletableFuture<Void> removeWriter(String appId, String groupId);

    CompletableFuture<Void> removeReader(String appId, String groupId);

    CompletableFuture<Map<String, List<VersionInfo>>> listWriterAppsInGroup(String groupId);

    CompletableFuture<Map<String, List<VersionInfo>>> listReaderAppsInGroup(String groupId);
}
