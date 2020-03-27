/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.test.integrationtest;

import io.pravega.schemaregistry.MapWithToken;
import io.pravega.schemaregistry.client.RegistryClient;
import io.pravega.schemaregistry.contract.data.Application;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaEvolution;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRule;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.service.ApplicationRegistryService;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import org.apache.commons.lang3.NotImplementedException;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PassthruRegistryClient implements RegistryClient {
    private final SchemaRegistryService service;
    private final ApplicationRegistryService appService;

    public PassthruRegistryClient(SchemaRegistryService service, ApplicationRegistryService appService) {
        this.service = service;
        this.appService = appService;
    }
    
    @Override
    public boolean addGroup(String group, SchemaType schemaType, SchemaValidationRules validationRules, 
                            boolean validateByObjectType, Map<String, String> properties) {
        return service.createGroup(group, new GroupProperties(schemaType, validationRules, validateByObjectType, properties)).join();
    }

    @Override
    public void removeGroup(String group) {
        service.deleteGroup(group).join();
    }

    @Override
    public Map<String, GroupProperties> listGroups() {
        return service.listGroups(null).thenApply(MapWithToken::getMap).join();
    }

    @Override
    public GroupProperties getGroupProperties(String group) {
        return service.getGroupProperties(group).join();
    }

    @Override
    public void updateSchemaValidationRules(String group, SchemaValidationRules validationRules) {
        service.updateSchemaValidationRules(group, validationRules).join();
    }

    @Override
    public void addSchemaValidationRule(String group, SchemaValidationRule rule) {
        throw new NotImplementedException("add rule not implemented");
    }

    @Override
    public void removeSchemaValidationRule(String group, SchemaValidationRule rule) {
        throw new NotImplementedException("add rule not implemented");
    }

    @Override
    public List<String> getObjectTypes(String group) {
        return service.getObjectTypes(group, null).join().getList();
    }

    @Override
    public VersionInfo addSchemaIfAbsent(String group, SchemaInfo schema) {
        return service.addSchemaIfAbsent(group, schema).join();
    }

    @Override
    public SchemaInfo getSchema(String group, VersionInfo version) {
        return service.getSchema(group, version).join();
    }

    @Override
    public EncodingInfo getEncodingInfo(String group, EncodingId encodingId) {
        return service.getEncodingInfo(group, encodingId).join();
    }

    @Override
    public EncodingId getEncodingId(String group, VersionInfo version, CodecType codecType) {
        return service.getEncodingId(group, version, codecType).join();
    }

    @Override
    public SchemaWithVersion getLatestSchema(String group, @Nullable String subgroup) {
        return service.getLatestSchema(group, subgroup).join();
    }

    @Override
    public List<SchemaEvolution> getGroupEvolutionHistory(String group, @Nullable String subgroup) {
        return service.getGroupEvolutionHistory(group, subgroup).join();
    }

    @Override
    public VersionInfo getSchemaVersion(String group, SchemaInfo schema) {
        return service.getSchemaVersion(group, schema).join();
    }

    @Override
    public boolean validateSchema(String group, SchemaInfo schema) {
        return service.validateSchema(group, schema).join();
    }

    @Override
    public boolean canRead(String group, SchemaInfo schema) {
        return service.canRead(group, schema).join();
    }

    @Override
    public List<CodecType> getCodecs(String group) {
        return service.getCodecTypes(group).join();
    }

    @Override
    public void addCodec(String group, CodecType codecType) {
        service.addCodec(group, codecType).join();
    }

    @Override
    public void addApplication(String appId, Map<String, String> properties) {
        appService.addApplication(appId, properties).join();
    }

    @Override
    public Application getApplication(String appId) {
        return appService.getApplication(appId, service::getSchema).join();
    }

    @Override
    public void addWriter(String appId, String groupId, VersionInfo schemaVersion, CodecType codecType) {
        appService.addWriter(appId, groupId, schemaVersion, service::getGroupProperties, 
                x -> service.getGroupEvolutionHistory(x, null)).join();
    }

    @Override
    public void addReader(String appId, String groupId, VersionInfo schemaVersion, Set<CodecType> codecs) {
        appService.addReader(appId, groupId, schemaVersion, service::getGroupProperties,
                x -> service.getGroupEvolutionHistory(x, null)).join();
    }

    @Override
    public void removeWriter(String appId, String groupId) {
        appService.removeWriter(appId, groupId).join();
    }

    @Override
    public void removeReader(String appId, String groupId) {
        appService.removeReader(appId, groupId).join();
    }

    @Override
    public Map<String, List<VersionInfo>> listWriterAppsInGroup(String groupId) {
        return appService.listWriterAppsInGroup(groupId).join();
    }

    @Override
    public Map<String, List<VersionInfo>> listReaderAppsInGroup(String groupId) {
        return appService.listReaderAppsInGroup(groupId).join();
    }
}
