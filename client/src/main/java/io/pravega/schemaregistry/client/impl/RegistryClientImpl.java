/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.client.impl;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.pravega.schemaregistry.client.RegistryClient;
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
import io.pravega.schemaregistry.contract.exceptions.IncompatibleSchemaException;
import io.pravega.schemaregistry.contract.exceptions.NotFoundException;
import io.pravega.schemaregistry.contract.exceptions.SchemaTypeMismatchException;
import io.pravega.schemaregistry.contract.generated.rest.model.AddReaderRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.AddSchemaToGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.AddWriterRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.Application;
import io.pravega.schemaregistry.contract.generated.rest.model.ApplicationsInGroup;
import io.pravega.schemaregistry.contract.generated.rest.model.CanRead;
import io.pravega.schemaregistry.contract.generated.rest.model.CanReadRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CodecsList;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateApplicationRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetSchemaVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupsList;
import io.pravega.schemaregistry.contract.generated.rest.model.IncompatibleSchema;
import io.pravega.schemaregistry.contract.generated.rest.model.ObjectTypesList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaList;
import io.pravega.schemaregistry.contract.generated.rest.model.UpdateValidationRulesPolicyRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.Valid;
import io.pravega.schemaregistry.contract.generated.rest.model.ValidateRequest;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import org.apache.commons.lang3.NotImplementedException;
import org.glassfish.jersey.client.ClientConfig;

import javax.annotation.Nullable;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RegistryClientImpl implements RegistryClient {
    private static final HashFunction HASH = Hashing.murmur3_128();
    private final Client client = ClientBuilder.newClient(new ClientConfig());
    private final URI uri;

    public RegistryClientImpl(URI uri) {
        this.uri = uri;
    }
    
    @Override
    public boolean addGroup(String group, SchemaType schemaType, SchemaValidationRules validationRules, boolean validateByObjectType, Map<String, String> properties) {
        WebTarget webTarget = client.target(uri).path("v1/groups");

        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);

        io.pravega.schemaregistry.contract.generated.rest.model.SchemaType schemaTypeModel = ModelHelper.encode(schemaType);

        io.pravega.schemaregistry.contract.generated.rest.model.SchemaValidationRules compatibility = ModelHelper.encode(validationRules);
        CreateGroupRequest request = new CreateGroupRequest().schemaType(schemaTypeModel)
                                                             .properties(properties).validateByObjectType(validateByObjectType)
                                                             .groupName(group)
                                                             .validationRules(compatibility);
        Response response = invocationBuilder.post(Entity.entity(request, MediaType.APPLICATION_JSON));
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return true;
        } else if (response.getStatus() == Response.Status.CONFLICT.getStatusCode()) {
            return false;
        } else {
            throw new RuntimeException("Internal Service error. Failed to add the group.");
        }
    }

    @Override
    public void removeGroup(String group) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.delete();
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw new RuntimeException("Internal Service error. Failed to remove the group.");
        }
    }

    @Override
    public Map<String, GroupProperties> listGroups() {
        WebTarget webTarget = client.target(uri).path("v1/groups");

        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);

        Response response = invocationBuilder.get();
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw new RuntimeException("Internal Service error. Failed to list groups.");
        }

        GroupsList entity = response.readEntity(GroupsList.class);
        return entity.getGroups().stream().collect(Collectors.toMap(x -> x.getGroupName(), 
                x -> {
                    SchemaType schemaType = ModelHelper.decode(x.getSchemaType());
                    SchemaValidationRules rules = ModelHelper.decode(x.getSchemaValidationRules());
                    return new GroupProperties(schemaType, rules, x.isValidateByObjectType(), x.getProperties());
                }));
    }

    @Override
    public GroupProperties getGroupProperties(String group) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.get();
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties.class));
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new NotFoundException("Group not found.");
        } else {
            throw new RuntimeException("Internal error. Failed to get group properties.");
        }
    }

    @Override
    public void updateSchemaValidationRules(String group, SchemaValidationRules validationRules) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        UpdateValidationRulesPolicyRequest request = new UpdateValidationRulesPolicyRequest();
        request.setValidationRules(ModelHelper.encode(validationRules));

        Response response = invocationBuilder.put(Entity.entity(request, MediaType.APPLICATION_JSON));
        if (response.getStatus() == Response.Status.PRECONDITION_FAILED.getStatusCode()) {
            throw new RuntimeException("Conflict attempting to update the rules. Try again.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to update schema validation rules.");
        }
    }

    @Override
    public void addSchemaValidationRule(String group, SchemaValidationRule rule) {
        throw new NotImplementedException("SchemaValidationRule not implemented");
    }

    @Override
    public void removeSchemaValidationRule(String group, SchemaValidationRule rule) {
        throw new NotImplementedException("SchemaValidationRule not implemented");
    }

    @Override
    public List<String> getObjectTypes(String group) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group).path("objectTypes");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.get();
        ObjectTypesList objectTypesList = response.readEntity(ObjectTypesList.class);
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return objectTypesList.getGroups();
        } else {
            throw new RuntimeException("Internal Service error. Failed to get objectTypes.");
        }    
    }

    @Override
    public VersionInfo addSchemaIfAbsent(String group, SchemaInfo schema) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group).path("schemas");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        AddSchemaToGroupRequest addSchemaToGroupRequest = new AddSchemaToGroupRequest();
        addSchemaToGroupRequest.schemaInfo(ModelHelper.encode(schema));
        Response response = invocationBuilder.post(Entity.entity(addSchemaToGroupRequest, MediaType.APPLICATION_JSON));
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo.class));
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new NotFoundException("Group not found.");
        } else if (response.getStatus() == Response.Status.CONFLICT.getStatusCode()) {
            throw new IncompatibleSchemaException("Schema is incompatible.");
        } else if (response.getStatus() == Response.Status.EXPECTATION_FAILED.getStatusCode()) {
            throw new SchemaTypeMismatchException("Schema type is invalid.");
        } else if (response.getStatus() == Response.Status.PRECONDITION_FAILED.getStatusCode()) {
            throw new RuntimeException("Write conflict.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to addSchema.");
        }
    }

    @Override
    public SchemaInfo getSchema(String group, VersionInfo version) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group).path("schemas").path("versions").path(Integer.toString(version.getVersion()));
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.get();
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo.class));
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new NotFoundException("Schema not found.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get schema.");
        }
    }

    @Override
    public EncodingInfo getEncodingInfo(String group, EncodingId encodingId) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group).path("encodings").path(Integer.toString(encodingId.getId()));
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.get();
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo.class));
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new NotFoundException("Encoding not found.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get encoding info.");
        }
    }

    @Override
    public EncodingId getEncodingId(String group, VersionInfo version, CodecType codecType) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group).path("encodings");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        GetEncodingIdRequest getEncodingIdRequest = new GetEncodingIdRequest();
        getEncodingIdRequest.codecType(ModelHelper.encode(codecType))
                            .versionInfo(ModelHelper.encode(version));
        Response response = invocationBuilder.put(Entity.entity(getEncodingIdRequest, MediaType.APPLICATION_JSON));
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.EncodingId.class));
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new RuntimeException("getEncodingId failed. Either Group or Version does not exist.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get encoding info.");
        }
    }

    @Override
    public SchemaWithVersion getLatestSchema(String group, @Nullable String objectTypeName) {
        if (objectTypeName == null) {
            return getLatestSchemaForGroup(group);
        } else {
            return getLatestSchemaByObjectType(group, objectTypeName);
        }
    }

    private SchemaWithVersion getLatestSchemaForGroup(String group) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group).path("schemas").path("versions").path("latest");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.get();
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return processLatestSchemaResponse(response);
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new RuntimeException("getLatestSchemaForGroup failed. Either Group or Version does not exist.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get latest schema for group.");
        }
    }

    private SchemaWithVersion getLatestSchemaByObjectType(String group, String objectTypeName) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group)
                                    .path("objectTypes").path(objectTypeName).path("schemas").path("versions").path("latest");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.get();
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return processLatestSchemaResponse(response);
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new RuntimeException("getLatestSchemaForGroup failed. Either Group or Version does not exist.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get latest schema for group.");
        }
    }

    private SchemaWithVersion processLatestSchemaResponse(Response response) {
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion.class));
        } else {
            throw new RuntimeException("Internal Service error. Failed to get encoding info.");
        }
    }

    @Override
    public List<SchemaEvolution> getGroupEvolutionHistory(String group, @Nullable String objectTypeName) {
        if (objectTypeName == null) {
            return getEvolutionHistory(group);
        } else {
            return getEvolutionHistoryByObjectType(group, objectTypeName);
        }
    }
    
    private List<SchemaEvolution> getEvolutionHistory(String group) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group).path("schemas").path("versions");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.get();
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return processHistoryResponse(response);
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new RuntimeException("getEvolutionHistory failed. Either Group or Version does not exist.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get schema evolution history for group.");
        }
    }

    private List<SchemaEvolution> getEvolutionHistoryByObjectType(String group, String objectTypeName) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group).path("objectTypes").path(objectTypeName)
                                    .path("schemas").path("versions");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.get();
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return processHistoryResponse(response);
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new RuntimeException("getEvolutionHistory failed. Either Group or Version does not exist.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get schema evolution history for group.");
        }
    }

    private List<SchemaEvolution> processHistoryResponse(Response response) {
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            SchemaList schemaList = response.readEntity(SchemaList.class);
            return schemaList.getSchemas().stream().map(ModelHelper::decode).collect(Collectors.toList());
        } else {
            throw new RuntimeException("Internal Service error. Failed to get group history.");
        }
    }

    @Override
    public VersionInfo getSchemaVersion(String group, SchemaInfo schema) {
        long fingerprint = HASH.hashBytes(schema.getSchemaData()).asLong();

        WebTarget webTarget = client.target(uri).path("v1/groups").path(group).path("schemas/schema").path(Long.toString(fingerprint));

        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);

        GetSchemaVersion getSchemaVersion = new GetSchemaVersion().schemaInfo(ModelHelper.encode(schema));

        Response response = invocationBuilder.post(Entity.entity(getSchemaVersion, MediaType.APPLICATION_JSON));
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo.class));
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new NotFoundException("Schema not found.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get schema version.");
        }
    }

    @Override
    public boolean validateSchema(String group, SchemaInfo schema) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group).path("schemas").path("validate");
        
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        ValidateRequest validateRequest = new ValidateRequest()
                .schemaInfo(ModelHelper.encode(schema));
        Response response = invocationBuilder.post(Entity.entity(validateRequest, MediaType.APPLICATION_JSON));
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return response.readEntity(Valid.class).isValid();
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new NotFoundException("Schema not found.");
        } else {
            throw new RuntimeException("Internal Service error.");
        }
    }

    @Override
    public boolean canRead(String group, SchemaInfo schema) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group).path("schemas").path("canRead");

        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        CanReadRequest request = new CanReadRequest().schemaInfo(ModelHelper.encode(schema));
        Response response = invocationBuilder.post(Entity.entity(request, MediaType.APPLICATION_JSON));
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return response.readEntity(CanRead.class).isCompatible();
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new NotFoundException("Schema not found.");
        } else {
            throw new RuntimeException("Internal Service error.");
        }
    }
    
    @Override
    public List<CodecType> getCodecs(String group) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group).path("codecs");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.get();
        CodecsList list = response.readEntity(CodecsList.class);
        
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return list.getCodecTypes().stream().map(ModelHelper::decode).collect(Collectors.toList());
        } else {
            throw new RuntimeException("Failed to get codecs");
        }    
    }

    @Override
    public void addCodec(String group, CodecType codecType) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group).path("codecs");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.post(Entity.entity(ModelHelper.encode(codecType), MediaType.APPLICATION_JSON));

        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw new RuntimeException("Failed to add codec");
        }
    }
    
    @Override
    public void addApplication(String appId, Map<String, String> properties) {
        WebTarget webTarget = client.target(uri).path("v1/applications");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        CreateApplicationRequest request = new CreateApplicationRequest()
                .applicationName(appId).properties(properties);
        Response response = invocationBuilder.post(Entity.entity(request, MediaType.APPLICATION_JSON));

        if (!(response.getStatus() == Response.Status.OK.getStatusCode())) {
            throw new RuntimeException("Failed to add application");
        }
    }

    @Override
    public io.pravega.schemaregistry.contract.data.Application getApplication(String appId) {
        WebTarget webTarget = client.target(uri).path("v1/applications").path(appId);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.get();

        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            Application app = response.readEntity(Application.class);
            return ModelHelper.decode(app);
        } else {
            throw new RuntimeException("Failed to add application");
        }
    }

    @Override
    public void addWriter(String appId, String groupId, VersionInfo schemaVersion) {
        WebTarget webTarget = client.target(uri).path("v1/applications").path(appId).path("writers");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        AddWriterRequest request = new AddWriterRequest()
                .groupId(groupId).version(ModelHelper.encode(schemaVersion));
        Response response = invocationBuilder.post(Entity.entity(request, MediaType.APPLICATION_JSON));

        if (response.getStatus() == Response.Status.CONFLICT.getStatusCode()) {
            IncompatibleSchema error = response.readEntity(IncompatibleSchema.class);
            throw new IncompatibleSchemaException(error.getErrorMessage());
        } else if (!(response.getStatus() == Response.Status.OK.getStatusCode())) {
            throw new RuntimeException("Failed to add writer application");
        }
    }

    @Override
    public void addReader(String appId, String groupId, VersionInfo schemaVersion) {
        WebTarget webTarget = client.target(uri).path("v1/applications").path(appId).path("readers");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        AddReaderRequest request = new AddReaderRequest()
                .groupId(groupId).version(ModelHelper.encode(schemaVersion));
        Response response = invocationBuilder.post(Entity.entity(request, MediaType.APPLICATION_JSON));

        if (response.getStatus() == Response.Status.CONFLICT.getStatusCode()) {
            IncompatibleSchema error = response.readEntity(IncompatibleSchema.class);
            throw new IncompatibleSchemaException(error.getErrorMessage());
        } else if (!(response.getStatus() == Response.Status.OK.getStatusCode())) {
            throw new RuntimeException("Failed to add reader application");
        }
    }

    @Override
    public void removeWriter(String appId, String groupId) {
        WebTarget webTarget = client.target(uri).path("v1/applications").path(appId).path("groups").path(groupId).path("reader");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.delete();

        if (!(response.getStatus() == Response.Status.NO_CONTENT.getStatusCode())) {
            throw new RuntimeException("Failed to add reader application");
        }
    }

    @Override
    public void removeReader(String appId, String groupId) {
        WebTarget webTarget = client.target(uri).path("v1/applications").path(appId).path("groups").path(groupId).path("writer");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.delete();

        if (!(response.getStatus() == Response.Status.NO_CONTENT.getStatusCode())) {
            throw new RuntimeException("Failed to add reader application");
        }
    }

    @Override
    public Map<String, List<VersionInfo>> listWriterAppsInGroup(String groupId) {
        WebTarget webTarget = client.target(uri).path("v1/applications/groups").path(groupId).path("writers");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.get();

        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            ApplicationsInGroup app = response.readEntity(ApplicationsInGroup.class);
            return app.getMap().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, 
                    x -> x.getValue().getVersions().stream().map(ModelHelper::decode).collect(Collectors.toList())));
        } else {
            throw new RuntimeException("Failed to get writers in group");
        }
    }

    @Override
    public Map<String, List<VersionInfo>> listReaderAppsInGroup(String groupId) {
        WebTarget webTarget = client.target(uri).path("v1/applications/groups").path(groupId).path("readers");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.get();

        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            ApplicationsInGroup app = response.readEntity(ApplicationsInGroup.class);
            return app.getMap().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                    x -> x.getValue().getVersions().stream().map(ModelHelper::decode).collect(Collectors.toList())));
        } else {
            throw new RuntimeException("Failed to get writers in group");
        }
    }
}
