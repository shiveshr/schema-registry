package io.pravega.schemaregistry.contract.generated.rest.server.api;

import io.pravega.schemaregistry.contract.generated.rest.server.api.*;
import io.pravega.schemaregistry.contract.generated.rest.model.*;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import io.pravega.schemaregistry.contract.generated.rest.model.CanRead;
import io.pravega.schemaregistry.contract.generated.rest.model.CodecTypesList;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingId;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupHistory;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties;
import io.pravega.schemaregistry.contract.generated.rest.model.ListGroupsResponse;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaVersionsList;
import io.pravega.schemaregistry.contract.generated.rest.model.UpdateValidationRulesRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.Valid;
import io.pravega.schemaregistry.contract.generated.rest.model.ValidateRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo;

import java.util.List;
import io.pravega.schemaregistry.contract.generated.rest.server.api.NotFoundException;

import java.io.InputStream;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.validation.constraints.*;

public abstract class GroupsApiService {
    public abstract Response addCodecType(String groupName,String codecType,SecurityContext securityContext) throws NotFoundException;
    public abstract Response addSchema(String groupName,SchemaInfo schemaInfo,SecurityContext securityContext) throws NotFoundException;
    public abstract Response canRead(String groupName,SchemaInfo schemaInfo,SecurityContext securityContext) throws NotFoundException;
    public abstract Response createGroup(CreateGroupRequest createGroupRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response deleteGroup(String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response deleteSchemaVersion(String groupName,String type,Integer version,SecurityContext securityContext) throws NotFoundException;
    public abstract Response deleteSchemaVersionOrinal(String groupName,Integer versionOrdinal,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getCodecTypesList(String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getEncodingId(String groupName,GetEncodingIdRequest getEncodingIdRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getEncodingInfo(String groupName,Integer encodingId,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getGroupHistory(String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getGroupProperties(String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getSchemaFromVersion(String groupName,String type,Integer version,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getSchemaFromVersionOrdinal(String groupName,Integer versionOrdinal,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getSchemaVersion(String groupName,SchemaInfo schemaInfo,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getSchemaVersions(String groupName, String type,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getSchemas(String groupName, String type,SecurityContext securityContext) throws NotFoundException;
    public abstract Response listGroups( String continuationToken, Integer limit,SecurityContext securityContext) throws NotFoundException;
    public abstract Response updateSchemaValidationRules(String groupName,UpdateValidationRulesRequest updateValidationRulesRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response validate(String groupName,ValidateRequest validateRequest,SecurityContext securityContext) throws NotFoundException;
}
