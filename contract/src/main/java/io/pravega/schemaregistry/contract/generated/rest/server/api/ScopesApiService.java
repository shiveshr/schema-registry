package io.pravega.schemaregistry.contract.generated.rest.server.api;

import io.pravega.schemaregistry.contract.generated.rest.server.api.*;
import io.pravega.schemaregistry.contract.generated.rest.model.*;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import io.pravega.schemaregistry.contract.generated.rest.model.AddCodec;
import io.pravega.schemaregistry.contract.generated.rest.model.AddSchemaRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CanReadRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CodecsList;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingId;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetSchemaVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupHistory;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties;
import io.pravega.schemaregistry.contract.generated.rest.model.ListGroupsResponse;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaNamesList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaValidationRules;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaVersionsList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.UpdateValidationRulesRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.ValidateRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo;

import java.util.List;
import io.pravega.schemaregistry.contract.generated.rest.server.api.NotFoundException;

import java.io.InputStream;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.validation.constraints.*;

public abstract class ScopesApiService {
    public abstract Response addCodec(String scopeName,String groupName,AddCodec addCodec,SecurityContext securityContext) throws NotFoundException;
    public abstract Response addSchema(String scopeName,String groupName,AddSchemaRequest addSchemaRequest, String schemaName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response canRead(String scopeName,String groupName,CanReadRequest canReadRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response createGroup(String scopeName,CreateGroupRequest createGroupRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response deleteGroup(String scopeName,String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response deleteSchemaVersion(String scopeName,String groupName,Integer version,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getCodecsList(String scopeName,String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getEncodingId(String scopeName,String groupName,GetEncodingIdRequest getEncodingIdRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getEncodingInfo(String scopeName,String groupName,Integer encodingId,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getGroupHistory(String scopeName,String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getGroupProperties(String scopeName,String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getLatestSchema(String scopeName,String groupName, String schemaName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getSchemaFromVersion(String scopeName,String groupName,Integer version,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getSchemaNames(String scopeName,String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getSchemaValidationRules(String scopeName,String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getSchemaVersion(String scopeName,String groupName,GetSchemaVersion getSchemaVersion,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getSchemas(String scopeName,String groupName, String schemaName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response listGroups(String scopeName, String continuationToken, Integer limit,SecurityContext securityContext) throws NotFoundException;
    public abstract Response updateSchemaValidationRules(String scopeName,String groupName,UpdateValidationRulesRequest updateValidationRulesRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response validate(String scopeName,String groupName,ValidateRequest validateRequest,SecurityContext securityContext) throws NotFoundException;
}
