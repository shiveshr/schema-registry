package io.pravega.schemaregistry.contract.generated.rest.server.api;

import io.pravega.schemaregistry.contract.generated.rest.model.*;
import io.pravega.schemaregistry.contract.generated.rest.server.api.GroupsApiService;
import io.pravega.schemaregistry.contract.generated.rest.server.api.factories.GroupsApiServiceFactory;

import io.swagger.annotations.ApiParam;
import io.swagger.jaxrs.*;

import io.pravega.schemaregistry.contract.generated.rest.model.AddSchemaToGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CanReadRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CompressionsList;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingId;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetSchemaForObjectTypeByVersionRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetSchemaFromVersionRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetSchemaVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupsList;
import io.pravega.schemaregistry.contract.generated.rest.model.ObjectTypesList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaValidationRules;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.UpdateValidationRulesPolicyRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.ValidateRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo;

import java.util.Map;
import java.util.List;
import io.pravega.schemaregistry.contract.generated.rest.server.api.NotFoundException;

import java.io.InputStream;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.servlet.ServletConfig;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.*;
import javax.validation.constraints.*;

@Path("/groups")


@io.swagger.annotations.Api(description = "the groups API")

public class GroupsApi  {
   private final GroupsApiService delegate;

   public GroupsApi(@Context ServletConfig servletContext) {
      GroupsApiService delegate = null;

      if (servletContext != null) {
         String implClass = servletContext.getInitParameter("GroupsApi.implementation");
         if (implClass != null && !"".equals(implClass.trim())) {
            try {
               delegate = (GroupsApiService) Class.forName(implClass).newInstance();
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         } 
      }

      if (delegate == null) {
         delegate = GroupsApiServiceFactory.getGroupsApi();
      }

      this.delegate = delegate;
   }

    @POST
    @Path("/{groupName}/schemas")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "adds a new schema to the group", response = VersionInfo.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added schema to the group", response = VersionInfo.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 409, message = "Incompatible schema", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a Group", response = Void.class) })
    public Response addSchemaToGroupIfAbsent(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Add new schema to group" ,required=true) AddSchemaToGroupRequest addSchemaToGroupRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.addSchemaToGroupIfAbsent(groupName,addSchemaToGroupRequest,securityContext);
    }
    @POST
    @Path("/{groupName}/schemas/canRead")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "check if given schema can be used for reads subject to compatibility policy in the schema validation rules.", response = Void.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Schema can be used to read", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response canRead(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Checks if schema can be used to read the data in the stream based on compatibility rules." ,required=true) CanReadRequest canReadRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.canRead(groupName,canReadRequest,securityContext);
    }
    @POST
    
    @Consumes({ "application/json" })
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "Create a new Group", response = Void.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added group", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 409, message = "Group with given name already exists", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a Group", response = Void.class) })
    public Response createGroup(@ApiParam(value = "The Group configuration" ,required=true) CreateGroupRequest createGroupRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.createGroup(createGroupRequest,securityContext);
    }
    @DELETE
    @Path("/{groupName}")
    
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "Delete a Group", response = Void.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 204, message = "Successfully deleted the Group", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while deleting the Group", response = Void.class) })
    public Response deleteGroup(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.deleteGroup(groupName,securityContext);
    }
    @GET
    @Path("/{groupName}/compressions")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = CompressionsList.class, tags={ "Encoding", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found Compressions", response = CompressionsList.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group or encoding id with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getCompressionsList(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getCompressionsList(groupName,securityContext);
    }
    @GET
    @Path("/{groupName}/encodings/{encodingId}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = EncodingInfo.class, tags={ "Encoding", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found Encoding", response = EncodingInfo.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group or encoding id with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getEncodingInfo(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Encoding id that identifies a unique combination of encoding and compression",required=true) @PathParam("encodingId") Integer encodingId
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getEncodingInfo(groupName,encodingId,securityContext);
    }
    @GET
    @Path("/{groupName}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = GroupProperties.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group properties", response = GroupProperties.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getGroupProperties(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getGroupProperties(groupName,securityContext);
    }
    @GET
    @Path("/{groupName}/schemas/versions")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaList.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Versioned history of schemas registered under the group", response = SchemaList.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getGroupSchemas(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getGroupSchemas(groupName,securityContext);
    }
    @GET
    @Path("/{groupName}/schemas/versions/latest")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaWithVersion.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group properties", response = SchemaWithVersion.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getLatestGroupSchema(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getLatestGroupSchema(groupName,securityContext);
    }
    @GET
    @Path("/{groupName}/objectTypes/{objectTypeName}/schemas/versions/latest")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaWithVersion.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found latest schema in objectType", response = SchemaWithVersion.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getLatestSchemaForObjectType(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Object type",required=true) @PathParam("objectTypeName") String objectTypeName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getLatestSchemaForObjectType(groupName,objectTypeName,securityContext);
    }
    @GET
    @Path("/{groupName}/objectTypes/{objectTypeName}/schemas/versions")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch all schemas registered with the given schema name", response = SchemaList.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Versioned history of schemas registered under the group of specified schema type", response = SchemaList.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getObjectTypeSchemas(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Object type",required=true) @PathParam("objectTypeName") String objectTypeName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getObjectTypeSchemas(groupName,objectTypeName,securityContext);
    }
    @GET
    @Path("/{groupName}/objectTypes")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch all objectTypes under a Group. This api will return schema types.", response = ObjectTypesList.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "List of objectTypes under the group", response = ObjectTypesList.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getObjectTypes(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getObjectTypes(groupName,securityContext);
    }
    @PUT
    @Path("/{groupName}/encodings")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = EncodingId.class, tags={ "Encoding", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found Encoding", response = EncodingId.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group or encoding id with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getOrGenerateEncodingId(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Get schema corresponding to the version" ,required=true) GetEncodingIdRequest getEncodingIdRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getOrGenerateEncodingId(groupName,getEncodingIdRequest,securityContext);
    }
    @GET
    @Path("/{groupName}/schemas/versions/{versionId}")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaInfo.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Schema corresponding to the version", response = SchemaInfo.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getSchemaFromVersion(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Group name",required=true) @PathParam("versionId") String versionId
,@ApiParam(value = "Get schema corresponding to the version" ,required=true) GetSchemaFromVersionRequest getSchemaFromVersionRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getSchemaFromVersion(groupName,versionId,getSchemaFromVersionRequest,securityContext);
    }
    @GET
    @Path("/{groupName}/objectTypes/{objectTypeName}/schemas/versions/{versionId}")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaInfo.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Schema corresponding to the version", response = SchemaInfo.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getSchemaFromVersionForObjectType(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Object type",required=true) @PathParam("objectTypeName") String objectTypeName
,@ApiParam(value = "version id",required=true) @PathParam("versionId") Integer versionId
,@ApiParam(value = "Get schema corresponding to the version" ,required=true) GetSchemaForObjectTypeByVersionRequest getSchemaForObjectTypeByVersionRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getSchemaFromVersionForObjectType(groupName,objectTypeName,versionId,getSchemaForObjectTypeByVersionRequest,securityContext);
    }
    @GET
    @Path("/{groupName}/rules")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaValidationRules.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group schema validation rules", response = SchemaValidationRules.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getSchemaValidationRules(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getSchemaValidationRules(groupName,securityContext);
    }
    @POST
    @Path("/{groupName}/schemas/schema/{fingerprint}")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Get the version for the schema if it is registered.", response = VersionInfo.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Schema version", response = VersionInfo.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getSchemaVersion(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "schema fingerprint",required=true) @PathParam("fingerprint") Long fingerprint
,@ApiParam(value = "Get schema corresponding to the version" ,required=true) GetSchemaVersion getSchemaVersion
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getSchemaVersion(groupName,fingerprint,getSchemaVersion,securityContext);
    }
    @GET
    
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "List all groups", response = GroupsList.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "List of all groups", response = GroupsList.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching the list of Groups", response = Void.class) })
    public Response listGroups(@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.listGroups(securityContext);
    }
    @PUT
    @Path("/{groupName}/rules")
    @Consumes({ "application/json" })
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "update schema validation rules of an existing Group", response = Void.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Updated schema validation policy", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 409, message = "Conflict while attempting to update policy.", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response updateSchemaValidationRules(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "update group policy" ,required=true) UpdateValidationRulesPolicyRequest updateValidationRulesPolicyRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.updateSchemaValidationRules(groupName,updateValidationRulesPolicyRequest,securityContext);
    }
    @POST
    @Path("/{groupName}/schemas/validate")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "check if given schema is compatible with schemas in the registry for current policy setting.", response = Void.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Schema is valid", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response validate(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Checks if schema is valid with respect to supplied validation rules" ,required=true) ValidateRequest validateRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.validate(groupName,validateRequest,securityContext);
    }
}
