package io.pravega.schemaregistry.server.rest.generated.api;

import io.pravega.schemaregistry.server.rest.generated.model.*;
import io.pravega.schemaregistry.server.rest.generated.api.ScopesApiService;
import io.pravega.schemaregistry.server.rest.generated.api.factories.ScopesApiServiceFactory;

import io.swagger.annotations.ApiParam;
import io.swagger.jaxrs.*;

import io.pravega.schemaregistry.server.rest.generated.model.AddSchemaRequest;
import io.pravega.schemaregistry.server.rest.generated.model.AddSchemaRequest1;
import io.pravega.schemaregistry.server.rest.generated.model.CanReadUsingSchemaRequest;
import io.pravega.schemaregistry.server.rest.generated.model.CheckCompatibilityRequest;
import io.pravega.schemaregistry.server.rest.generated.model.CompressionsList;
import io.pravega.schemaregistry.server.rest.generated.model.CreateGroupRequest;
import io.pravega.schemaregistry.server.rest.generated.model.EncodingInfo;
import io.pravega.schemaregistry.server.rest.generated.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.server.rest.generated.model.GetSchemaFromVersionRequest;
import io.pravega.schemaregistry.server.rest.generated.model.GroupProperty;
import io.pravega.schemaregistry.server.rest.generated.model.GroupsList;
import io.pravega.schemaregistry.server.rest.generated.model.SchemaInfo;
import io.pravega.schemaregistry.server.rest.generated.model.SchemaWithVersion;
import io.pravega.schemaregistry.server.rest.generated.model.SchemaWithVersionList;
import io.pravega.schemaregistry.server.rest.generated.model.UpdateCompatibilityPolicyRequest;
import io.pravega.schemaregistry.server.rest.generated.model.VersionInfo;

import java.util.Map;
import java.util.List;
import io.pravega.schemaregistry.server.rest.generated.api.NotFoundException;

import java.io.InputStream;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.servlet.ServletConfig;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.*;
import javax.validation.constraints.*;

@Path("/scopes")


@io.swagger.annotations.Api(description = "the scopes API")

public class ScopesApi  {
   private final ScopesApiService delegate;

   public ScopesApi(@Context ServletConfig servletContext) {
      ScopesApiService delegate = null;

      if (servletContext != null) {
         String implClass = servletContext.getInitParameter("ScopesApi.implementation");
         if (implClass != null && !"".equals(implClass.trim())) {
            try {
               delegate = (ScopesApiService) Class.forName(implClass).newInstance();
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         } 
      }

      if (delegate == null) {
         delegate = ScopesApiServiceFactory.getScopesApi();
      }

      this.delegate = delegate;
   }

    @POST
    @Path("/{scopeName}/Groups/{GroupName}/schemas")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "adds a new schema to the group", response = VersionInfo.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added schema to the group", response = VersionInfo.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a Group", response = Void.class) })
    public Response addSchemaToGroupIfAbsent(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
,@ApiParam(value = "Add new schema to group" ,required=true) AddSchemaRequest addSchemaRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.addSchemaToGroupIfAbsent(scopeName,groupName,addSchemaRequest,securityContext);
    }
    @POST
    @Path("/{scopeName}/Groups/{GroupName}/subgroups/{SubgroupName}/schemas")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "adds a new schema to the group", response = VersionInfo.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added schema to subgroup", response = VersionInfo.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a Group", response = Void.class) })
    public Response addSchemaToSubgroupIfAbsent(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
,@ApiParam(value = "Subgroup name",required=true) @PathParam("SubgroupName") String subgroupName
,@ApiParam(value = "Add new schema to group" ,required=true) AddSchemaRequest1 addSchemaRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.addSchemaToSubgroupIfAbsent(scopeName,groupName,subgroupName,addSchemaRequest,securityContext);
    }
    @GET
    @Path("/{scopeName}/Groups/{GroupName}/schemas/canRead")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "check if given schema is compatible with schemas in the registry for current policy setting.", response = Void.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Compatibility check", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response canRead(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
,@ApiParam(value = "Get schema is compatible with existing schemas for existing compatibility setting" ,required=true) CanReadUsingSchemaRequest canReadUsingSchemaRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.canRead(scopeName,groupName,canReadUsingSchemaRequest,securityContext);
    }
    @GET
    @Path("/{scopeName}/Groups/{GroupName}/schemas/compatibility")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "check if given schema is compatible with schemas in the registry for current policy setting.", response = Void.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Schema is compatible", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response checkCompatibility(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
,@ApiParam(value = "Get schema is compatible with existing schemas for existing compatibility setting" ,required=true) CheckCompatibilityRequest checkCompatibilityRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.checkCompatibility(scopeName,groupName,checkCompatibilityRequest,securityContext);
    }
    @POST
    @Path("/{scopeName}/groups")
    @Consumes({ "application/json" })
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "Create a new Group", response = Void.class, tags={ "Groups", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added group to the namespace", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 409, message = "Group with given name already exists", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a Group", response = Void.class) })
    public Response createGroup(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "The Group configuration" ,required=true) CreateGroupRequest createGroupRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.createGroup(scopeName,createGroupRequest,securityContext);
    }
    @DELETE
    @Path("/{scopeName}/Groups/{GroupName}")
    
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "Delete a Group", response = Void.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 204, message = "Successfully deleted the Group", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while deleting the Group", response = Void.class) })
    public Response deleteGroup(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.deleteGroup(scopeName,groupName,securityContext);
    }
    @GET
    @Path("/{scopeName}/Groups/{GroupName}/compressions")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = CompressionsList.class, tags={ "Encoding", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found Compressions", response = CompressionsList.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group or encoding id with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getCompressionsList(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getCompressionsList(scopeName,groupName,securityContext);
    }
    @GET
    @Path("/{scopeName}/Groups/{GroupName}/encodings/{EncodingId}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = EncodingInfo.class, tags={ "Encoding", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found Encoding", response = EncodingInfo.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group or encoding id with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getEncodingId(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
,@ApiParam(value = "Encoding id that identifies a unique combination of encoding and compression",required=true) @PathParam("EncodingId") String encodingId
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getEncodingId(scopeName,groupName,encodingId,securityContext);
    }
    @GET
    @Path("/{scopeName}/Groups/{GroupName}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = GroupProperty.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group properties", response = GroupProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getGroupProperties(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getGroupProperties(scopeName,groupName,securityContext);
    }
    @GET
    @Path("/{scopeName}/Groups/{GroupName}/schemas")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaWithVersionList.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Versioned history of schemas registered under the group", response = SchemaWithVersionList.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getGroupSchemas(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getGroupSchemas(scopeName,groupName,securityContext);
    }
    @GET
    @Path("/{scopeName}/Groups/{GroupName}/schemas/latest")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaWithVersion.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group properties", response = SchemaWithVersion.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getLatestGroupSchema(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getLatestGroupSchema(scopeName,groupName,securityContext);
    }
    @GET
    @Path("/{scopeName}/Groups/{GroupName}/subgroups/{SubgroupName}/schemas/latest")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaWithVersion.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found latest schema in subgroup", response = SchemaWithVersion.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getLatestSubgroupSchema(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
,@ApiParam(value = "Subgroup name",required=true) @PathParam("SubgroupName") String subgroupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getLatestSubgroupSchema(scopeName,groupName,subgroupName,securityContext);
    }
    @PUT
    @Path("/{scopeName}/Groups/{GroupName}/encodings")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = EncodingInfo.class, tags={ "Encoding", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found Encoding", response = EncodingInfo.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group or encoding id with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getOrGenerateEncodingId(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
,@ApiParam(value = "Get schema corresponding to the version" ,required=true) GetEncodingIdRequest getEncodingIdRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getOrGenerateEncodingId(scopeName,groupName,getEncodingIdRequest,securityContext);
    }
    @GET
    @Path("/{scopeName}/Groups/{GroupName}/subgroups/{SubgroupName}/schemas/version")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaInfo.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Schema corresponding to the version", response = SchemaInfo.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getSchemaFromSubgroupVersion(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
,@ApiParam(value = "Subgroup name",required=true) @PathParam("SubgroupName") String subgroupName
,@ApiParam(value = "Get schema corresponding to the version" ,required=true) GetSchemaFromVersionRequest getSchemaFromVersionRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getSchemaFromSubgroupVersion(scopeName,groupName,subgroupName,getSchemaFromVersionRequest,securityContext);
    }
    @GET
    @Path("/{scopeName}/Groups/{GroupName}/schemas/version")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaInfo.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Schema corresponding to the version", response = SchemaInfo.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getSchemaFromVersion(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
,@ApiParam(value = "Get schema corresponding to the version" ,required=true) GetSchemaFromVersionRequest getSchemaFromVersionRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getSchemaFromVersion(scopeName,groupName,getSchemaFromVersionRequest,securityContext);
    }
    @GET
    @Path("/{scopeName}/Groups/{GroupName}/subgroups/{SubgroupName}/schemas")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch all schemas registered under a sub Group", response = SchemaWithVersionList.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Versioned history of schemas registered under the group", response = SchemaWithVersionList.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getSubGroupSchemas(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
,@ApiParam(value = "Subgroup name",required=true) @PathParam("SubgroupName") String subgroupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getSubGroupSchemas(scopeName,groupName,subgroupName,securityContext);
    }
    @GET
    @Path("/{scopeName}/groups")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "List groups within the given scope", response = GroupsList.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "List of all groups under the given scope namespace", response = GroupsList.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching the list of Groups for the given scope", response = Void.class) })
    public Response listGroups(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "") @QueryParam("") String ERROR_UNKNOWN
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.listGroups(scopeName,ERROR_UNKNOWN,securityContext);
    }
    @PUT
    @Path("/{scopeName}/Groups/{GroupName}")
    @Consumes({ "application/json" })
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "update compatibility policy of an existing Group", response = Void.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Updated compatibility policy", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 409, message = "Conflict while attempting to update policy.", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response updateCompatibilityPolicy(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
,@ApiParam(value = "update group policy" ,required=true) UpdateCompatibilityPolicyRequest updateCompatibilityPolicyRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.updateCompatibilityPolicy(scopeName,groupName,updateCompatibilityPolicyRequest,securityContext);
    }
}
