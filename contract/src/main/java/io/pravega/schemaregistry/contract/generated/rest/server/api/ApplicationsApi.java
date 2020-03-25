package io.pravega.schemaregistry.contract.generated.rest.server.api;

import io.pravega.schemaregistry.contract.generated.rest.model.*;
import io.pravega.schemaregistry.contract.generated.rest.server.api.ApplicationsApiService;
import io.pravega.schemaregistry.contract.generated.rest.server.api.factories.ApplicationsApiServiceFactory;

import io.swagger.annotations.ApiParam;
import io.swagger.jaxrs.*;

import io.pravega.schemaregistry.contract.generated.rest.model.AddReaderRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.AddWriterRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.Application;
import io.pravega.schemaregistry.contract.generated.rest.model.ApplicationsInGroup;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateApplicationRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.IncompatibleSchema;

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

@Path("/applications")


@io.swagger.annotations.Api(description = "the applications API")

public class ApplicationsApi  {
   private final ApplicationsApiService delegate;

   public ApplicationsApi(@Context ServletConfig servletContext) {
      ApplicationsApiService delegate = null;

      if (servletContext != null) {
         String implClass = servletContext.getInitParameter("ApplicationsApi.implementation");
         if (implClass != null && !"".equals(implClass.trim())) {
            try {
               delegate = (ApplicationsApiService) Class.forName(implClass).newInstance();
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         } 
      }

      if (delegate == null) {
         delegate = ApplicationsApiServiceFactory.getApplicationsApi();
      }

      this.delegate = delegate;
   }

    @POST
    @Path("/{appId}/reader")
    @Consumes({ "application/json" })
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "Add new reader to application", response = Void.class, tags={ "Application", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added reader", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 409, message = "Schema incompatible", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while adding reader to group", response = Void.class) })
    public Response addReader(@ApiParam(value = "appId",required=true) @PathParam("appId") String appId
,@ApiParam(value = "schema version" ,required=true) AddReaderRequest addReaderRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.addReader(appId,addReaderRequest,securityContext);
    }
    @POST
    @Path("/{appId}/writer")
    @Consumes({ "application/json" })
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "Add new writer to application", response = Void.class, tags={ "Application", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added writer", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 409, message = "Schema incompatible", response = IncompatibleSchema.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while adding writer to group", response = Void.class) })
    public Response addWriter(@ApiParam(value = "appId",required=true) @PathParam("appId") String appId
,@ApiParam(value = "schema version" ,required=true) AddWriterRequest addWriterRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.addWriter(appId,addWriterRequest,securityContext);
    }
    @POST
    
    @Consumes({ "application/json" })
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "Create a new Application", response = Void.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added group", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 409, message = "Group with given name already exists", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a Group", response = Void.class) })
    public Response createApplication(@ApiParam(value = "The Group configuration" ,required=true) CreateApplicationRequest createApplicationRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.createApplication(createApplicationRequest,securityContext);
    }
    @DELETE
    @Path("/{appId}/groups/{groupId}/reader")
    
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "Delete a reader from an app", response = Void.class, tags={ "Application", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 204, message = "Successfully deleted reader", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "App not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while deleting the reader app", response = Void.class) })
    public Response deleteReaderFromApp(@ApiParam(value = "appId",required=true) @PathParam("appId") String appId
,@ApiParam(value = "groupId",required=true) @PathParam("groupId") String groupId
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.deleteReaderFromApp(appId,groupId,securityContext);
    }
    @DELETE
    @Path("/{appId}/groups/{groupId}/writer")
    
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "Delete a writer from an app", response = Void.class, tags={ "Application", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 204, message = "Successfully deleted writer", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "App not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while deleting the writer app", response = Void.class) })
    public Response deleteWriterFromApp(@ApiParam(value = "appId",required=true) @PathParam("appId") String appId
,@ApiParam(value = "groupId",required=true) @PathParam("groupId") String groupId
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.deleteWriterFromApp(appId,groupId,securityContext);
    }
    @GET
    @Path("/{appId}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the application", response = Application.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group properties", response = Application.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getApplication(@ApiParam(value = "appId",required=true) @PathParam("appId") String appId
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getApplication(appId,securityContext);
    }
    @GET
    @Path("/groups/{groupId}/readers")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "List all writer applications in group", response = ApplicationsInGroup.class, tags={ "Application", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "List of all writers in group", response = ApplicationsInGroup.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching the list of writers in Groups", response = Void.class) })
    public Response listReaders(@ApiParam(value = "groupId",required=true) @PathParam("groupId") String groupId
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.listReaders(groupId,securityContext);
    }
    @GET
    @Path("/groups/{groupId}/writers")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "List all writer applications in group", response = ApplicationsInGroup.class, tags={ "Application", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "List of all writers in group", response = ApplicationsInGroup.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching the list of writers in Groups", response = Void.class) })
    public Response listWriters(@ApiParam(value = "groupId",required=true) @PathParam("groupId") String groupId
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.listWriters(groupId,securityContext);
    }
}
