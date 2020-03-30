package io.pravega.schemaregistry.contract.generated.rest.server.api.impl;

import io.pravega.schemaregistry.contract.generated.rest.server.api.*;
import io.pravega.schemaregistry.contract.generated.rest.model.*;

import io.pravega.schemaregistry.contract.generated.rest.model.AddReaderRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.AddWriterRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.Application;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateApplicationRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.IncompatibleSchema;
import io.pravega.schemaregistry.contract.generated.rest.model.ReadersInGroup;
import io.pravega.schemaregistry.contract.generated.rest.model.WritersInGroup;

import java.util.List;
import io.pravega.schemaregistry.contract.generated.rest.server.api.NotFoundException;

import java.io.InputStream;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.validation.constraints.*;

public class ApplicationsApiServiceImpl extends ApplicationsApiService {
    @Override
    public Response addReader(String appId, AddReaderRequest addReaderRequest, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
    }
    @Override
    public Response addWriter(String appId, AddWriterRequest addWriterRequest, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
    }
    @Override
    public Response createApplication(CreateApplicationRequest createApplicationRequest, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
    }
    @Override
    public Response deleteReaderFromApp(String appId, String groupId, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
    }
    @Override
    public Response deleteWriterFromApp(String appId, String groupId, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
    }
    @Override
    public Response getApplication(String appId, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
    }
    @Override
    public Response listReaders(String groupId, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
    }
    @Override
    public Response listWriters(String groupId, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
    }
}
