package io.pravega.schemaregistry.contract.generated.rest.server.api;

import io.pravega.schemaregistry.contract.generated.rest.server.api.*;
import io.pravega.schemaregistry.contract.generated.rest.model.*;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

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

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.validation.constraints.*;

public abstract class ApplicationsApiService {
    public abstract Response addReader(String appId,AddReaderRequest addReaderRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response addWriter(String appId,AddWriterRequest addWriterRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response createApplication(CreateApplicationRequest createApplicationRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response deleteReaderFromApp(String appId,String groupId,SecurityContext securityContext) throws NotFoundException;
    public abstract Response deleteWriterFromApp(String appId,String groupId,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getApplication(String appId,SecurityContext securityContext) throws NotFoundException;
    public abstract Response listReaders(String groupId,SecurityContext securityContext) throws NotFoundException;
    public abstract Response listWriters(String groupId,SecurityContext securityContext) throws NotFoundException;
}
