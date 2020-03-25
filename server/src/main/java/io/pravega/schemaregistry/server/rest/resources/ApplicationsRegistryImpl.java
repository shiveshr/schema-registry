/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest.resources;

import io.pravega.common.Exceptions;
import io.pravega.schemaregistry.contract.exceptions.IncompatibleSchemaException;
import io.pravega.schemaregistry.contract.generated.rest.model.AddReaderRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.AddWriterRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.Application;
import io.pravega.schemaregistry.contract.generated.rest.model.ApplicationsInGroup;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateApplicationRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupsList;
import io.pravega.schemaregistry.contract.generated.rest.model.IncompatibleSchema;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfoList;
import io.pravega.schemaregistry.contract.generated.rest.server.api.NotFoundException;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import io.pravega.schemaregistry.server.rest.v1.ApiV1;
import io.pravega.schemaregistry.service.ApplicationRegistryService;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.swagger.models.Model;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.util.Map;
import java.util.stream.Collectors;

/*
Implementation of Ping, which serves as a check for working REST server.
 */
@Slf4j
public class ApplicationsRegistryImpl implements ApiV1.ApplicationsApi {
    @Context
    HttpHeaders headers;

    private final ApplicationRegistryService applicationRegistryService;
    private final SchemaRegistryService schemaRegistryService;

    public ApplicationsRegistryImpl(ApplicationRegistryService applicationRegistryService, SchemaRegistryService schemaRegistryService) {
        this.applicationRegistryService = applicationRegistryService;
        this.schemaRegistryService = schemaRegistryService;
    }

    @Override
    public void addReader(String appId, AddReaderRequest addReaderRequest, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        applicationRegistryService.addReader(appId, addReaderRequest.getGroupId(), ModelHelper.decode(addReaderRequest.getVersion()),
                schemaRegistryService::getGroupProperties, x -> schemaRegistryService.getGroupEvolutionHistory(x, null))
                                  .thenApply(v -> {
                                      return Response.status(Response.Status.OK).build();
                                  })
                                  .exceptionally(exception -> {
                                      Throwable unwrap = Exceptions.unwrap(exception);
                                      if (unwrap instanceof IncompatibleSchemaException) {
                                          return Response.status(Response.Status.CONFLICT).entity(new IncompatibleSchema()
                                                  .errorMessage(unwrap.getMessage())).build();
                                      }
                                      log.warn("add application failed with exception: ", exception);
                                      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
                                  })
                                  .thenApply(response -> {
                                      asyncResponse.resume(response);
                                      return response;
                                  });
    }

    @Override
    public void addWriter(String appId, AddWriterRequest addWriterRequest, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        applicationRegistryService.addWriter(appId, addWriterRequest.getGroupId(), ModelHelper.decode(addWriterRequest.getVersion()),
                schemaRegistryService::getGroupProperties, x -> schemaRegistryService.getGroupEvolutionHistory(x, null))
                                  .thenApply(v -> {
                                      return Response.status(Response.Status.OK).build();
                                  })
                                  .exceptionally(exception -> {
                                      Throwable unwrap = Exceptions.unwrap(exception);
                                      if (unwrap instanceof IncompatibleSchemaException) {
                                          return Response.status(Response.Status.CONFLICT).entity(new IncompatibleSchema()
                                                  .errorMessage(unwrap.getMessage())).build();
                                      }
                                      log.warn("add application failed with exception: ", exception);
                                      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
                                  })
                                  .thenApply(response -> {
                                      asyncResponse.resume(response);
                                      return response;
                                  });
    }

    @Override
    public void createApplication(CreateApplicationRequest createApplicationRequest, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        applicationRegistryService.addApplication(createApplicationRequest.getApplicationName(), createApplicationRequest.getProperties())
                                  .thenApply(v -> {
                                      return Response.status(Response.Status.OK).build();
                                  })
                                  .exceptionally(exception -> {
                                      log.warn("add application failed with exception: ", exception);
                                      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
                                  })
                                  .thenApply(response -> {
                                      asyncResponse.resume(response);
                                      return response;
                                  });
    }

    @Override
    public void getApplication(String appId, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        applicationRegistryService.getApplication(appId, schemaRegistryService::getSchema)
                                  .thenApply(app -> {
                                      Application application = ModelHelper.encode(app);
                                      return Response.status(Response.Status.OK).entity(application).build();
                                  })
                                  .exceptionally(exception -> {
                                      log.warn("add application failed with exception: ", exception);
                                      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
                                  })
                                  .thenApply(response -> {
                                      asyncResponse.resume(response);
                                      return response;
                                  });
    }

    @Override
    public void listReaders(String groupId, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        applicationRegistryService.listReaderAppsInGroup(groupId)
                                  .thenApply(map -> {
                                      ApplicationsInGroup applicationsInGroup = new ApplicationsInGroup()
                                              .map(map.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, 
                                                      x -> new VersionInfoList().versions(x.getValue().stream().map(ModelHelper::encode)
                                                                                           .collect(Collectors.toList())))));
                                      return Response.status(Response.Status.OK).entity(applicationsInGroup).build();
                                  })
                                  .exceptionally(exception -> {
                                      log.warn("add application failed with exception: ", exception);
                                      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
                                  })
                                  .thenApply(response -> {
                                      asyncResponse.resume(response);
                                      return response;
                                  });
    }

    @Override
    public void listWriters(String groupId, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        applicationRegistryService.listWriterAppsInGroup(groupId)
                                  .thenApply(map -> {
                                      ApplicationsInGroup applicationsInGroup = new ApplicationsInGroup()
                                              .map(map.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                                                      x -> new VersionInfoList().versions(x.getValue().stream().map(ModelHelper::encode)
                                                                                           .collect(Collectors.toList())))));
                                      return Response.status(Response.Status.OK).entity(applicationsInGroup).build();
                                  })
                                  .exceptionally(exception -> {
                                      log.warn("add application failed with exception: ", exception);
                                      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
                                  })
                                  .thenApply(response -> {
                                      asyncResponse.resume(response);
                                      return response;
                                  });
    }
}
