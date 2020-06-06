/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest.resources;

import com.google.common.base.Preconditions;
import io.pravega.auth.AuthException;
import io.pravega.auth.AuthHandler;
import io.pravega.auth.AuthenticationException;
import io.pravega.auth.AuthorizationException;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.generated.rest.model.AddedTo;
import io.pravega.schemaregistry.contract.generated.rest.model.CanRead;
import io.pravega.schemaregistry.contract.generated.rest.model.CodecTypesList;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingId;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupHistory;
import io.pravega.schemaregistry.contract.generated.rest.model.ListGroupsResponse;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaVersionsList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.UpdateValidationRulesRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.Valid;
import io.pravega.schemaregistry.contract.generated.rest.model.ValidateRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo;
import io.pravega.schemaregistry.contract.generated.rest.server.api.NotFoundException;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import io.pravega.schemaregistry.contract.v1.ApiV1;
import io.pravega.schemaregistry.exceptions.CodecTypeNotRegisteredException;
import io.pravega.schemaregistry.exceptions.IncompatibleSchemaException;
import io.pravega.schemaregistry.exceptions.PreconditionFailedException;
import io.pravega.schemaregistry.exceptions.SerializationFormatMismatchException;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.server.rest.auth.AuthHandlerManager;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.schemaregistry.storage.ContinuationToken;
import io.pravega.schemaregistry.storage.StoreExceptions;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.pravega.auth.AuthHandler.Permissions.READ;
import static io.pravega.auth.AuthHandler.Permissions.READ_UPDATE;
import static javax.ws.rs.core.Response.Status;

/**
 * Schema Registry Resource implementation.
 */
@Slf4j
public class SchemaRegistryResourceImpl implements ApiV1.GroupsApiAsync, ApiV1.SchemasApiAsync {
    private static final int DEFAULT_LIST_GROUPS_LIMIT = 100;

    // region auth resources
    private static class AuthResources {
        private static final String ROOT = "/";
        private static final String GROUP_FORMAT = ROOT + "%s";
        private static final String GROUP_SCHEMA_FORMAT = GROUP_FORMAT + "/schemas";
        private static final String GROUP_CODEC_FORMAT = GROUP_FORMAT + "/codecs";
    }
    // endregion
    
    @Context
    HttpHeaders headers;

    private final AuthHandlerManager authManager;

    private SchemaRegistryService registryService;
    private ServiceConfig config;
    
    public SchemaRegistryResourceImpl(SchemaRegistryService registryService, ServiceConfig config) {
        this.registryService = registryService;
        this.config = config;
        this.authManager = new AuthHandlerManager(config);
    }

    @Override
    public void listGroups(String continuationToken, Integer limit, 
                           AsyncResponse asyncResponse) throws NotFoundException {
        log.info("List Groups called");
        int limitUnboxed = limit == null ? DEFAULT_LIST_GROUPS_LIMIT : limit;

        withCompletion("listGroups", READ, AuthResources.ROOT, asyncResponse,
                () -> registryService.listGroups(ContinuationToken.fromString(continuationToken), limitUnboxed)
                                     .thenApply(result -> {
                                         ListGroupsResponse groupsList = new ListGroupsResponse();
                                         result.getMap().forEach((x, y) -> {
                                             if (y != null) {
                                                 try {
                                                     authenticateAuthorize(getAuthorizationHeader(), 
                                                             String.format(AuthResources.GROUP_FORMAT, x), READ);
                                                     groupsList.putGroupsItem(x, ModelHelper.encode(y));
                                                 } catch (AuthException e) {
                                                    // skip groups the user is not authorized on.                                                      
                                                 }
                                             }
                                         });
                                         groupsList.continuationToken(result.getToken() == null ? null : result.getToken().toString());
                                         return Response.status(Status.OK).entity(groupsList).build();
                                     })
                                     .exceptionally(exception -> {
                                         log.warn("listGroups failed with exception: ", exception);
                                         return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                     }))                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }

    @Override
    public void createGroup(CreateGroupRequest createGroupRequest, 
                            AsyncResponse asyncResponse) throws NotFoundException {
        withCompletion("createGroup", READ_UPDATE, AuthResources.ROOT, asyncResponse, () -> {
            GroupProperties groupProperties = ModelHelper.decode(createGroupRequest.getGroupProperties());
            String groupName = createGroupRequest.getGroupName();
            return registryService.createGroup(groupName, groupProperties)
                                  .thenApply(createStatus -> {
                                      if (!createStatus) {
                                          log.info("group {} exists", groupName);
                                          return Response.status(Status.CONFLICT).build();
                                      }
                                      log.info("group {} created", groupName);
                                      return Response.status(Status.CREATED).build();
                                  })
                                  .exceptionally(exception -> {
                                      log.warn("createGroup failed with exception: ", exception);
                                      return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                  });
        }).thenApply(response -> {
            asyncResponse.resume(response);
            return response;
        });
    }

    @Override
    public void getGroupProperties(String groupName, 
                                   AsyncResponse asyncResponse) throws NotFoundException {
        withCompletion("getGroupProperties", READ, String.format(AuthResources.GROUP_FORMAT, groupName), asyncResponse,
                () -> registryService.getGroupProperties(groupName)
                                     .thenApply(groupProperty -> {
                                         log.info("Group {} property found are {}", groupName, groupProperty);
                                         return Response.status(Status.OK).entity(ModelHelper.encode(groupProperty)).build();
                                     })
                                     .exceptionally(exception -> {
                                         if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                             log.warn("Group {} not found", groupName);
                                             return Response.status(Status.NOT_FOUND).build();
                                         }
                                         log.warn("getGroupProperties for group {} failed with exception: {}", groupName, exception);
                                         return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                     }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }

    @Override
    public void getGroupHistory(String groupName,  AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Get group history called for group {}", groupName);
        withCompletion("getGroupHistory", READ, String.format(AuthResources.GROUP_FORMAT, groupName), asyncResponse,
                () -> registryService.getGroupHistory(groupName, null)
                                     .thenApply(history -> {
                                         GroupHistory list = new GroupHistory()
                                                 .history(history.stream().map(ModelHelper::encode)
                                                                 .collect(Collectors.toList()));
                                         log.info("getGroupHistory: {} schemas found for group {}", list.getHistory().size(), groupName);
                                         return Response.status(Status.OK).entity(list).build();
                                     })
                                     .exceptionally(exception -> {
                                         if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                             log.warn("Group {} not found", groupName);
                                             return Response.status(Status.NOT_FOUND).build();
                                         }

                                         log.warn("getGroupHistory failed with exception: ", exception);
                                         return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                     }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });

    }

    @Override
    public void updateSchemaValidationRules(String groupName, UpdateValidationRulesRequest updateValidationRulesRequest,  AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Update schema validation rules called for group {} with new request {}", groupName, updateValidationRulesRequest);
        withCompletion("updateSchemaValidationRules", READ_UPDATE, String.format(AuthResources.GROUP_FORMAT, groupName), asyncResponse,
                () -> {
                    SchemaValidationRules rules = ModelHelper.decode(updateValidationRulesRequest.getValidationRules());
                    SchemaValidationRules previousRules = updateValidationRulesRequest.getPreviousRules() == null ?
                            null : ModelHelper.decode(updateValidationRulesRequest.getPreviousRules());
                    return registryService.updateSchemaValidationRules(groupName, rules, previousRules)
                                          .thenApply(groupProperty -> Response.status(Status.OK).build())
                                          .exceptionally(exception -> {
                                              if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                                  log.warn("Group {} not found", groupName);
                                                  return Response.status(Status.NOT_FOUND).build();
                                              } else if (Exceptions.unwrap(exception) instanceof PreconditionFailedException) {
                                                  log.warn("updateSchemaValidationRules write conflict {}", groupName);
                                                  return Response.status(Status.CONFLICT).build();
                                              } else {
                                                  log.warn("updateSchemaValidationRules failed with exception: ", exception);
                                                  return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                              }
                                          });
                }).thenApply(response -> {
            asyncResponse.resume(response);
            return response;
        });
    }
    
    @Override
    public void deleteGroup(String groupName, 
                            AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Delete group called for group {}", groupName);
        withCompletion("deleteGroup", READ_UPDATE, AuthResources.ROOT, asyncResponse,
                () -> registryService.deleteGroup(groupName)
                                     .thenApply(status -> {
                                         log.info("Group {} deleted", groupName);
                                         return Response.status(Status.NO_CONTENT).build();
                                     })
                                     .exceptionally(exception -> {
                                         log.warn("deleteGroup failed with exception: ", exception);
                                         return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                     }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }

    @Override
    public void getSchemaVersions(String groupName, String type,  AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Get group schemas called for group {}", groupName);
        withCompletion("getSchemaVersions", READ, String.format(AuthResources.GROUP_FORMAT, groupName), asyncResponse,
                () -> registryService.getGroupHistory(groupName, null)
                                     .thenApply(history -> {
                                         SchemaVersionsList list = new SchemaVersionsList()
                                                 .schemas(history.stream().map(x -> new SchemaWithVersion()
                                                         .schemaInfo(ModelHelper.encode(x.getSchema()))
                                                         .version(ModelHelper.encode(x.getVersion())))
                                                                 .collect(Collectors.toList()));
                                         log.info("getSchemaVersions: {} schemas found for group {}", list.getSchemas().size(), groupName);
                                         return Response.status(Status.OK).entity(list).build();
                                     })
                                     .exceptionally(exception -> {
                                         if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                             log.warn("Group {} not found", groupName);
                                             return Response.status(Status.NOT_FOUND).build();
                                         }

                                         log.warn("getSchemaVersions failed with exception: ", exception);
                                         return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                     }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }

    @Override
    public void addSchema(String groupName, SchemaInfo schemaInfo,
                                          AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Add schema to group called for group {}", groupName);
        withCompletion("addSchema", READ_UPDATE, String.format(AuthResources.GROUP_SCHEMA_FORMAT, groupName), asyncResponse,
                () -> {
                    return registryService.addSchema(groupName, ModelHelper.decode(schemaInfo))
                                          .thenApply(versionInfo -> {
                                              VersionInfo version = ModelHelper.encode(versionInfo);
                                              log.info("schema added to group {} with new version {}", groupName, versionInfo);
                                              return Response.status(Status.CREATED).entity(version).build();
                                          })
                                          .exceptionally(exception -> {
                                              Throwable unwrap = Exceptions.unwrap(exception);
                                              if (unwrap instanceof StoreExceptions.DataNotFoundException) {
                                                  log.warn("Group {} not found", groupName);
                                                  return Response.status(Status.NOT_FOUND).build();
                                              } else if (unwrap instanceof IncompatibleSchemaException) {
                                                  log.info("addSchema incompatible schema {}", groupName);
                                                  return Response.status(Status.CONFLICT).build();
                                              } else if (unwrap instanceof SerializationFormatMismatchException) {
                                                  log.info("addSchema serialization format mismatched {}", groupName);
                                                  return Response.status(Status.EXPECTATION_FAILED).build();
                                              } else {
                                                  log.warn("addSchema failed with exception: ", unwrap);
                                                  return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                              }
                                          });
                }).thenApply(response -> {
            asyncResponse.resume(response);
            return response;
        });
    }

    @Override
    public void validate(String groupName, ValidateRequest validateRequest,  AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Validate schema called for group {}", groupName);

        withCompletion("validate", READ, String.format(AuthResources.GROUP_FORMAT, groupName), asyncResponse,
                () -> {
                    return registryService.validateSchema(groupName, ModelHelper.decode(validateRequest.getSchemaInfo()))
                                          .thenApply(compatible -> {
                                              log.info("Schema is valid for group {}", groupName);
                                              return Response.status(Status.OK).entity(new Valid().valid(compatible)).build();
                                          })
                                          .exceptionally(exception -> {
                                              if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                                  log.warn("Group {} not found", groupName);
                                                  return Response.status(Status.NOT_FOUND).build();
                                              }
                                              log.warn("validate failed with exception: ", exception);
                                              return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                          });
                }).thenApply(response -> {
            asyncResponse.resume(response);
            return response;
        });
    }

    @Override
    public void canRead(String groupName, SchemaInfo schemaInfo,  AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Can read using schema called for group {}", groupName);

        withCompletion("canRead", READ, String.format(AuthResources.GROUP_FORMAT, groupName), asyncResponse,
                () -> {
                    return registryService.canRead(groupName, ModelHelper.decode(schemaInfo))
                                          .thenApply(canRead -> {
                                              log.info("For group {}, can read using schema response = {}", groupName, canRead);
                                              return Response.status(Status.OK).entity(new CanRead().compatible(canRead)).build();
                                          })
                                          .exceptionally(exception -> {
                                              if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                                  log.warn("Group {} not found", groupName);
                                                  return Response.status(Status.NOT_FOUND).build();
                                              }
                                              log.warn("can read failed with exception: ", exception);
                                              return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                          });
                }).thenApply(response -> {
            asyncResponse.resume(response);
            return response;
        });
    }

    @Override
    public void getSchemaFromVersionOrdinal(String groupName, Integer versionOrdinal, AsyncResponse asyncResponse) {
        log.info("Get schema from version {} called for group {}", versionOrdinal, groupName);
        withCompletion("getSchemaFromVersionOrdinal", READ, String.format(AuthResources.GROUP_FORMAT, groupName), asyncResponse,
                () -> registryService.getSchema(groupName, versionOrdinal)
                                     .thenApply(schemaWithVersion -> {
                                         SchemaInfo schema = ModelHelper.encode(schemaWithVersion);
                                         log.info("Schema for version {} for group {} found.", versionOrdinal, groupName);
                                         return Response.status(Status.OK).entity(schema).build();
                                     })
                                     .exceptionally(exception -> {
                                         if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                             log.warn("Group {} or version {} not found", groupName, versionOrdinal);
                                             return Response.status(Status.NOT_FOUND).build();
                                         }
                                         log.warn("getSchemaFromVersionOrdinal failed with exception: ", exception);
                                         return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                     }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }

    @Override
    public void getSchemaFromVersion(String groupName, String schemaType, Integer version, AsyncResponse asyncResponse) {
        log.info("Get schema from version {} called for group {}", version, groupName);
        withCompletion("getSchemaFromVersion", READ, String.format(AuthResources.GROUP_FORMAT, groupName), asyncResponse,
                () -> registryService.getSchema(groupName, schemaType, version)
                                                                    .thenApply(schemaWithVersion -> {
                                                                        SchemaInfo schema = ModelHelper.encode(schemaWithVersion);
                                                                        log.info("Schema for version {} for group {} found.", version, groupName);
                                                                        return Response.status(Status.OK).entity(schema).build();
                                                                    })
                                                                    .exceptionally(exception -> {
                                                                        if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                                                            log.warn("Group {} or version {} not found", groupName, version);
                                                                            return Response.status(Status.NOT_FOUND).build();
                                                                        }
                                                                        log.warn("getSchemaFromVersionOrdinal failed with exception: ", exception);
                                                                        return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                                                    }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }

    @Override
    public void deleteSchemaFromVersionOrdinal(String groupName, Integer versionOrdinal,
                                    AsyncResponse asyncResponse) {
        log.info("Delete schema from version {} called for group {}", versionOrdinal, groupName);
        withCompletion("deleteSchemaFromVersionOrdinal", READ_UPDATE, String.format(AuthResources.GROUP_FORMAT, groupName), asyncResponse,
                () -> registryService.deleteSchema(groupName, versionOrdinal)
                                     .thenApply(v -> {
                                         log.info("Schema for version {} for group {} deleted.", versionOrdinal, groupName);
                                         return Response.status(Status.NO_CONTENT).build();
                                     })
                                     .exceptionally(exception -> {
                                         if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                             log.warn("Group {} or version {} not found", groupName, versionOrdinal);
                                             return Response.status(Status.NOT_FOUND).build();
                                         }
                                         log.warn("deleteSchemaFromVersionOrdinal failed with exception: ", exception);
                                         return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                     }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }

    @Override
    public void deleteSchemaVersion(String groupName, String schemaType, Integer version,
                                      AsyncResponse asyncResponse) {
        log.info("Delete schema from version {}/{} called for group {}", schemaType, version, groupName);
        withCompletion("deleteSchemaVersion", READ_UPDATE, String.format(AuthResources.GROUP_FORMAT, groupName), asyncResponse,
                () -> registryService.deleteSchema(groupName, schemaType, version)
                                     .thenApply(v -> {
                                         log.info("Schema for version {}/{} for group {} deleted.", schemaType, version, groupName);
                                         return Response.status(Status.NO_CONTENT).build();
                                     })
                                     .exceptionally(exception -> {
                                         if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                             log.warn("Group {} or version {}/{} not found", groupName, schemaType, version);
                                             return Response.status(Status.NOT_FOUND).build();
                                         }
                                         log.warn("deleteSchemaFromVersionOrdinal failed with exception: ", exception);
                                         return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                     }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }

    @Override
    public void getEncodingId(String groupName, GetEncodingIdRequest getEncodingIdRequest,
                               AsyncResponse asyncResponse) throws NotFoundException {
        log.info("getEncodingId called for group {} with version {} and codec {}", groupName,
                getEncodingIdRequest.getVersionInfo(), getEncodingIdRequest.getCodecType());
        withCompletion("getEncodingId", READ, String.format(AuthResources.GROUP_FORMAT, groupName), asyncResponse,
                () -> {
                    io.pravega.schemaregistry.contract.data.VersionInfo version = ModelHelper.decode(getEncodingIdRequest.getVersionInfo());
                    String codecType = getEncodingIdRequest.getCodecType();
                    return registryService.getEncodingId(groupName, version, codecType)
                                          .thenApply(encodingId -> {
                                              EncodingId id = ModelHelper.encode(encodingId);
                                              log.info("For group {} with version {} and codec {}, returning encoding id {}", groupName,
                                                      getEncodingIdRequest.getVersionInfo(), getEncodingIdRequest.getCodecType(), id);
                                              return Response.status(Status.OK).entity(id).build();
                                          })
                                          .exceptionally(exception -> {
                                              if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                                  log.warn("Group {} not found", groupName);
                                                  return Response.status(Status.NOT_FOUND).build();
                                              } else if (Exceptions.unwrap(exception) instanceof CodecTypeNotRegisteredException) {
                                                  log.info("getEncodingId failed Codec Not Found {}", groupName);
                                                  return Response.status(Status.PRECONDITION_FAILED).build();
                                              } else {
                                                  log.warn("getEncodingId failed with exception: ", exception);
                                                  return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                              }
                                          });
                }).thenApply(response -> {
            asyncResponse.resume(response);
            return response;
        });
    }

    @Override
    public void getSchemaVersion(String groupName, SchemaInfo schemaInfo,  AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Get schema version called for group {}", groupName);
        withCompletion("getSchemaVersion", READ, String.format(AuthResources.GROUP_FORMAT, groupName), asyncResponse,
                () -> {
                    return registryService.getSchemaVersion(groupName, ModelHelper.decode(schemaInfo))
                                          .thenApply(version -> {
                                              VersionInfo versionInfo = ModelHelper.encode(version);
                                              log.info("schema version {} found for group {}", versionInfo, groupName);
                                              return Response.status(Status.OK).entity(versionInfo).build();
                                          })
                                          .exceptionally(exception -> {
                                              if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                                  log.warn("Group {} or schema not found", groupName);
                                                  return Response.status(Status.NOT_FOUND).build();
                                              }

                                              log.warn("getSchemaVersion failed with exception: ", exception);
                                              return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                          });
                }).thenApply(response -> {
            asyncResponse.resume(response);
            return response;
        });
    }
    
    @Override
    public void getSchemas(String groupName, String type,  AsyncResponse asyncResponse) throws NotFoundException {
        log.info("getSchemas called for group {} ", groupName);
        withCompletion("getSchemas", READ, String.format(AuthResources.GROUP_FORMAT, groupName), asyncResponse,
                () -> registryService.getSchemas(groupName, type)
                                                          .thenApply(schemas -> {
                                                              SchemaVersionsList schemaList = new SchemaVersionsList()
                                                                      .schemas(schemas.stream().map(ModelHelper::encode).collect(Collectors.toList()));
                                                              List<String> names = schemaList.getSchemas().stream().map(x -> x.getSchemaInfo().getType()).collect(Collectors.toList());
                                                              log.info("Found schemas {} for group {} ", names, groupName);
                                                              return Response.status(Status.OK).entity(schemaList).build();
                                                          })
                                                          .exceptionally(exception -> {
                                                              if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                                                  log.warn("Group {} not found", groupName);
                                                                  return Response.status(Status.NOT_FOUND).build();
                                                              }
                                                              log.warn("getSchemas failed with exception: ", exception);
                                                              return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                                          }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }

    @Override
    public void getEncodingInfo(String groupName, Integer encodingId,  AsyncResponse asyncResponse) throws NotFoundException {
        log.info("getEncodingInfo called for group {} encodingId {}", groupName, encodingId);
        withCompletion("getEncodingInfo", READ, String.format(AuthResources.GROUP_FORMAT, groupName), asyncResponse,
                () -> {
                    io.pravega.schemaregistry.contract.data.EncodingId id = new io.pravega.schemaregistry.contract.data.EncodingId(encodingId);
                    return registryService.getEncodingInfo(groupName, id)
                                          .thenApply(encodingInfo -> {
                                              EncodingInfo encoding = ModelHelper.encode(encodingInfo);
                                              log.info("group {} encoding id {} encodingInfo {}", groupName, encodingId, encoding);
                                              return Response.status(Status.OK).entity(encoding).build();
                                          })
                                          .exceptionally(exception -> {
                                              if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                                  log.warn("Group {} not found", groupName);
                                                  return Response.status(Status.NOT_FOUND).build();
                                              }
                                              log.warn("getEncodingInfo failed with exception: ", exception);
                                              return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                          });
                }).thenApply(response -> {
            asyncResponse.resume(response);
            return response;
        });
    }


    @Override
    public void getCodecTypesList(String groupName, 
                              AsyncResponse asyncResponse) throws NotFoundException {
        log.info("getCodecTypesList called for group {} ", groupName);
        withCompletion("getCodecTypesList", READ, String.format(AuthResources.GROUP_FORMAT, groupName), asyncResponse,
                () -> registryService.getCodecTypes(groupName)
                                     .thenApply(list -> {
                                         CodecTypesList codecsList = new CodecTypesList().codecTypes(list);
                                         log.info("group {}, codecTypes {} ", groupName, codecsList);
                                         return Response.status(Status.OK).entity(codecsList).build();
                                     })
                                     .exceptionally(exception -> {
                                         if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                             log.warn("Group {} not found", groupName);
                                             return Response.status(Status.NOT_FOUND).build();
                                         }
                                         log.warn("getCodecTypesList failed with exception: ", exception);
                                         return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                     }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }

    @Override
    public void addCodecType(String groupName, String codecType,  AsyncResponse asyncResponse) throws NotFoundException {
        log.info("addCodecType called for group {} codecType {}", groupName, codecType);
        withCompletion("addCodecType", READ, String.format(AuthResources.GROUP_CODEC_FORMAT, groupName), asyncResponse,
                () -> registryService.addCodecType(groupName, codecType)
                                     .thenApply(v -> {
                                         log.info("codecType {} added to group {}", codecType, groupName);
                                         return Response.status(Status.CREATED).build();
                                     })
                                     .exceptionally(exception -> {
                                         if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                             log.warn("Group {} not found", groupName);
                                             return Response.status(Status.NOT_FOUND).build();
                                         }
                                         log.warn("addCodecType failed with exception: ", exception);
                                         return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                     }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }

    @Override
    public void getSchemaReferences(SchemaInfo schemaInfo, AsyncResponse asyncResponse) {
        withCompletion("getSchemaReferences", READ, AuthResources.ROOT, asyncResponse,
                () -> registryService.getSchemaReferences(ModelHelper.decode(schemaInfo))
                                                                   .thenApply(map -> {
                                                                       AddedTo addedTo = new AddedTo()
                                                                               .groups(map.entrySet().stream().collect(
                                                                                       Collectors.toMap(Map.Entry::getKey,
                                                                                               x -> ModelHelper.encode(x.getValue()))));
                                                                       log.info("getSchemaReferences {} ", map.keySet());
                                                                       return Response.status(Status.OK).entity(addedTo).build();
                                                                   })
                                                                   .exceptionally(exception -> {
                                                                       if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                                                           log.warn("Schema {} not found", schemaInfo.getType());
                                                                           return Response.status(Status.NOT_FOUND).build();
                                                                       }
                                                                       log.warn("getCodecTypesList failed with exception: ", exception);
                                                                       return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                                                   }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }
    
    /**
     * This is a shortcut for {@code headers.getRequestHeader().get(HttpHeaders.AUTHORIZATION)}.
     *
     * @return a list of read-only values of the HTTP Authorization header
     * @throws IllegalStateException if called outside the scope of the HTTP request
     */
    private List<String> getAuthorizationHeader() {
        return headers.getRequestHeader(HttpHeaders.AUTHORIZATION);
    }

    private CompletableFuture<Response> withCompletion(String request, AuthHandler.Permissions permissions,
                                                       String resource, AsyncResponse response,
                                                       Supplier<CompletableFuture<Response>> future) {
        try {
            authenticateAuthorize(getAuthorizationHeader(), resource, permissions);
            return future.get();
        } catch (AuthException e) {
            log.warn("Auth failed {}", request, e);
            response.resume(Response.status(Status.fromStatusCode(e.getResponseCode())).build());
            throw e;
        } catch (IllegalArgumentException e) {
            log.warn("Bad request {}", request);
            return CompletableFuture.completedFuture(Response.status(Status.BAD_REQUEST).build());
        } catch (Exception e) {
            log.error("request failed with exception {}", e);
            return Futures.failedFuture(e);
        }
    }

    private void authenticateAuthorize(List<String> authHeader, String resource, AuthHandler.Permissions permission)
            throws AuthException {
        if (config.isAuthEnabled()) {
            String credentials = parseCredentials(authHeader);
            if (!authManager.authenticateAndAuthorize(resource, credentials, permission)) {
                throw new AuthorizationException(
                        String.format("Failed to authorize for resource [%s]", resource),
                        Response.Status.FORBIDDEN.getStatusCode());
            }
        }
    }

    private String parseCredentials(List<String> authHeader) throws AuthenticationException {
        if (authHeader == null || authHeader.isEmpty()) {
            throw new AuthenticationException("Missing authorization header.");
        }

        // Expecting a single value here. If there are multiple, we'll deal with just the first one.
        String credentials = authHeader.get(0);
        Preconditions.checkNotNull(credentials, "Missing credentials.");
        return credentials;
    }

}
