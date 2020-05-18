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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.controller.server.rpc.auth.StrongPasswordProcessor;
import io.pravega.schemaregistry.MapWithToken;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.generated.rest.model.ListGroupsResponse;
import io.pravega.schemaregistry.server.rest.RegistryApplication;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.After;
import org.junit.Test;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class SchemaRegistryAuthTest extends JerseyTest {
    private SchemaRegistryService service;
    private File authFile;

    @Override
    protected Application configure() {
        service = mock(SchemaRegistryService.class);
        final Set<Object> resourceObjs = new HashSet<>();
        ServiceConfig config = ServiceConfig.builder().authEnabled(true).build();
        resourceObjs.add(new SchemaRegistryResourceImpl(service, config));
        this.authFile = createAuthFile();

        return new RegistryApplication(resourceObjs);
    }

    @After
    public void tearDown() {
        authFile.delete();
    }

    @Test
    public void groups() throws ExecutionException, InterruptedException {
        GroupProperties group1 = new GroupProperties(SchemaType.Avro,
                SchemaValidationRules.of(Compatibility.backward()),
                false, Collections.singletonMap("Encode", Boolean.toString(false)));
        doAnswer(x -> {
            Map<String, GroupProperties> map = new HashMap<>();
            map.put("group1", group1);
            map.put("group2", null);
            return CompletableFuture.completedFuture(new MapWithToken<>(map, null));
        }).when(service).listGroups(any(), anyInt());

        Future<Response> future = target("v1/groups").queryParam("limit", 100).request().async().get();
        Response response = future.get();
        assertEquals(response.getStatus(), 200);
        ListGroupsResponse list = response.readEntity(ListGroupsResponse.class);
        assertEquals(list.getGroups().size(), 2);

        // region create group
        // endregion

        // region delete group
        // endregion
    }

    private File createAuthFile() {
        try {
            File authFile = File.createTempFile("auth_file", ".txt");
            StrongPasswordProcessor passwordEncryptor = StrongPasswordProcessor.builder().build();

            try (FileWriter writer = new FileWriter(authFile.getAbsolutePath())) {
                String defaultPassword = passwordEncryptor.encryptPassword("1111_aaaa");
                writer.write(credentialsAndAclAsString(UserNames.SYSTEM_ADMIN, defaultPassword, "*,READ_UPDATE;"));
                writer.write(credentialsAndAclAsString(UserNames.SYSTEM_READER, defaultPassword, "/,READ"));
                writer.write(credentialsAndAclAsString(UserNames.GROUP1_ADMIN, defaultPassword, "/group1,READ_UPDATE"));
                writer.write(credentialsAndAclAsString(UserNames.GROUP1_USER, defaultPassword, "/group1,READ"));
                writer.write(credentialsAndAclAsString(UserNames.GROUP2_USER, defaultPassword, "/group2,READ"));
            }
            return authFile;
        } catch (IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException(e);
        }
    }

    private String credentialsAndAclAsString(String username, String password, String acl) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(username)
                && !Strings.isNullOrEmpty(password)
                && acl != null
                && !acl.startsWith(":"));

        // This will return a string that looks like this:"<username>:<pasword>:acl\n"
        return String.format("%s:%s:%s%n", username, password, acl);
    }

    private static class UserNames {
        private final static String SYSTEM_ADMIN = "admin";
        private final static String SYSTEM_READER = "rootreader";
        private final static String GROUP1_ADMIN = "group1readupdate";
        private final static String GROUP1_USER = "group1read";
        private final static String GROUP2_USER = "group2read";
    }
}
