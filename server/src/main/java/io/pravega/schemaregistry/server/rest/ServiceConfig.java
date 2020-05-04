/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest;

import com.google.common.base.Strings;
import io.pravega.auth.ServerConfig;
import io.pravega.common.Exceptions;
import lombok.Builder;
import lombok.Getter;

/**
 * REST server config.
 */
@Getter
@Builder
public class ServiceConfig implements ServerConfig {
    private final String host;
    private final int port;
    private final boolean tlsEnabled;
    private final String keyFilePath;
    private final String keyFilePasswordPath;
    private final boolean authEnabled;

    private ServiceConfig(String host, int port, boolean tlsEnabled, String keyFilePath, String keyFilePasswordPath, boolean authEnabled) {
        Exceptions.checkNotNullOrEmpty(host, "host");
        Exceptions.checkArgument(port > 0, "port", "Should be positive integer");
        Exceptions.checkArgument(!tlsEnabled || (!Strings.isNullOrEmpty(keyFilePath) 
                && !Strings.isNullOrEmpty(keyFilePasswordPath)), "keyFilePath", 
                "If tls is enabled then key file path and key file password path should be non empty");
        this.host = host;
        this.port = port;
        this.tlsEnabled = tlsEnabled;
        this.keyFilePath = keyFilePath;
        this.keyFilePasswordPath = keyFilePasswordPath;
        this.authEnabled = authEnabled;
    }

    public static final class ServiceConfigBuilder {
        private boolean tlsEnabled = false;
        private String keyFilePath = "";
        private String keyFilePasswordPath = "";
    }
    
    @Override
    public String toString() {
        // Note: We don't use Lombok @ToString to automatically generate an implementation of this method,
        // in order to avoid returning a string containing sensitive security configuration.

        return new StringBuilder(String.format("%s(", getClass().getSimpleName()))
                .append(String.format("host: %s, ", host))
                .append(String.format("port: %d, ", port))
                .toString();
    }
}
