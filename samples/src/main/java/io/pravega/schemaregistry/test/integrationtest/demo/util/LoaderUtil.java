/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.test.integrationtest.demo.util;

import lombok.SneakyThrows;

import java.net.URL;
import java.net.URLClassLoader;
import java.security.PrivilegedAction;

import static java.security.AccessController.doPrivileged;

public class LoaderUtil {
    public static <T> T getInstance(String className, URL url, Class<T> interfaceCls) {
        final PrivilegedAction<T> action = new PrivilegedAction<T>() {
            @Override
            @SneakyThrows
            @SuppressWarnings("unchecked")
            public T run() {
                Class<?> aClass;
                // Load the class.
                URLClassLoader loader = new URLClassLoader(
                        new URL[]{url},
                        this.getClass().getClassLoader()
                );
                aClass = Class.forName(className, true, loader);
                if (interfaceCls.isAssignableFrom(aClass)) {
                    return (T) aClass.newInstance();
                } else {
                    throw new RuntimeException("implementation not found");
                }
            }
        };
        return doPrivileged(action);
    }
}
