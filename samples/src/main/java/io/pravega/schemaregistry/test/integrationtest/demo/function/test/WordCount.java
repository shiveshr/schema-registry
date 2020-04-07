/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.test.integrationtest.demo.function.test;

import io.pravega.schemaregistry.test.integrationtest.demo.function.interfaces.WindowedFunction;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class WordCount implements WindowedFunction<String[], Map<String, Integer>> {
    @Override
    public Map<String, Integer> apply(Collection<String[]> i) {
        Map<String, Integer> wordCounted = new HashMap<>();
        for (String[] strArray : i) {
            for (String s : strArray) {
                wordCounted.compute(s, (a, b) -> {
                    if (b == null) {
                        b = 0;
                    }
                    return b + 1;
                });
            }
        }
        return wordCounted;
    }
}