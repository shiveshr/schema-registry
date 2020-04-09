/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.test.integrationtest.demo.function.runtime;

import com.google.common.collect.Lists;
import lombok.Data;
import lombok.Synchronized;

import java.util.ArrayList;
import java.util.List;

@Data
public class Pipeline<I, O> {
    private final List<StreamProcess> pipeline;

    private Pipeline(List<StreamProcess> pipeline) {
        this.pipeline = pipeline;
    }

    @Synchronized
    public <K> Pipeline<I, K> addProcessing(StreamProcess<O, K> process) {
        ArrayList<StreamProcess> newPipeline = Lists.newArrayList(this.pipeline);
        StreamProcess previous = this.pipeline.get(pipeline.size() - 1);
        if (previous.getOutputStream().getScope().equals(process.getInputStream().getScope()) &&
            previous.getOutputStream().getStream().equals(process.getInputStream().getStream())) {
            newPipeline.add(process);
            return new Pipeline<I, K>(newPipeline);
        } else {
            throw new RuntimeException("Stream mismatch");
        }
    } 
    
    public static <T, K> Pipeline<T, K> of(StreamProcess<T, K> process) {
        return new Pipeline<>(Lists.newArrayList(process));
    }
}
