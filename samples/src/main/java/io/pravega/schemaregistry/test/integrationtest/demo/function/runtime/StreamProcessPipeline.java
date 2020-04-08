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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.pravega.schemaregistry.test.integrationtest.demo.function.interfaces.Function;
import io.pravega.schemaregistry.test.integrationtest.demo.function.interfaces.SerDe;
import io.pravega.schemaregistry.test.integrationtest.demo.function.interfaces.WindowedFunction;
import io.pravega.schemaregistry.test.integrationtest.demo.util.LoaderUtil;
import lombok.Data;
import lombok.Synchronized;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@Data
public class StreamProcessPipeline<I, O> {
    private final List<StreamProcess> pipeline;

    private StreamProcessPipeline(List<StreamProcess> pipeline) {
        this.pipeline = pipeline;
    }

    @Synchronized
    public <K> StreamProcessPipeline<I, K> addProcessing(StreamProcess<O, K> process) {
        ArrayList<StreamProcess> newPipeline = Lists.newArrayList(this.pipeline);
        newPipeline.add(process);
        return new StreamProcessPipeline<I, K>(newPipeline);
    } 
    
    public static <T, K> StreamProcessPipeline<T, K> of(StreamProcess<T, K> process) {
        return new StreamProcessPipeline<>(Lists.newArrayList(process));
    }
}
