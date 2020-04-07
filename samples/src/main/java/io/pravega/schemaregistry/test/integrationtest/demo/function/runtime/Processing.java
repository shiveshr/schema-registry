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
import io.pravega.schemaregistry.test.integrationtest.demo.function.interfaces.Function;
import io.pravega.schemaregistry.test.integrationtest.demo.function.interfaces.SerDe;
import io.pravega.schemaregistry.test.integrationtest.demo.function.interfaces.WindowedFunction;
import io.pravega.schemaregistry.test.integrationtest.demo.util.LoaderUtil;
import lombok.Data;

import java.net.URL;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@Data
public class Processing<I, O> {
    private final Source<I> inputStream;
    private final Sink<O> outputStream;
    private final List<Operation> operations;

    private Processing(Source<I> inputStream, Sink<O> outputStream, List<Operation> operations) {
        Preconditions.checkNotNull(inputStream);
        Preconditions.checkNotNull(outputStream);
        Preconditions.checkNotNull(operations);
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        this.operations = operations;
    }

    @SuppressWarnings("unchecked")
    O process(I event) {
        AtomicReference<Object> next = new AtomicReference<>(event);
        for (Operation x : operations) {
            Object process = x.process(next.get());
            if (x instanceof WindowedMapFunc && process == null) {
                return null;
            } 
            next.set(process);
        }
        return (O) next.get();
    }
    
    public static class ProcessingBuilder<T, K> {
        private Source<T> inputStream;
        private Sink<K> outputStream;

        private final List<Operation> operations = new LinkedList<>();

        @SuppressWarnings("unchecked")
        public ProcessingBuilder inputStream(String scope, String inputStream, String inputSerDe, URL inputSerDeFilepath) {
            SerDe<T> serDe = LoaderUtil.getInstance(inputSerDe, inputSerDeFilepath, SerDe.class);
            this.inputStream = new Source<>(scope, inputStream, serDe);
            return this;
        }

        @SuppressWarnings("unchecked")
        public ProcessingBuilder outputStream(String scope, String outputStream, String outputSerDe, URL outputSerDeFilepath) {
            SerDe<K> serDe = LoaderUtil.getInstance(outputSerDe, outputSerDeFilepath, SerDe.class);

            this.outputStream = new Sink<>(scope, outputStream, serDe);
            return this;
        }
        
        public <I, O> ProcessingBuilder map(java.util.function.Function<I, O> mapFunc) {
            Function<I, O> function = mapFunc::apply;
            this.operations.add(new MapFunc<>(function));
            return this;
        }

        @SuppressWarnings("unchecked")
        public <I, O> ProcessingBuilder map(String functionName, URL funcFilepath) {
            Function<I, O> func = LoaderUtil.getInstance(functionName, funcFilepath, Function.class);

            this.operations.add(new MapFunc<>(func));
            return this;
        }

        @SuppressWarnings("unchecked")
        public <I, O> ProcessingBuilder windowedMap(String functionName, URL funcFilepath, int window) {
            WindowedFunction<Collection<I>, O> func = LoaderUtil.getInstance(functionName, funcFilepath, WindowedFunction.class);

            this.operations.add(new WindowedMapFunc<>(func, window));
            return this;
        }

        public Processing<T, K> build() {
            return new Processing<>(inputStream, outputStream, operations);
        }
    }

    private interface Operation<T, K> {
        K process(T t);
    }

    @Data
    private static class MapFunc<T, K> implements Operation<T, K>  {
        private final Function<T, K> mapFunc;

        @Override
        public K process(T input) {
            return mapFunc.apply(input);
        }
    }
    
    @Data
    private static class WindowedMapFunc<T, K> implements Operation<T, K>  {
        private final WindowedFunction<T, K> mapFunc;
        private final List<T> collection = new LinkedList<>();
        private final int window;
        
        @Override
        public K process(T input) {
            if (collection.size() < window) {
                collection.add(input);
            } 
            
            if (collection.size() == window) {
                K out = mapFunc.apply(collection);
                collection.removeIf(x -> true);
                return out;
            } else {
                return null;
            }
        }
    }

    @Data
    static class Source<T> {
        private final String scope;
        private final String stream;
        private final SerDe<T> serDe;
    }

    @Data
    static class Sink<T> {
        private final String scope;
        private final String stream;
        private final SerDe<T> serDe;
    }
}
