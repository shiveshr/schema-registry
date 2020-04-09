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

import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

@Data
public class StreamProcess<I, O> {
    private final Source<I> inputStream;
    private final Sink<O> outputStream;
    private final List<Operation> operations;
    private final int parallelism;
    private final Function<O, String> routingKeyFunction;

    private StreamProcess(Source<I> inputStream, Sink<O> outputStream, List<Operation> operations, int parallelism,
                          Function<O, String> routingKeyFunction) {
        this.routingKeyFunction = routingKeyFunction;
        Preconditions.checkNotNull(inputStream);
        Preconditions.checkNotNull(outputStream);
        Preconditions.checkNotNull(operations);
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        this.operations = operations;
        this.parallelism = parallelism;
    }

    @SuppressWarnings("unchecked")
    O process(I event, Context context) {
        AtomicReference<Object> next = new AtomicReference<>(event);
        for (Operation x : operations) {
            Object process = x.process(next.get(), context);
            if (x instanceof WindowedMapFunc && process == null) {
                return null;
            }
            next.set(process);
        }
        return (O) next.get();
    }

    public static <T> StreamProcessBuilder<T, T> readFrom(String scope, String inputStream, String inputSerDe, URL inputSerDeFilepath, Class<T> inputClass) {
        return readFrom(scope, inputStream, inputSerDe, inputSerDeFilepath, inputClass, 1);
    }

    @SuppressWarnings("unchecked")
    public static <T> StreamProcessBuilder<T, T> readFrom(String scope, String inputStream, String inputSerDe, URL inputSerDeFilepath, Class<T> inputClass, int parallelism) {
        SerDe<T> serDe = LoaderUtil.getInstance(inputSerDe, inputSerDeFilepath, SerDe.class);
        Source<T> source = new Source<>(scope, inputStream, serDe);
        return new StreamProcessBuilder<>(source, parallelism);
    }

    public static class StreamProcessBuilder<T, K> {
        private final Source<T> inputStream;
        private int parallelism;

        private final List<Operation> operations;

        private StreamProcessBuilder(Source<T> is, int parallelism) {
            this.inputStream = is;
            this.parallelism = parallelism;
            this.operations = new LinkedList<>();
        }

        private <P> StreamProcessBuilder(StreamProcessBuilder<T, P> previous, Operation<P, K> is) {
            this.inputStream = previous.inputStream;
            this.parallelism = previous.parallelism;
            this.operations = Lists.newArrayList(previous.operations);
            this.operations.add(is);
        }

        public <O> StreamProcess<T, O> writeTo(String scope, String outputStream, String outputSerDe, URL outputSerDeFilepath) {
            return writeTo(scope, outputStream, outputSerDe, outputSerDeFilepath, Object::toString);
        }

        @SuppressWarnings("unchecked")
        public <O> StreamProcess<T, O> writeTo(String scope, String outputStream, String outputSerDe, URL outputSerDeFilepath,
                                               Function<O, String> routingKeyFunction) {
            SerDe<O> serDe = LoaderUtil.getInstance(outputSerDe, outputSerDeFilepath, SerDe.class);

            Sink<O> os = new Sink<>(scope, outputStream, serDe);
            return new StreamProcess<>(inputStream, os, operations, parallelism, routingKeyFunction);
        }

        public <O> StreamProcessBuilder<T, O> map(Function<K, O> mapFunc) {
            return new StreamProcessBuilder<T, O>(this, new MapFunc<>(mapFunc));
        }

        @SuppressWarnings("unchecked")
        public <O> StreamProcessBuilder<T, O> map(String functionName, URL funcFilepath, Class<O> type) {
            Function<K, O> func = LoaderUtil.getInstance(functionName, funcFilepath, Function.class);

            return new StreamProcessBuilder<T, O>(this, new MapFunc<>(func));
        }

        public <O> StreamProcessBuilder<T, O> windowedMap(WindowedFunction<K, O> windowedFunction, int window) {
            return new StreamProcessBuilder<T, O>(this, new WindowedMapFunc<>(windowedFunction, window));
        }

        @SuppressWarnings("unchecked")
        public <O> StreamProcessBuilder<T, O> windowedMap(String functionName, URL funcFilepath, int window, Class<O> type) {
            WindowedFunction<K, O> func = LoaderUtil.getInstance(functionName, funcFilepath, WindowedFunction.class);

            return new StreamProcessBuilder<T, O>(this, new WindowedMapFunc<>(func, window));
        }
    }

    private interface Operation<T, K> {
        K process(T t, Context context);
    }

    @Data
    private static class MapFunc<T, K> implements Operation<T, K> {
        private final Function<T, K> mapFunc;

        @Override
        public K process(T input, Context context) {
            return mapFunc.apply(input);
        }
    }

    @Data
    private static class WindowedMapFunc<T, K> implements Operation<T, K> {
        private final WindowedFunction<T, K> mapFunc;
        private final ConcurrentHashMap<Context, List<T>> contextWindow = new ConcurrentHashMap<>();
        private final int window;

        @Override
        public K process(T input, Context context) {
            List<T> collection;
            collection = contextWindow.compute(context, (x, y) -> {
                if (y == null) {
                    return new ArrayList<>(window);
                } else {
                    return y;
                }
            });

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

    interface Context {
    }
}
