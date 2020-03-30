/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage;

import com.google.common.collect.ImmutableMap;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public interface Table<V> {
    Map<Class<? extends TableKey>, ? extends VersionedSerializer.WithBuilder<? extends TableValue,
            ? extends ObjectBuilder<? extends TableValue>>> SERIALIZERS_BY_KEY_TYPE =
            ImmutableMap.<Class<? extends TableKey>, VersionedSerializer.WithBuilder<? extends TableValue, ? extends ObjectBuilder<? extends TableValue>>>builder()
                    .put(WriterKey.class, WriterValue.SERIALIZER)
                    .put(ReaderKey.class, ReaderValue.SERIALIZER)
                    .put(TableEtag.class, TableEtag.SERIALIZER)
                    .build();

    Etag toEtag(V version);

    V fromEtag(Etag etag);

    CompletableFuture<Map<TableKey, TableValue>> getAllRecords();
    
    CompletableFuture<Void> updateEntries(Map<TableKey, ValueWithVersion<TableValue, V>> entries);

    <T extends TableValue> CompletableFuture<T> getRecord(TableKey key, Class<T> tClass);
    
    <T extends TableValue> CompletableFuture<List<ValueWithVersion<T, V>>> getRecords(List<? extends TableKey> key, Class<T> tClass);
    
    CompletableFuture<Void> addRecord(TableKey key, TableValue value);
    
    CompletableFuture<Void> deleteRecord(TableKey key);
    
    <T extends TableValue> CompletableFuture<ValueWithVersion<T, V>> getRecordWithVersion(TableKey key, Class<T> tClass);
    
    interface TableKey {

    }

    interface TableValue {
        byte[] toBytes();
    }

    @Data
    @Builder
    @AllArgsConstructor
    class WriterKey implements TableKey {
        public static final WriterKey.Serializer SERIALIZER = new WriterKey.Serializer();
        
        private final String appId;
        private final String writerId;
        
        private static class WriterKeyBuilder implements ObjectBuilder<WriterKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<WriterKey, WriterKey.WriterKeyBuilder> {
            @Override
            protected WriterKey.WriterKeyBuilder newBuilder() {
                return WriterKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(WriterKey e, RevisionDataOutput target) throws IOException {
                target.writeUTF(e.appId);
                target.writeUTF(e.writerId);
            }

            private void read00(RevisionDataInput source, WriterKey.WriterKeyBuilder b) throws IOException {
                b.appId(source.readUTF());
                b.writerId(source.readUTF());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class ReaderKey implements TableKey {
        public static final ReaderKey.Serializer SERIALIZER = new ReaderKey.Serializer();

        private final String appId;
        private final String readerId;

        private static class ReaderKeyBuilder implements ObjectBuilder<ReaderKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<ReaderKey, ReaderKey.ReaderKeyBuilder> {
            @Override
            protected ReaderKey.ReaderKeyBuilder newBuilder() {
                return ReaderKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(ReaderKey e, RevisionDataOutput target) throws IOException {
                target.writeUTF(e.appId);
                target.writeUTF(e.readerId);
            }

            private void read00(RevisionDataInput source, ReaderKey.ReaderKeyBuilder b) throws IOException {
                b.appId(source.readUTF());
                b.readerId(source.readUTF());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class WriterValue implements TableValue {
        public static final WriterValue.Serializer SERIALIZER = new WriterValue.Serializer();
        
        private final List<VersionInfo> versions;
        private final CodecType codec;

        @SneakyThrows
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class WriterValueBuilder implements ObjectBuilder<WriterValue> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<WriterValue, WriterValue.WriterValueBuilder> {
            @Override
            protected WriterValue.WriterValueBuilder newBuilder() {
                return WriterValue.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(WriterValue e, RevisionDataOutput target) throws IOException {
                target.writeCollection(e.versions, VersionInfoSerializer.SERIALIZER::serialize);
                CodecTypeRecord.SERIALIZER.serialize(target, new CodecTypeRecord(e.getCodec()));
            }

            private void read00(RevisionDataInput source, WriterValue.WriterValueBuilder b) throws IOException {
                b.versions(new ArrayList<>(source.readCollection(VersionInfoSerializer.SERIALIZER::deserialize)));
                b.codec(CodecTypeRecord.SERIALIZER.deserialize(source).getCodecType());
            }
        }
    }
    
    @Data
    @Builder
    @AllArgsConstructor
    class ReaderValue implements TableValue {
        public static final ReaderValue.Serializer SERIALIZER = new ReaderValue.Serializer();

        private final List<VersionInfo> versions;
        private final List<CodecType> decoders;

        @SneakyThrows
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class ReaderValueBuilder implements ObjectBuilder<ReaderValue> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<ReaderValue, ReaderValue.ReaderValueBuilder> {
            @Override
            protected ReaderValue.ReaderValueBuilder newBuilder() {
                return ReaderValue.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(ReaderValue e, RevisionDataOutput target) throws IOException {
                target.writeCollection(e.getVersions(), VersionInfoSerializer.SERIALIZER::serialize);
                target.writeCollection(e.getDecoders().stream().map(CodecTypeRecord::new).collect(Collectors.toList()),
                    CodecTypeRecord.SERIALIZER::serialize);
            }

            private void read00(RevisionDataInput source, ReaderValue.ReaderValueBuilder b) throws IOException {
                b.versions(new ArrayList<>(source.readCollection(VersionInfoSerializer.SERIALIZER::deserialize)));
                b.decoders(new ArrayList<>(source.readCollection(CodecTypeRecord.SERIALIZER::deserialize)
                                                     .stream().map(CodecTypeRecord::getCodecType).collect(Collectors.toList())));
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class TableEtag implements TableKey, TableValue {
        public static final Serializer SERIALIZER = new Serializer();

        @SneakyThrows
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class TableEtagBuilder implements ObjectBuilder<TableEtag> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<TableEtag, TableEtag.TableEtagBuilder> {
            @Override
            protected TableEtag.TableEtagBuilder newBuilder() {
                return TableEtag.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(TableEtag e, RevisionDataOutput target) throws IOException {
            }

            private void read00(RevisionDataInput source, TableEtag.TableEtagBuilder b) throws IOException {

            }
        }

    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static <T extends TableValue> T fromBytes(Class<? extends TableKey> keyClass, byte[] bytes, Class<T> valueClass) {
        return (T) SERIALIZERS_BY_KEY_TYPE.get(keyClass).deserialize(bytes);
    }

    @Data
    class ValueWithVersion<T extends TableValue, V> {
        private final T value;
        private final V version;
    }
}
