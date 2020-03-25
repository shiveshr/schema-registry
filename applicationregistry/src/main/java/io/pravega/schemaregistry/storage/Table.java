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
import io.pravega.schemaregistry.contract.data.VersionInfo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface Table<V> {
    Map<Class<? extends TableKey>, ? extends VersionedSerializer.WithBuilder<? extends TableValue,
            ? extends ObjectBuilder<? extends TableValue>>> SERIALIZERS_BY_KEY_TYPE =
            ImmutableMap.<Class<? extends TableKey>, VersionedSerializer.WithBuilder<? extends TableValue, ? extends ObjectBuilder<? extends TableValue>>>builder()
                    .put(Writer.class, SchemaVersions.SERIALIZER)
                    .put(Reader.class, SchemaVersions.SERIALIZER)
                    .put(TableEtag.class, TableEtag.SERIALIZER)
                    .build();

    Etag toEtag(V version);

    V fromEtag(Etag etag);

    CompletableFuture<Map<TableKey, TableValue>> getAllRecords();
    
    CompletableFuture<Void> updateEntries(Map<TableKey, ValueWithVersion<TableValue, V>> entries);

    <T extends TableValue> CompletableFuture<T> getRecord(TableKey key, Class<T> tClass);
    
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
    class Writer implements TableKey {
        public static final Writer.Serializer SERIALIZER = new Writer.Serializer();
        
        private final String appId;
        
        private static class WriterBuilder implements ObjectBuilder<Writer> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<Writer, Writer.WriterBuilder> {
            @Override
            protected Writer.WriterBuilder newBuilder() {
                return Writer.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(Writer e, RevisionDataOutput target) throws IOException {
                target.writeUTF(e.appId);
            }

            private void read00(RevisionDataInput source, Writer.WriterBuilder b) throws IOException {
                b.appId(source.readUTF());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class Reader implements TableKey {
        public static final Reader.Serializer SERIALIZER = new Reader.Serializer();

        private final String appId;

        private static class ReaderBuilder implements ObjectBuilder<Reader> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<Reader, Reader.ReaderBuilder> {
            @Override
            protected Reader.ReaderBuilder newBuilder() {
                return Reader.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(Reader e, RevisionDataOutput target) throws IOException {
                target.writeUTF(e.appId);
            }

            private void read00(RevisionDataInput source, Reader.ReaderBuilder b) throws IOException {
                b.appId(source.readUTF());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class SchemaVersions implements TableValue {
        public static final SchemaVersions.Serializer SERIALIZER = new SchemaVersions.Serializer();

        private final Map<String, VersionInfo> versions;

        @SneakyThrows
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class SchemaVersionsBuilder implements ObjectBuilder<SchemaVersions> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SchemaVersions, SchemaVersions.SchemaVersionsBuilder> {
            @Override
            protected SchemaVersions.SchemaVersionsBuilder newBuilder() {
                return SchemaVersions.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SchemaVersions e, RevisionDataOutput target) throws IOException {
                target.writeMap(e.versions, DataOutput::writeUTF, VersionInfoSerializer.SERIALIZER::serialize);
            }

            private void read00(RevisionDataInput source, SchemaVersions.SchemaVersionsBuilder b) throws IOException {
                b.versions(source.readMap(DataInput::readUTF, VersionInfoSerializer.SERIALIZER::deserialize));
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
