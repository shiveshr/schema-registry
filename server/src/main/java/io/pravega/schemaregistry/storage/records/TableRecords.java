/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.records;

import com.google.common.collect.ImmutableMap;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.cert.Extension;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Table Records with different implementations for {@link Key} and {@link Record}.
 */
public interface TableRecords {
    Map<Class<? extends Key>, ? extends VersionedSerializer.WithBuilder<? extends Record, ? extends ObjectBuilder<? extends Record>>> SERIALIZERS_BY_KEY_TYPE =
            ImmutableMap.<Class<? extends Key>, VersionedSerializer.WithBuilder<? extends Record, ? extends ObjectBuilder<? extends Record>>>builder()
                    .put(VersionInfoKey.class, SchemaInfoValue.SERIALIZER)
                    .put(SchemaFingerprintKey.class, SchemaVersionValue.SERIALIZER)
                    .put(LatestSchemaVersionKey.class, LatestSchemaVersionValue.SERIALIZER)
                    .put(GroupPropertyKey.class, GroupPropertiesValue.SERIALIZER)
                    .put(ValidationRulesKey.class, ValidationRulesValue.SERIALIZER)
                    .put(EncodingId.class, EncodingInfo.SERIALIZER)
                    .put(EncodingInfo.class, EncodingId.SERIALIZER)
                    .put(LatestEncodingIdKey.class, LatestEncodingIdValue.SERIALIZER)
                    .put(Etag.class, Etag.SERIALIZER)
                    .build();

    interface Key {
    }
    
    interface Record {
        byte[] toBytes();
    }

    @Data
    @Builder
    @AllArgsConstructor
    class GroupPropertyKey implements Key {
        public static final Serializer SERIALIZER = new Serializer();
        
        private static class GroupPropertyKeyBuilder implements ObjectBuilder<GroupPropertyKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<GroupPropertyKey, GroupPropertyKey.GroupPropertyKeyBuilder> {
            @Override
            protected GroupPropertyKey.GroupPropertyKeyBuilder newBuilder() {
                return GroupPropertyKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(GroupPropertyKey e, RevisionDataOutput target) throws IOException {
            }

            private void read00(RevisionDataInput source, GroupPropertyKey.GroupPropertyKeyBuilder b) throws IOException {
                
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class GroupPropertiesValue implements Record {
        public static final Serializer SERIALIZER = new Serializer();

        private final SchemaType schemaType;
        private final boolean validateByObjectType;
        private final Map<String, String> properties;

        @SneakyThrows
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class GroupPropertiesValueBuilder implements ObjectBuilder<GroupPropertiesValue> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<GroupPropertiesValue, GroupPropertiesValue.GroupPropertiesValueBuilder> {
            @Override
            protected GroupPropertiesValue.GroupPropertiesValueBuilder newBuilder() {
                return GroupPropertiesValue.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(GroupPropertiesValue e, RevisionDataOutput target) throws IOException {
                SchemaTypeRecord.SERIALIZER.serialize(target, new SchemaTypeRecord(e.schemaType));
                target.writeBoolean(e.validateByObjectType);
                target.writeMap(e.properties, DataOutput::writeUTF, DataOutput::writeUTF);
            }

            private void read00(RevisionDataInput source, GroupPropertiesValue.GroupPropertiesValueBuilder b) throws IOException {
                b.schemaType(SchemaTypeRecord.SERIALIZER.deserialize(source).getSchemaType())
                 .validateByObjectType(source.readBoolean())
                 .properties(source.readMap(DataInput::readUTF, DataInput::readUTF));
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class ValidationRulesKey implements Key {
        public static final Serializer SERIALIZER = new Serializer();
        
        private static class ValidationRulesKeyBuilder implements ObjectBuilder<ValidationRulesKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<ValidationRulesKey, ValidationRulesKey.ValidationRulesKeyBuilder> {
            @Override
            protected ValidationRulesKey.ValidationRulesKeyBuilder newBuilder() {
                return ValidationRulesKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(ValidationRulesKey e, RevisionDataOutput target) throws IOException {
            }

            private void read00(RevisionDataInput source, ValidationRulesKey.ValidationRulesKeyBuilder b) throws IOException {
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    public class ValidationRulesValue implements Record {
        public static final Serializer SERIALIZER = new Serializer();

        private final SchemaValidationRulesRecord validationRules;

        @SneakyThrows
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class ValidationRulesValueBuilder implements ObjectBuilder<ValidationRulesValue> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<ValidationRulesValue, ValidationRulesValue.ValidationRulesValueBuilder> {
            @Override
            protected ValidationRulesValue.ValidationRulesValueBuilder newBuilder() {
                return ValidationRulesValue.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(ValidationRulesValue e, RevisionDataOutput target) throws IOException {
                SchemaValidationRulesRecord.SERIALIZER.serialize(target, e.validationRules);
            }

            private void read00(RevisionDataInput source, ValidationRulesValue.ValidationRulesValueBuilder b) throws IOException {
                b.validationRules(SchemaValidationRulesRecord.SERIALIZER.deserialize(source));
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class SchemaFingerprintKey implements Key {
        public static final Serializer SERIALIZER = new Serializer();
        
        private final long fingerprint;

        private static class SchemaFingerprintKeyBuilder implements ObjectBuilder<SchemaFingerprintKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SchemaFingerprintKey, SchemaFingerprintKey.SchemaFingerprintKeyBuilder> {
            @Override
            protected SchemaFingerprintKey.SchemaFingerprintKeyBuilder newBuilder() {
                return SchemaFingerprintKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SchemaFingerprintKey e, RevisionDataOutput target) throws IOException {
                target.writeLong(e.fingerprint);
            }

            private void read00(RevisionDataInput source, SchemaFingerprintKey.SchemaFingerprintKeyBuilder b) throws IOException {
                b.fingerprint(source.readLong());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class SchemaInfoValue implements Record {
        public static final Serializer SERIALIZER = new Serializer();

        private final String name;
        private final SchemaTypeRecord schemaType;
        private final Map<String, String> properties;
        private final int schemaPosition;

        // either we encode the data or the UUID based on 
        private final Either<byte[], UUID> schemaData;

        @SneakyThrows
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class SchemaInfoValueBuilder implements ObjectBuilder<SchemaInfoValue> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SchemaInfoValue, SchemaInfoValue.SchemaInfoValueBuilder> {
            @Override
            protected SchemaInfoValue.SchemaInfoValueBuilder newBuilder() {
                return SchemaInfoValue.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SchemaInfoValue e, RevisionDataOutput target) throws IOException {
                target.writeUTF(e.name);
                SchemaTypeRecord.SERIALIZER.serialize(target, e.schemaType);
                if (!e.properties.isEmpty()) {
                    target.writeMap(e.properties, DataOutput::writeUTF, DataOutput::writeUTF);
                }
                target.writeInt(e.schemaPosition);
                
                if (e.schemaData.isLeft()) {
                    target.writeBoolean(true);
                    target.writeArray(e.schemaData.getLeft());
                } else {
                    target.writeBoolean(false);
                    target.writeUUID(e.schemaData.getRight());
                }
            }
            
            private void read00(RevisionDataInput source, SchemaInfoValue.SchemaInfoValueBuilder b) throws IOException {
                b.name(source.readUTF())
                 .schemaType(SchemaTypeRecord.SERIALIZER.deserialize(source))
                 .properties(source.readMap(DataInput::readUTF, DataInput::readUTF))
                 .schemaPosition(source.readInt());
                boolean isLeft = source.readBoolean();
                if (isLeft) {
                    b.schemaData(Either.left(source.readArray()));
                } else {
                    b.schemaData(Either.right(source.readUUID()));
                }
            }
        }
    }
    
    @Data
    @Builder
    @AllArgsConstructor
    class SchemaBytesChunkKey implements Key {
        public static final Serializer SERIALIZER = new Serializer();
        
        private final UUID id;

        private static class SchemaBytesChunkKeyBuilder implements ObjectBuilder<SchemaBytesChunkKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SchemaBytesChunkKey, SchemaBytesChunkKey.SchemaBytesChunkKeyBuilder> {
            @Override
            protected SchemaBytesChunkKey.SchemaBytesChunkKeyBuilder newBuilder() {
                return SchemaBytesChunkKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SchemaBytesChunkKey e, RevisionDataOutput target) throws IOException {
                target.writeUUID(e.id);
            }

            private void read00(RevisionDataInput source, SchemaBytesChunkKey.SchemaBytesChunkKeyBuilder b) throws IOException {
                b.id(source.readUUID());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class SchemaBytesChunkValue implements Record {
        public static final Serializer SERIALIZER = new Serializer();
        
        private final byte[] schemaDataChunk;

        @SneakyThrows
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class SchemaBytesChunkValueBuilder implements ObjectBuilder<SchemaBytesChunkValue> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SchemaBytesChunkValue, SchemaBytesChunkValue.SchemaBytesChunkValueBuilder> {
            @Override
            protected SchemaBytesChunkValue.SchemaBytesChunkValueBuilder newBuilder() {
                return SchemaBytesChunkValue.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SchemaBytesChunkValue e, RevisionDataOutput target) throws IOException {
                target.writeArray(e.schemaDataChunk);
            }

            private void read00(RevisionDataInput source, SchemaBytesChunkValue.SchemaBytesChunkValueBuilder b) throws IOException {
                b.schemaDataChunk(source.readArray());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class VersionInfoKey implements Key {
        public static final Serializer SERIALIZER = new Serializer();
        
        @Getter(AccessLevel.PRIVATE)
        private final VersionInfoRecord versionInfo;

        public VersionInfo getVersion() {
            return versionInfo.getVersionInfo();
        }
        
        private static class VersionInfoKeyBuilder implements ObjectBuilder<VersionInfoKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<VersionInfoKey, VersionInfoKey.VersionInfoKeyBuilder> {
            @Override
            protected VersionInfoKey.VersionInfoKeyBuilder newBuilder() {
                return VersionInfoKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(VersionInfoKey e, RevisionDataOutput target) throws IOException {
                VersionInfoRecord.SERIALIZER.serialize(target, e.versionInfo);
            }

            private void read00(RevisionDataInput source, VersionInfoKey.VersionInfoKeyBuilder b) throws IOException {
                b.versionInfo(VersionInfoRecord.SERIALIZER.deserialize(source));
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class SchemaVersionValue implements Record {
        public static final Serializer SERIALIZER = new Serializer();
        
        private final List<VersionInfoRecord> versions;

        @SneakyThrows
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();    
        }
        
        private static class SchemaVersionValueBuilder implements ObjectBuilder<SchemaVersionValue> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SchemaVersionValue, SchemaVersionValue.SchemaVersionValueBuilder> {
            @Override
            protected SchemaVersionValue.SchemaVersionValueBuilder newBuilder() {
                return SchemaVersionValue.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SchemaVersionValue e, RevisionDataOutput target) throws IOException {
                target.writeCollection(e.versions, VersionInfoRecord.SERIALIZER::serialize);
            }

            private void read00(RevisionDataInput source, SchemaVersionValue.SchemaVersionValueBuilder b) throws IOException {
                b.versions(new ArrayList<>(source.readCollection(VersionInfoRecord.SERIALIZER::deserialize)));
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class EncodingInfo implements Key, Record {
        public static final Serializer SERIALIZER = new Serializer();
        
        private final VersionInfoRecord versionInfo;
        private final CompressionTypeRecord compressionType;

        @SneakyThrows
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class EncodingInfoBuilder implements ObjectBuilder<EncodingInfo> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<EncodingInfo, EncodingInfo.EncodingInfoBuilder> {
            @Override
            protected EncodingInfo.EncodingInfoBuilder newBuilder() {
                return EncodingInfo.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(EncodingInfo e, RevisionDataOutput target) throws IOException {
                VersionInfoRecord.SERIALIZER.serialize(target, e.versionInfo);
                CompressionTypeRecord.SERIALIZER.serialize(target, e.compressionType);
            }

            private void read00(RevisionDataInput source, EncodingInfo.EncodingInfoBuilder b) throws IOException {
                b.versionInfo(VersionInfoRecord.SERIALIZER.deserialize(source))
                 .compressionType(CompressionTypeRecord.SERIALIZER.deserialize(source));
            }
        }
    }
    
    @Data
    @Builder
    @AllArgsConstructor
    class EncodingId implements Key, Record {
        public static final Serializer SERIALIZER = new Serializer();

        private final EncodingIdRecord id;
        
        @SneakyThrows
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        public io.pravega.schemaregistry.contract.data.EncodingId getEncodingId() {
            return id.getEncodingId();
        }

        private static class EncodingIdBuilder implements ObjectBuilder<EncodingId> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<EncodingId, EncodingId.EncodingIdBuilder> {
            @Override
            protected EncodingId.EncodingIdBuilder newBuilder() {
                return EncodingId.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(EncodingId e, RevisionDataOutput target) throws IOException {
                EncodingIdRecord.SERIALIZER.serialize(target, e.getId());
            }

            private void read00(RevisionDataInput source, EncodingId.EncodingIdBuilder b) throws IOException {
                b.id(EncodingIdRecord.SERIALIZER.deserialize(source));
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class LatestEncodingIdKey implements Key {
        public static final Serializer SERIALIZER = new Serializer();

        private static class LatestEncodingIdKeyBuilder implements ObjectBuilder<LatestEncodingIdKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<LatestEncodingIdKey, LatestEncodingIdKey.LatestEncodingIdKeyBuilder> {
            @Override
            protected LatestEncodingIdKey.LatestEncodingIdKeyBuilder newBuilder() {
                return LatestEncodingIdKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(LatestEncodingIdKey e, RevisionDataOutput target) throws IOException {
            }

            private void read00(RevisionDataInput source, LatestEncodingIdKey.LatestEncodingIdKeyBuilder b) throws IOException {

            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class LatestEncodingIdValue implements Record {
        public static final Serializer SERIALIZER = new Serializer();

        private final int id;

        @SneakyThrows
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class LatestEncodingIdValueBuilder implements ObjectBuilder<LatestEncodingIdValue> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<LatestEncodingIdValue, LatestEncodingIdValue.LatestEncodingIdValueBuilder> {
            @Override
            protected LatestEncodingIdValue.LatestEncodingIdValueBuilder newBuilder() {
                return LatestEncodingIdValue.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(LatestEncodingIdValue e, RevisionDataOutput target) throws IOException {
                target.writeInt(e.id);
            }

            private void read00(RevisionDataInput source, LatestEncodingIdValue.LatestEncodingIdValueBuilder b) throws IOException {
                b.id(source.readInt());
            }
        }
    }
    
    @Data
    @Builder
    @AllArgsConstructor
    class LatestSchemaVersionKey implements Key {
        public static final Serializer SERIALIZER = new Serializer();
        private final String schemaName;
        
        private static class LatestSchemaVersionKeyBuilder implements ObjectBuilder<LatestSchemaVersionKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<LatestSchemaVersionKey, LatestSchemaVersionKey.LatestSchemaVersionKeyBuilder> {
            @Override
            protected LatestSchemaVersionKey.LatestSchemaVersionKeyBuilder newBuilder() {
                return LatestSchemaVersionKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(LatestSchemaVersionKey e, RevisionDataOutput target) throws IOException {
                target.writeUTF(e.getSchemaName());
            }

            private void read00(RevisionDataInput source, LatestSchemaVersionKey.LatestSchemaVersionKeyBuilder b) throws IOException {
                b.schemaName(source.readUTF());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class LatestSchemaVersionValue implements Record {
        public static final Serializer SERIALIZER = new Serializer();

        private final VersionInfoRecord versionInfo;

        @SneakyThrows
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class LatestSchemaVersionValueBuilder implements ObjectBuilder<LatestSchemaVersionValue> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<LatestSchemaVersionValue, LatestSchemaVersionValue.LatestSchemaVersionValueBuilder> {
            @Override
            protected LatestSchemaVersionValue.LatestSchemaVersionValueBuilder newBuilder() {
                return LatestSchemaVersionValue.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(LatestSchemaVersionValue e, RevisionDataOutput target) throws IOException {
                VersionInfoRecord.SERIALIZER.serialize(target, e.versionInfo);
            }

            private void read00(RevisionDataInput source, LatestSchemaVersionValue.LatestSchemaVersionValueBuilder b) throws IOException {
                b.versionInfo(VersionInfoRecord.SERIALIZER.deserialize(source));
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class Etag implements Key, Record {
        public static final Serializer SERIALIZER = new Serializer();

        @SneakyThrows
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class EtagBuilder implements ObjectBuilder<Etag> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<Etag, Etag.EtagBuilder> {
            @Override
            protected Etag.EtagBuilder newBuilder() {
                return Etag.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(Etag e, RevisionDataOutput target) throws IOException {
            }

            private void read00(RevisionDataInput source, Etag.EtagBuilder b) throws IOException {

            }
        }
    }
    
    @SneakyThrows(IOException.class)
    @SuppressWarnings("unchecked")
    static Record fromBytes(Class<? extends Key> clasz, byte[] bytes) {
        return SERIALIZERS_BY_KEY_TYPE.get(clasz).deserialize(bytes);
    }
}
