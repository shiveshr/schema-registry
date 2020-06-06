/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl.group.records;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Index Records with different implementations for {@link TableKey} and {@link TableValue}.
 */
public interface TableRecords {
    Map<Class<? extends TableKey>, ? extends VersionedSerializer.WithBuilder<? extends TableValue,
            ? extends ObjectBuilder<? extends TableValue>>> SERIALIZERS_BY_KEY_TYPE =
            ImmutableMap.<Class<? extends TableKey>, VersionedSerializer.WithBuilder<? extends TableValue, ? extends ObjectBuilder<? extends TableValue>>>builder()
                    .put(VersionKey.class, SchemaRecord.SERIALIZER)
                    .put(VersionDeletedRecord.class, VersionDeletedRecord.SERIALIZER)
                    .put(SchemaFingerprintKey.class, SchemaVersionList.SERIALIZER)
                    .put(GroupPropertyKey.class, GroupPropertiesRecord.SERIALIZER)
                    .put(ValidationPolicyKey.class, ValidationRecord.SERIALIZER)
                    .put(Etag.class, Etag.SERIALIZER)
                    .put(CodecTypesKey.class, CodecTypesListValue.SERIALIZER)
                    .put(SchemaTypesKey.class, SchemaTypesListValue.SERIALIZER)
                    .put(EncodingIdRecord.class, EncodingInfoRecord.SERIALIZER)
                    .put(EncodingInfoRecord.class, EncodingIdRecord.SERIALIZER)
                    .put(LatestEncodingIdKey.class, LatestEncodingIdValue.SERIALIZER)
                    .put(LatestSchemaVersionKey.class, LatestSchemaVersionValue.SERIALIZER)
                    .put(LatestSchemaVersionForTypeKey.class, LatestSchemaVersionValue.SERIALIZER)
                    .put(SchemaTypeVersionKey.class, VersionOrdinalValue.SERIALIZER)
                    .build();

    interface TableKey {
    }

    interface TableValue {
        byte[] toBytes();
    }

    @Data
    @Builder
    @AllArgsConstructor
    class GroupPropertyKey implements TableKey {
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
    class GroupPropertiesRecord implements TableValue {
        public static final GroupPropertiesRecord.Serializer SERIALIZER = new GroupPropertiesRecord.Serializer();

        private final SerializationFormat serializationFormat;
        private final boolean allowMultipleTypes;
        private final Map<String, String> properties;

        @Override
        @SneakyThrows
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class GroupPropertiesRecordBuilder implements ObjectBuilder<GroupPropertiesRecord> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<GroupPropertiesRecord, GroupPropertiesRecord.GroupPropertiesRecordBuilder> {
            @Override
            protected GroupPropertiesRecord.GroupPropertiesRecordBuilder newBuilder() {
                return GroupPropertiesRecord.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(GroupPropertiesRecord e, RevisionDataOutput target) throws IOException {
                SerializationFormatRecord.SERIALIZER.serialize(target, new SerializationFormatRecord(e.serializationFormat));
                target.writeBoolean(e.allowMultipleTypes);
                target.writeMap(e.properties, DataOutput::writeUTF, DataOutput::writeUTF);
            }

            private void read00(RevisionDataInput source, GroupPropertiesRecord.GroupPropertiesRecordBuilder b) throws IOException {
                b.serializationFormat(SerializationFormatRecord.SERIALIZER.deserialize(source).getSerializationFormat())
                 .allowMultipleTypes(source.readBoolean())
                 .properties(source.readMap(DataInput::readUTF, DataInput::readUTF));
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class ValidationPolicyKey implements TableKey {
        public static final Serializer SERIALIZER = new Serializer();

        private static class ValidationPolicyKeyBuilder implements ObjectBuilder<ValidationPolicyKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<ValidationPolicyKey, ValidationPolicyKey.ValidationPolicyKeyBuilder> {
            @Override
            protected ValidationPolicyKey.ValidationPolicyKeyBuilder newBuilder() {
                return ValidationPolicyKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(ValidationPolicyKey e, RevisionDataOutput target) throws IOException {
            }

            private void read00(RevisionDataInput source, ValidationPolicyKey.ValidationPolicyKeyBuilder b) throws IOException {
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class ValidationRecord implements TableValue {
        public static final ValidationRecord.Serializer SERIALIZER = new ValidationRecord.Serializer();

        private final SchemaValidationRules validationRules;

        @Override
        @SneakyThrows
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class ValidationRecordBuilder implements ObjectBuilder<ValidationRecord> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<ValidationRecord, ValidationRecordBuilder> {
            @Override
            protected ValidationRecordBuilder newBuilder() {
                return ValidationRecord.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(ValidationRecord e, RevisionDataOutput target) throws IOException {
                SchemaValidationRulesSerializer.SERIALIZER.serialize(target, e.validationRules);
            }

            private void read00(RevisionDataInput source, ValidationRecordBuilder b) throws IOException {
                b.validationRules(SchemaValidationRulesSerializer.SERIALIZER.deserialize(source));
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class Etag implements TableKey, TableValue {
        public static final Serializer SERIALIZER = new Serializer();

        @Override
        @SneakyThrows
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

    @Data
    @Builder
    @AllArgsConstructor
    class SchemaFingerprintKey implements TableKey {
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
    class SchemaVersionList implements TableValue {
        public static final Serializer SERIALIZER = new Serializer();

        private final List<VersionInfo> versions;

        @SneakyThrows
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class SchemaVersionListBuilder implements ObjectBuilder<SchemaVersionList> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SchemaVersionList, SchemaVersionList.SchemaVersionListBuilder> {
            @Override
            protected SchemaVersionList.SchemaVersionListBuilder newBuilder() {
                return SchemaVersionList.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SchemaVersionList e, RevisionDataOutput target) throws IOException {
                target.writeCollection(e.versions, VersionInfoSerializer.SERIALIZER::serialize);
            }

            private void read00(RevisionDataInput source, SchemaVersionList.SchemaVersionListBuilder b) throws IOException {
                b.versions(new ArrayList<>(source.readCollection(VersionInfoSerializer.SERIALIZER::deserialize)));
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class VersionKey implements TableKey {
        public static final Serializer SERIALIZER = new Serializer();

        private final int ordinal;

        private static class VersionKeyBuilder implements ObjectBuilder<VersionKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<VersionKey, VersionKey.VersionKeyBuilder> {
            @Override
            protected VersionKey.VersionKeyBuilder newBuilder() {
                return VersionKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(VersionKey e, RevisionDataOutput target) throws IOException {
                target.writeInt(e.ordinal);
            }

            private void read00(RevisionDataInput source, VersionKey.VersionKeyBuilder b) throws IOException {
                b.ordinal(source.readInt());
            }
        }
    }
    
    @Data
    @Builder
    @AllArgsConstructor
    class SchemaTypeVersionKey implements TableKey {
        public static final Serializer SERIALIZER = new Serializer();

        private final String schemaType;
        private final int version;

        private static class SchemaTypeVersionKeyBuilder implements ObjectBuilder<SchemaTypeVersionKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SchemaTypeVersionKey, SchemaTypeVersionKey.SchemaTypeVersionKeyBuilder> {
            @Override
            protected SchemaTypeVersionKey.SchemaTypeVersionKeyBuilder newBuilder() {
                return SchemaTypeVersionKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SchemaTypeVersionKey e, RevisionDataOutput target) throws IOException {
                target.writeUTF(e.schemaType);
                target.writeInt(e.version);
            }

            private void read00(RevisionDataInput source, SchemaTypeVersionKey.SchemaTypeVersionKeyBuilder b) throws IOException {
                b.schemaType(source.readUTF());
                b.version(source.readInt());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class VersionDeletedRecord implements TableKey, TableValue {
        public static final Serializer SERIALIZER = new Serializer();

        private final int ordinal;

        @SneakyThrows
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class VersionDeletedRecordBuilder implements ObjectBuilder<VersionDeletedRecord> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<VersionDeletedRecord, VersionDeletedRecord.VersionDeletedRecordBuilder> {
            @Override
            protected VersionDeletedRecord.VersionDeletedRecordBuilder newBuilder() {
                return VersionDeletedRecord.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(VersionDeletedRecord e, RevisionDataOutput target) throws IOException {
                target.writeInt(e.ordinal);
            }

            private void read00(RevisionDataInput source, VersionDeletedRecord.VersionDeletedRecordBuilder b) throws IOException {
                b.ordinal(source.readInt());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class SchemaRecord implements TableValue {
        public static final SchemaRecord.Serializer SERIALIZER = new SchemaRecord.Serializer();

        private final SchemaInfo schemaInfo;
        private final VersionInfo versionInfo;
        private final SchemaValidationRules validationRules;
        private final long timestamp;

        @Override
        @SneakyThrows
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class SchemaRecordBuilder implements ObjectBuilder<SchemaRecord> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SchemaRecord, SchemaRecord.SchemaRecordBuilder> {
            @Override
            protected SchemaRecord.SchemaRecordBuilder newBuilder() {
                return SchemaRecord.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SchemaRecord e, RevisionDataOutput target) throws IOException {
                SchemaInfoSerializer.SERIALIZER.serialize(target, e.schemaInfo);
                VersionInfoSerializer.SERIALIZER.serialize(target, e.versionInfo);
                SchemaValidationRulesSerializer.SERIALIZER.serialize(target, e.validationRules);
                target.writeLong(e.timestamp);
            }

            private void read00(RevisionDataInput source, SchemaRecord.SchemaRecordBuilder b) throws IOException {
                b.schemaInfo(SchemaInfoSerializer.SERIALIZER.deserialize(source))
                 .versionInfo(VersionInfoSerializer.SERIALIZER.deserialize(source))
                 .validationRules(SchemaValidationRulesSerializer.SERIALIZER.deserialize(source))
                 .timestamp(source.readLong());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class CodecTypesKey implements TableKey {
        public static final Serializer SERIALIZER = new Serializer();

        private static class CodecTypesKeyBuilder implements ObjectBuilder<CodecTypesKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<CodecTypesKey, CodecTypesKey.CodecTypesKeyBuilder> {
            @Override
            protected CodecTypesKey.CodecTypesKeyBuilder newBuilder() {
                return CodecTypesKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(CodecTypesKey e, RevisionDataOutput target) throws IOException {
            }

            private void read00(RevisionDataInput source, CodecTypesKey.CodecTypesKeyBuilder b) throws IOException {
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class CodecTypesListValue implements TableValue {
        public static final Serializer SERIALIZER = new Serializer();

        private final List<String> codecTypes;

        @SneakyThrows
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class CodecTypesListValueBuilder implements ObjectBuilder<CodecTypesListValue> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<CodecTypesListValue, CodecTypesListValue.CodecTypesListValueBuilder> {
            @Override
            protected CodecTypesListValue.CodecTypesListValueBuilder newBuilder() {
                return CodecTypesListValue.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(CodecTypesListValue e, RevisionDataOutput target) throws IOException {
                target.writeCollection(e.codecTypes, DataOutput::writeUTF);
            }

            private void read00(RevisionDataInput source, CodecTypesListValue.CodecTypesListValueBuilder b) throws IOException {
                b.codecTypes(Lists.newArrayList(source.readCollection(DataInput::readUTF)));
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class SchemaTypesKey implements TableKey {
        public static final Serializer SERIALIZER = new Serializer();

        private static class SchemaTypesKeyBuilder implements ObjectBuilder<SchemaTypesKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SchemaTypesKey, SchemaTypesKey.SchemaTypesKeyBuilder> {
            @Override
            protected SchemaTypesKey.SchemaTypesKeyBuilder newBuilder() {
                return SchemaTypesKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SchemaTypesKey e, RevisionDataOutput target) throws IOException {
            }

            private void read00(RevisionDataInput source, SchemaTypesKey.SchemaTypesKeyBuilder b) throws IOException {
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class SchemaTypesListValue implements TableValue {
        public static final Serializer SERIALIZER = new Serializer();

        private final List<String> types;

        @SneakyThrows
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class SchemaTypesListValueBuilder implements ObjectBuilder<SchemaTypesListValue> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SchemaTypesListValue, SchemaTypesListValue.SchemaTypesListValueBuilder> {
            @Override
            protected SchemaTypesListValue.SchemaTypesListValueBuilder newBuilder() {
                return SchemaTypesListValue.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SchemaTypesListValue e, RevisionDataOutput target) throws IOException {
                target.writeCollection(e.types, DataOutput::writeUTF);
            }

            private void read00(RevisionDataInput source, SchemaTypesListValue.SchemaTypesListValueBuilder b) throws IOException {
                b.types(Lists.newArrayList(source.readCollection(DataInput::readUTF)));
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class EncodingInfoRecord implements TableKey, TableValue {
        public static final Serializer SERIALIZER = new Serializer();

        private final VersionInfo versionInfo;
        private final String codecType;

        @SneakyThrows
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class EncodingInfoRecordBuilder implements ObjectBuilder<EncodingInfoRecord> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<EncodingInfoRecord, EncodingInfoRecord.EncodingInfoRecordBuilder> {
            @Override
            protected EncodingInfoRecord.EncodingInfoRecordBuilder newBuilder() {
                return EncodingInfoRecord.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(EncodingInfoRecord e, RevisionDataOutput target) throws IOException {
                VersionInfoSerializer.SERIALIZER.serialize(target, e.versionInfo);
                target.writeUTF(e.codecType);
            }

            private void read00(RevisionDataInput source, EncodingInfoRecord.EncodingInfoRecordBuilder b) throws IOException {
                b.versionInfo(VersionInfoSerializer.SERIALIZER.deserialize(source))
                 .codecType(source.readUTF());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class EncodingIdRecord implements TableKey, TableValue {
        public static final Serializer SERIALIZER = new Serializer();

        private final EncodingId encodingId;

        @SneakyThrows
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class EncodingIdRecordBuilder implements ObjectBuilder<EncodingIdRecord> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<EncodingIdRecord, EncodingIdRecord.EncodingIdRecordBuilder> {
            @Override
            protected EncodingIdRecord.EncodingIdRecordBuilder newBuilder() {
                return EncodingIdRecord.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(EncodingIdRecord e, RevisionDataOutput target) throws IOException {
                EncodingIdSerializer.SERIALIZER.serialize(target, e.encodingId);
            }

            private void read00(RevisionDataInput source, EncodingIdRecord.EncodingIdRecordBuilder b) throws IOException {
                b.encodingId(EncodingIdSerializer.SERIALIZER.deserialize(source));
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class LatestSchemaVersionKey implements TableKey {
        public static final Serializer SERIALIZER = new Serializer();

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
            }

            private void read00(RevisionDataInput source, LatestSchemaVersionKey.LatestSchemaVersionKeyBuilder b) throws IOException {
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class LatestSchemaVersionForTypeKey implements TableKey {
        public static final Serializer SERIALIZER = new Serializer();

        private final String type;

        private static class LatestSchemaVersionForTypeKeyBuilder implements ObjectBuilder<LatestSchemaVersionForTypeKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<LatestSchemaVersionForTypeKey, LatestSchemaVersionForTypeKey.LatestSchemaVersionForTypeKeyBuilder> {
            @Override
            protected LatestSchemaVersionForTypeKey.LatestSchemaVersionForTypeKeyBuilder newBuilder() {
                return LatestSchemaVersionForTypeKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(LatestSchemaVersionForTypeKey e, RevisionDataOutput target) throws IOException {
                target.writeUTF(e.type);
            }

            private void read00(RevisionDataInput source, LatestSchemaVersionForTypeKey.LatestSchemaVersionForTypeKeyBuilder b) throws IOException {
                b.type(source.readUTF());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class LatestSchemaVersionValue implements TableValue {
        public static final Serializer SERIALIZER = new Serializer();

        private final VersionInfo version;
        
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
                VersionInfoSerializer.SERIALIZER.serialize(target, e.version);
            }

            private void read00(RevisionDataInput source, LatestSchemaVersionValue.LatestSchemaVersionValueBuilder b) throws IOException {
                b.version(VersionInfoSerializer.SERIALIZER.deserialize(source));
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class LatestEncodingIdKey implements TableKey {
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
    class LatestEncodingIdValue implements TableValue {
        public static final Serializer SERIALIZER = new Serializer();

        private final EncodingId encodingId;

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
                EncodingIdSerializer.SERIALIZER.serialize(target, e.encodingId);
            }

            private void read00(RevisionDataInput source, LatestEncodingIdValue.LatestEncodingIdValueBuilder b) throws IOException {
                b.encodingId(EncodingIdSerializer.SERIALIZER.deserialize(source));
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class VersionOrdinalValue implements TableValue {
        public static final Serializer SERIALIZER = new Serializer();

        private final int ordinal;

        @SneakyThrows
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class VersionOrdinalValueBuilder implements ObjectBuilder<VersionOrdinalValue> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<VersionOrdinalValue, VersionOrdinalValue.VersionOrdinalValueBuilder> {
            @Override
            protected VersionOrdinalValue.VersionOrdinalValueBuilder newBuilder() {
                return VersionOrdinalValue.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(VersionOrdinalValue e, RevisionDataOutput target) throws IOException {
                target.writeInt(e.ordinal);
            }

            private void read00(RevisionDataInput source, VersionOrdinalValue.VersionOrdinalValueBuilder b) throws IOException {
                b.ordinal(source.readInt());
            }
        }
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static <T extends TableValue> T fromBytes(Class<? extends TableKey> keyClass, byte[] bytes, Class<T> valueClass) {
        return (T) SERIALIZERS_BY_KEY_TYPE.get(keyClass).deserialize(bytes);
    }
}
