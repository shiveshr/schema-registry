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

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.IOException;

/**
 * Object that captures the version of a schema within a group.
 * It contains schema name matching {@link SchemaInfo#name} along with the registry assigned version for the schema in
 * the group. 
 */
@Data
@Builder
@AllArgsConstructor
public class VersionInfoRecord {
    public static final Serializer SERIALIZER = new Serializer();
    public static final VersionInfoRecord NON_EXISTENT = new VersionInfoRecord(new VersionInfo("", -1));

    private final VersionInfo versionInfo;

    public static VersionInfoRecord fromBytes(byte[] bytes) throws IOException {
        return SERIALIZER.deserialize(bytes);
    }
    
    public byte[] toBytes() throws IOException {
        return SERIALIZER.serialize(this).getCopy();
    }
    
    private static class VersionInfoRecordBuilder implements ObjectBuilder<VersionInfoRecord> {
    }

    public static class Serializer extends VersionedSerializer.WithBuilder<VersionInfoRecord, VersionInfoRecord.VersionInfoRecordBuilder> {
        @Override
        protected VersionInfoRecord.VersionInfoRecordBuilder newBuilder() {
            return VersionInfoRecord.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(VersionInfoRecord e, RevisionDataOutput target) throws IOException {
            target.writeUTF(e.getVersionInfo().getSchemaName());
            target.writeInt(e.getVersionInfo().getVersion());
        }

        private void read00(RevisionDataInput source, VersionInfoRecord.VersionInfoRecordBuilder b) throws IOException {
            String schemaName = source.readUTF();
            int version = source.readInt();
            b.versionInfo(new VersionInfo(schemaName, version));
        }
    }
}
