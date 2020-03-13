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
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.IOException;

/**
 * Encoding Id generated by registry service for each unique combination of schema version and compression type. 
 * The encoding id will typically be attached to the encoded data in a header to describe how to parse the following data. 
 * The registry service exposes APIs to resolve encoding id to {@link EncodingInfo} objects that include details about the
 * encoding used. 
 */
@Data
@Builder
@AllArgsConstructor
public class EncodingIdRecord {
    public static final Serializer SERIALIZER = new Serializer();

    private final EncodingId encodingId;

    private static class EncodingIdRecordBuilder implements ObjectBuilder<EncodingIdRecord> {
    }

    public static class Serializer extends VersionedSerializer.WithBuilder<EncodingIdRecord, EncodingIdRecord.EncodingIdRecordBuilder> {
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
            target.writeInt(e.getEncodingId().getId());
        }

        private void read00(RevisionDataInput source, EncodingIdRecord.EncodingIdRecordBuilder b) throws IOException {
            b.encodingId(new EncodingId(source.readInt()));
        }
    }
}