/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public interface ApplicationRecord {
    @Data
    @Builder
    @AllArgsConstructor
    class ApplicationValue {
        public static final ApplicationValue.Serializer SERIALIZER = new ApplicationValue.Serializer();

        /**
         * writer id -> group id
         */
        private final Map<String, String> writingTo;
        /**
         * reader id -> group id
         */
        private final Map<String, String> readingFrom;
        private final Map<String, String> properties;
        
        @SneakyThrows
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }
        
        @SneakyThrows
        public static ApplicationValue fromBytes(byte[] bytes) {
            return SERIALIZER.deserialize(bytes);
        }

        private static class ApplicationValueBuilder implements ObjectBuilder<ApplicationValue> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<ApplicationValue, ApplicationValue.ApplicationValueBuilder> {
            @Override
            protected ApplicationValue.ApplicationValueBuilder newBuilder() {
                return ApplicationValue.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(ApplicationValue e, RevisionDataOutput target) throws IOException {
                target.writeMap(e.writingTo, DataOutput::writeUTF, DataOutput::writeUTF);
                target.writeMap(e.readingFrom, DataOutput::writeUTF, DataOutput::writeUTF);
                target.writeMap(e.properties, DataOutput::writeUTF, DataOutput::writeUTF);
            }

            private void read00(RevisionDataInput source, ApplicationValue.ApplicationValueBuilder b) throws IOException {
                b.writingTo(source.readMap(DataInput::readUTF, DataInput::readUTF));
                b.readingFrom(source.readMap(DataInput::readUTF, DataInput::readUTF));
                b.properties(source.readMap(DataInput::readUTF, DataInput::readUTF));
            }
        }
    }
}
