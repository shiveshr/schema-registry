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
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.SchemaValidationRule;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import lombok.Builder;
import lombok.Data;

import java.io.IOException;

@Data
@Builder
public class CompatibilityRecord implements SchemaValidationRuleRecord {
    public static final Serializer SERIALIZER = new Serializer();

    private final Compatibility compatibility;
    
    public CompatibilityRecord(Compatibility compatibility) {
        this.compatibility = compatibility;
    }

    private static class CompatibilityRecordBuilder implements ObjectBuilder<CompatibilityRecord> {
    }

    static class Serializer extends VersionedSerializer.WithBuilder<CompatibilityRecord, CompatibilityRecord.CompatibilityRecordBuilder> {
        @Override
        protected CompatibilityRecord.CompatibilityRecordBuilder newBuilder() {
            return CompatibilityRecord.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(CompatibilityRecord e, RevisionDataOutput target) throws IOException {
            target.writeCompactInt(e.getCompatibility().getCompatibility().ordinal());
            if (e.getCompatibility().getBackwardTill() != null) {
                VersionInfoRecord.SERIALIZER.serialize(target, new VersionInfoRecord(e.getCompatibility().getBackwardTill()));
            } else {
                VersionInfoRecord.SERIALIZER.serialize(target, VersionInfoRecord.NON_EXISTENT);
            }
            if (e.getCompatibility().getForwardTill() != null) {
                VersionInfoRecord.SERIALIZER.serialize(target, new VersionInfoRecord(e.getCompatibility().getForwardTill()));
            } else {
                VersionInfoRecord.SERIALIZER.serialize(target, VersionInfoRecord.NON_EXISTENT);
            }
        }

        private void read00(RevisionDataInput source, CompatibilityRecord.CompatibilityRecordBuilder b) throws IOException {
            int ordinal = source.readCompactInt();
            Compatibility.Type compatibilityType = Compatibility.Type.values()[ordinal];
            VersionInfoRecord backwardTillRecord = VersionInfoRecord.SERIALIZER.deserialize(source);
            VersionInfo backwardTill = backwardTillRecord.equals(VersionInfoRecord.NON_EXISTENT) ? null : backwardTillRecord.getVersionInfo();
            VersionInfoRecord forwardTillRecord = VersionInfoRecord.SERIALIZER.deserialize(source);
            VersionInfo forwardTill = forwardTillRecord.equals(VersionInfoRecord.NON_EXISTENT) ? null : forwardTillRecord.getVersionInfo();
            b.compatibility(new Compatibility(compatibilityType, backwardTill, forwardTill));
        }
    }
}
