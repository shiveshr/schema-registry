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

import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.schemaregistry.contract.data.Application;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Object that captures the version of a schema within a group.
 * It contains schema name matching {@link SchemaInfo#name} along with the registry assigned version for the schema in
 * the group. 
 */
@AllArgsConstructor
public class WriterSerializer extends VersionedSerializer.WithBuilder<Application.Writer, Application.Writer.WriterBuilder> {
    static final WriterSerializer SERIALIZER = new WriterSerializer();
    
    @Override
    protected Application.Writer.WriterBuilder newBuilder() {
        return Application.Writer.builder();
    }

    @Override
    protected byte getWriteVersion() {
        return 0;
    }

    @Override
    protected void declareVersions() {
        version(0).revision(0, this::write00, this::read00);
    }

    private void write00(Application.Writer e, RevisionDataOutput target) throws IOException {
        target.writeCollection(e.getVersionInfos(), VersionInfoSerializer.SERIALIZER::serialize);
        CodecTypeRecord.SERIALIZER.serialize(target, new CodecTypeRecord(e.getCodecType()));
    }

    private void read00(RevisionDataInput source, Application.Writer.WriterBuilder b) throws IOException {
        b.versionInfos(new ArrayList<>(source.readCollection(VersionInfoSerializer.SERIALIZER::deserialize)))
            .codecType(CodecTypeRecord.SERIALIZER.deserialize(source).getCodecType());
    }
}
