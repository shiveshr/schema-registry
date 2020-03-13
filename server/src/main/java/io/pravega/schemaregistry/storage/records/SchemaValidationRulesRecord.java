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
import io.pravega.schemaregistry.contract.data.SchemaValidationRule;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Schema validation rules that are applied for checking if a schema is valid. 
 * This contains a set of rules. The schema will be compared against one or more existing schemas in the group by applying the rule. 
 */
@Data
@Builder
public class SchemaValidationRulesRecord {
    public static final Serializer SERIALIZER = new Serializer();

    private final Map<String, SchemaValidationRuleRecord> rules;

    private SchemaValidationRulesRecord(Map<String, SchemaValidationRuleRecord> rules) {
        this.rules = rules;
    }
    
    public static SchemaValidationRulesRecord of(List<SchemaValidationRuleRecord> rules) {
        return new SchemaValidationRulesRecord(rules.stream().collect(Collectors.toMap(x -> x.getSchemaValidationRule().getName(), 
                x -> x)));
    }
    
    private static class SchemaValidationRulesRecordBuilder implements ObjectBuilder<SchemaValidationRulesRecord> {
    }

    public static class Serializer extends VersionedSerializer.WithBuilder<SchemaValidationRulesRecord, SchemaValidationRulesRecord.SchemaValidationRulesRecordBuilder> {
        @Override
        protected SchemaValidationRulesRecord.SchemaValidationRulesRecordBuilder newBuilder() {
            return SchemaValidationRulesRecord.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        @SneakyThrows(IOException.class)
        private void write00(SchemaValidationRulesRecord e, RevisionDataOutput target) throws IOException {
            target.writeCompactInt(e.getRules().size());
            for (Map.Entry<String, SchemaValidationRuleRecord> rule : e.getRules().entrySet()) {
                target.writeUTF(rule.getKey());
                target.writeArray(SchemaValidationRuleRecord.SERIALIZER.toBytes(rule.getValue()));                
            }
        }

        @SneakyThrows(IOException.class)
        private void read00(RevisionDataInput source, SchemaValidationRulesRecord.SchemaValidationRulesRecordBuilder b) throws IOException {
            int count = source.readCompactInt();
            Map<String, SchemaValidationRuleRecord> rules = new HashMap<>();
            for (int i = 0; i < count; i++) {
                String name = source.readUTF();
                byte[] bytes = source.readArray();
                rules.put(name, SchemaValidationRuleRecord.SERIALIZER.fromBytes(bytes));
            }
            b.rules(rules);
        }
    }
}
