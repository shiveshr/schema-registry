/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.contract.transform;

import com.google.common.collect.ImmutableMap;
import io.pravega.schemaregistry.contract.data.SchemaEvolution;
import io.pravega.schemaregistry.contract.generated.rest.model.CompressionType;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingId;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaType;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaValidationRules;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaValidationRule;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaVersionAndRules;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.Compatibility;
import org.apache.commons.lang3.NotImplementedException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Provides translation (encode/decode) between the Model classes and its REST representation.
 */
public class ModelHelper {

    // region decode
    public static io.pravega.schemaregistry.contract.data.SchemaInfo decode(SchemaInfo schemaInfo) {
        io.pravega.schemaregistry.contract.data.SchemaType schemaType = decode(schemaInfo.getSchemaType());
        return new io.pravega.schemaregistry.contract.data.SchemaInfo(schemaInfo.getSchemaName(), schemaType, schemaInfo.getSchemaData(),
                ImmutableMap.copyOf(schemaInfo.getProperties()));
    }

    public static io.pravega.schemaregistry.contract.data.SchemaType decode(SchemaType schemaType) {
        switch (schemaType.getSchemaType()) {
            case CUSTOM:
                return io.pravega.schemaregistry.contract.data.SchemaType.custom(schemaType.getCustomTypeName());
            default:
                return searchEnum(io.pravega.schemaregistry.contract.data.SchemaType.class, schemaType.getSchemaType().name());
        }
    }

    public static io.pravega.schemaregistry.contract.data.SchemaValidationRules decode(SchemaValidationRules rules) {
        List<io.pravega.schemaregistry.contract.data.SchemaValidationRule> list = rules.getRules().entrySet().stream().map(rule -> {
            io.pravega.schemaregistry.contract.data.SchemaValidationRule schemaValidationRule = null;
            if (rule.getValue().getRule() instanceof LinkedHashMap) {
                String name = (String) ((LinkedHashMap) rule.getValue().getRule()).get("name");
                if (name.equals(Compatibility.class.getSimpleName())) {
                    String policy = (String) ((LinkedHashMap) rule.getValue().getRule()).get("policy");
                    io.pravega.schemaregistry.contract.data.Compatibility.Type policyType = searchEnum(io.pravega.schemaregistry.contract.data.Compatibility.Type.class, policy);
                    switch (policyType) {
                        case AllowAny:
                            schemaValidationRule = io.pravega.schemaregistry.contract.data.Compatibility.allowAny();
                            break;
                        case DenyAll:
                            schemaValidationRule = io.pravega.schemaregistry.contract.data.Compatibility.denyAll();
                            break;
                        case Backward:
                            schemaValidationRule = io.pravega.schemaregistry.contract.data.Compatibility.backward();
                            break;
                        case BackwardTransitive:
                            schemaValidationRule = io.pravega.schemaregistry.contract.data.Compatibility.backwardTransitive();
                            break;
                        case Forward:
                            schemaValidationRule = io.pravega.schemaregistry.contract.data.Compatibility.forward();
                            break;
                        case ForwardTransitive:
                            schemaValidationRule = io.pravega.schemaregistry.contract.data.Compatibility.forwardTransitive();
                            break;
                        case BackwardTill:
                            LinkedHashMap backwardTill = (LinkedHashMap) ((LinkedHashMap) rule.getValue().getRule()).get("backwardTill");
                            schemaValidationRule = io.pravega.schemaregistry.contract.data.Compatibility.backwardTill(getVersionInfo(backwardTill));
                            break;
                        case ForwardTill:
                            LinkedHashMap forwardTill = (LinkedHashMap) ((LinkedHashMap) rule.getValue().getRule()).get("forwardTill");
                            schemaValidationRule = io.pravega.schemaregistry.contract.data.Compatibility.forwardTill(getVersionInfo(forwardTill));
                            break;
                        case BackwardAndForwardTill:
                            LinkedHashMap backwardTill2 = (LinkedHashMap) ((LinkedHashMap) rule.getValue().getRule()).get("backwardTill");
                            LinkedHashMap forwardTill2 = (LinkedHashMap) ((LinkedHashMap) rule.getValue().getRule()).get("forwardTill");
                            schemaValidationRule = io.pravega.schemaregistry.contract.data.Compatibility.backwardTillAndForwardTill(getVersionInfo(backwardTill2), getVersionInfo(forwardTill2));
                            break;
                            default:
                                throw new IllegalArgumentException();
                    }
                    return schemaValidationRule;
                } else {
                    throw new NotImplementedException("Rule not implemented");
                }
            } else {
                throw new IllegalArgumentException("Rule not supported");
            }
        }).collect(Collectors.toList());
        return io.pravega.schemaregistry.contract.data.SchemaValidationRules.of(list);
    }
    
    public static io.pravega.schemaregistry.contract.data.Compatibility decode(Compatibility compatibility) {
        io.pravega.schemaregistry.contract.data.VersionInfo backwardTill = compatibility.getBackwardTill() == null ? null : decode(compatibility.getBackwardTill());
        io.pravega.schemaregistry.contract.data.VersionInfo forwardTill = compatibility.getForwardTill() == null ? null : decode(compatibility.getForwardTill());
        return new io.pravega.schemaregistry.contract.data.Compatibility(
                searchEnum(io.pravega.schemaregistry.contract.data.Compatibility.Type.class, compatibility.getPolicy().name()),
                backwardTill, forwardTill);
    }

    public static io.pravega.schemaregistry.contract.data.CompressionType decode(CompressionType compressionType) {
        switch (compressionType.getCompressionType()) {
            case CUSTOM:
                return io.pravega.schemaregistry.contract.data.CompressionType.custom(compressionType.getCustomTypeName());
            default:
                return searchEnum(
                        io.pravega.schemaregistry.contract.data.CompressionType.class, compressionType.getCompressionType().name());
        }
    }

    public static io.pravega.schemaregistry.contract.data.VersionInfo decode(VersionInfo versionInfo) {
        return new io.pravega.schemaregistry.contract.data.VersionInfo(versionInfo.getSchemaName(), versionInfo.getVersion());
    }

    public static io.pravega.schemaregistry.contract.data.EncodingInfo decode(EncodingInfo encodingInfo) {
        return new io.pravega.schemaregistry.contract.data.EncodingInfo(decode(encodingInfo.getVersionInfo()), 
                decode(encodingInfo.getSchemaInfo()), decode(encodingInfo.getCompressionType()));
    }

    public static <T> io.pravega.schemaregistry.contract.data.SchemaWithVersion decode(SchemaWithVersion schemaWithVersion) {
        return new io.pravega.schemaregistry.contract.data.SchemaWithVersion(decode(schemaWithVersion.getSchemaInfo()),
                decode(schemaWithVersion.getVersion()));
    }

    public static SchemaEvolution decode(SchemaVersionAndRules schemaEvolution) {
        return new io.pravega.schemaregistry.contract.data.SchemaEvolution(decode(schemaEvolution.getSchemaInfo()),
                decode(schemaEvolution.getVersion()), decode(schemaEvolution.getValidationRules()));
    }

    public static io.pravega.schemaregistry.contract.data.EncodingId decode(EncodingId encodingId) {
        return new io.pravega.schemaregistry.contract.data.EncodingId(encodingId.getEncodingId());
    }

    public static io.pravega.schemaregistry.contract.data.GroupProperties decode(GroupProperties groupProperties) {
        return new io.pravega.schemaregistry.contract.data.GroupProperties(decode(groupProperties.getSchemaType()),
                decode(groupProperties.getSchemaValidationRules()), groupProperties.isValidateByObjectType(),
                groupProperties.getProperties());
    }
    // endregion

    // region encode
    public static SchemaVersionAndRules encode(io.pravega.schemaregistry.contract.data.SchemaEvolution schemaEvolution) {
        SchemaInfo encode = encode(schemaEvolution.getSchema());
        return new SchemaVersionAndRules().schemaInfo(encode)
                                         .version(encode(schemaEvolution.getVersion())).validationRules(encode(schemaEvolution.getRules()));
    }

    public static SchemaValidationRules encode(io.pravega.schemaregistry.contract.data.SchemaValidationRules rules) {
        Map<String, SchemaValidationRule> map = rules.getRules().entrySet().stream().collect(Collectors.toMap(rule -> {
            if (rule.getValue() instanceof io.pravega.schemaregistry.contract.data.Compatibility) {
                return io.pravega.schemaregistry.contract.generated.rest.model.Compatibility.class.getSimpleName();
            } else {
                throw new NotImplementedException("Rule not implemented");
            }
        }, rule -> {
            SchemaValidationRule schemaValidationRule;
            if (rule.getValue() instanceof io.pravega.schemaregistry.contract.data.Compatibility) {
                schemaValidationRule = new SchemaValidationRule().rule(encode((io.pravega.schemaregistry.contract.data.Compatibility) rule.getValue()));
            } else {
                throw new NotImplementedException("Rule not implemented");
            }
            return schemaValidationRule;
        }));
        return new SchemaValidationRules().rules(map);
    }

    public static Compatibility encode(io.pravega.schemaregistry.contract.data.Compatibility compatibility) {
        Compatibility policy = new io.pravega.schemaregistry.contract.generated.rest.model.Compatibility()
                .name(compatibility.getName())
                .policy(searchEnum(Compatibility.PolicyEnum.class, compatibility.getCompatibility().name()));
        if (compatibility.getBackwardTill() != null) {
            VersionInfo backwardTill = encode(compatibility.getBackwardTill());
            policy = policy.backwardTill(backwardTill);
        }
        if (compatibility.getForwardTill() != null) {
            VersionInfo forwardTill = encode(compatibility.getForwardTill());
            policy = policy.forwardTill(forwardTill);
        }
        return policy;
    }

    public static SchemaWithVersion encode(io.pravega.schemaregistry.contract.data.SchemaWithVersion schemaWithVersion) {
        return new SchemaWithVersion().schemaInfo(encode(schemaWithVersion.getSchema()))
                                           .version(encode(schemaWithVersion.getVersion()));
    }

    public static GroupProperties encode(io.pravega.schemaregistry.contract.data.GroupProperties groupProperties) {
        return new GroupProperties()
                .schemaType(encode(groupProperties.getSchemaType()))
                .properties(groupProperties.getProperties())
                .validateByObjectType(groupProperties.isValidateByObjectType())
                .schemaValidationRules(encode(groupProperties.getSchemaValidationRules()));
    }

    public static GroupProperties encode(String groupName, io.pravega.schemaregistry.contract.data.GroupProperties groupProperties) {
        return encode(groupProperties).groupName(groupName);
    }

    public static VersionInfo encode(io.pravega.schemaregistry.contract.data.VersionInfo versionInfo) {
        return new VersionInfo().schemaName(versionInfo.getSchemaName()).version(versionInfo.getVersion());
    }

    public static SchemaInfo encode(io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo) {
        return new SchemaInfo().properties(schemaInfo.getProperties()).schemaData(schemaInfo.getSchemaData())
                                    .schemaName(schemaInfo.getName()).schemaType(encode(schemaInfo.getSchemaType()));
    }

    public static SchemaType encode(io.pravega.schemaregistry.contract.data.SchemaType schemaType) {
        if (schemaType.equals(io.pravega.schemaregistry.contract.data.SchemaType.Custom)) {
            SchemaType schemaTypeModel = new SchemaType().schemaType(SchemaType.SchemaTypeEnum.CUSTOM);
            return schemaTypeModel.customTypeName(schemaType.getCustomTypeName());
        } else {
            return new SchemaType().schemaType(
                    searchEnum(SchemaType.SchemaTypeEnum.class, schemaType.name()));
        }
    }

    public static EncodingId encode(io.pravega.schemaregistry.contract.data.EncodingId encodingId) {
        return new EncodingId().encodingId(encodingId.getId());
    }

    public static CompressionType encode(io.pravega.schemaregistry.contract.data.CompressionType compression) {
        if (compression.equals(io.pravega.schemaregistry.contract.data.CompressionType.Custom)) {
            return new CompressionType().compressionType(CompressionType.CompressionTypeEnum.CUSTOM)
                                             .customTypeName(compression.getCustomTypeName());
        } else {
            return new CompressionType().compressionType(
                    searchEnum(CompressionType.CompressionTypeEnum.class, compression.name()));
        }
    }

    public static EncodingInfo encode(io.pravega.schemaregistry.contract.data.EncodingInfo encodingInfo) {
        return new EncodingInfo().compressionType(encode(encodingInfo.getCompression()))
                                      .versionInfo(encode(encodingInfo.getVersionInfo()))
                                      .schemaInfo(encode(encodingInfo.getSchemaInfo()));
    }

    // endregion

    private static <T extends Enum<?>> T searchEnum(Class<T> enumeration, String search) {
        for (T each : enumeration.getEnumConstants()) {
            if (each.name().compareToIgnoreCase(search) == 0) {
                return each;
            }
        }
        throw new IllegalArgumentException();
    }

    private static io.pravega.schemaregistry.contract.data.VersionInfo getVersionInfo(LinkedHashMap backwardTill) {
        String schemaName = (String) backwardTill.get("schemaName");
        int version = (int) backwardTill.get("version");

        return new io.pravega.schemaregistry.contract.data.VersionInfo(schemaName, version);
    }

}