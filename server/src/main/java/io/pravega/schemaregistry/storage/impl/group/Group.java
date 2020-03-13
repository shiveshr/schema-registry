/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl.group;

import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import io.pravega.schemaregistry.ListWithToken;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaEvolution;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.storage.Position;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.records.CompatibilityRecord;
import io.pravega.schemaregistry.storage.records.SchemaValidationRulesRecord;
import io.pravega.schemaregistry.storage.records.TableRecords;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Class that implements all storage logic for a group. 
 * It makes use of two storage primitives, namely an append only write ahead log {@link Log} and an table {@link Table}.
 * The group's state is written into the log and then the table is updated for optimizing the queries. 
 * The source of truth is the log and the table may eventually catches up with the log. 
 * @param <V> Type of version used in the table. 
 */
public class Group<V> {
    private static final TableRecords.ValidationRulesKey VALIDATION_POLICY_KEY = new TableRecords.ValidationRulesKey();
    private static final TableRecords.GroupPropertyKey GROUP_PROPERTY_KEY = new TableRecords.GroupPropertyKey();
    private static final TableRecords.Etag ETAG_KEY = new TableRecords.Etag();
    
    private static final Comparator<Table.Entry> VERSION_COMPARATOR = (v1, v2) -> {
        TableRecords.VersionInfoKey version1 = (TableRecords.VersionInfoKey) v1.getKey();
        TableRecords.VersionInfoKey version2 = (TableRecords.VersionInfoKey) v2.getKey();
        return Integer.compare(version1.getVersion().getVersion(), version2.getVersion().getVersion());
    };
    private static final Comparator<Table.Entry> ENCODING_ID_COMPARATOR = (v1, v2) -> {
        TableRecords.EncodingId id1 = (TableRecords.EncodingId) v1.getKey();
        TableRecords.EncodingId id2 = (TableRecords.EncodingId) v2.getKey();
        return Integer.compare(id1.getEncodingId().getId(), id2.getEncodingId().getId());
    };
    private static final HashFunction HASH = Hashing.murmur3_128();
    public static final TableRecords.LatestEncodingIdKey LATEST_ENCODING_ID_KEY = new TableRecords.LatestEncodingIdKey();

    private final Table<V> table;
    
    private final ScheduledExecutorService executor;

    public Group(Table<V> table, ScheduledExecutorService executor) {
        this.table = table;
        this.executor = executor;
    }
    
    public CompletableFuture<Void> create(SchemaType schemaType, Map<String, String> properties, boolean validateByObjectType, SchemaValidationRules schemaValidationRules) {
        return getCurrentEtag()
                  .thenCompose(etag -> {
                      TableRecords.GroupPropertiesValue groupPropertiesRecord = new TableRecords.GroupPropertiesValue(schemaType, validateByObjectType, properties);
                      TableRecords.ValidationRulesValue validationRulesRecord = new TableRecords.ValidationRulesValue(SchemaValidationRulesRecord.of(
                              schemaValidationRules.getRules().entrySet().stream().map(x -> {
                                  if (x.getValue() instanceof Compatibility) {
                                      return new CompatibilityRecord((Compatibility) x.getValue());
                                  } else {
                                      throw new IllegalArgumentException();
                                  }
                              } ).collect(Collectors.toList())));
                      Operation.Add addGroupProp = new Operation.Add(GROUP_PROPERTY_KEY, groupPropertiesRecord);
                      Operation.Add addValidationRules = new Operation.Add(VALIDATION_POLICY_KEY, validationRulesRecord);
                      Operation.Add addEtag = new Operation.Add(ETAG_KEY, ETAG_KEY);
                      return updateTable(Lists.newArrayList(addGroupProp, addValidationRules, addEtag));
                  });
    }
    
    @SuppressWarnings("unchecked")
    public CompletableFuture<Position> getCurrentEtag() {
        return table.getRecordWithVersion(ETAG_KEY, TableRecords.Etag.class)
            .thenApply(record -> table.versionToPosition(record.getVersion()));
    }
    
    public CompletableFuture<ListWithToken<String>> getObjectTypes() {
        // get all versions and extract their object types
        return table.getRecord(new TableRecords.LatestSchemaVersionKey())
                    .thenApply(list -> {
                         List<String> objectTypes = list
                                 .stream().filter(x -> x instanceof TableRecords.VersionInfoKey)
                                 .map(x -> ((TableRecords.VersionInfoKey) x).getVersionInfo().getSchemaName())
                                 .distinct().collect(Collectors.toList());
                         return new ListWithToken<>(objectTypes, null);
                     });
    }

    public CompletableFuture<ListWithToken<SchemaWithVersion>> getSchemas() {
        // 1. get latest encoding id
        // 2. get all encoding infos corresponding to all previous encoding ids
        return table.getRecord(LATEST_ENCODING_ID_KEY, TableRecords.LatestEncodingIdValue.class)
                    .thenCompose(latest -> {

                        List<TableRecords.Key> keys = new LinkedList<>();
                        for (int i = 0; i < latest.getId(); i++) {
                            keys.add(new TableRecords.EncodingId(new EncodingId(i)));
                        }
                        return table.getRecords(keys, TableRecords.EncodingInfo.class);
                    }).thenApply(list -> list.stream().map(TableRecords.EncodingInfo::getCompressionType)
                                             .distinct().collect(Collectors.toList()));
    }
    
    public CompletableFuture<ListWithToken<SchemaWithVersion>> getSchemas(VersionInfo from) {
        TableRecords.VersionInfoKey versionInfoKey = new TableRecords.VersionInfoKey(from);
        return table.getRecord(versionInfoKey, TableRecords.WALPositionValue.class)
                    .thenCompose(fromPos -> getSchemasInternal(fromPos.getPosition(),
                            x -> x.getRecord() instanceof Record.SchemaRecord));
    }

    public CompletableFuture<ListWithToken<SchemaWithVersion>> getSchemas(String objectTypeName) {
        return getSchemasInternal(null, 
                x -> x.getRecord() instanceof Record.SchemaRecord && 
                        ((Record.SchemaRecord) x.getRecord()).getSchemaInfo().getName().equals(objectTypeName));
    }

    public CompletableFuture<ListWithToken<SchemaWithVersion>> getSchemas(String objectTypeName, VersionInfo from) {
        TableRecords.VersionInfoKey versionInfoKey = new TableRecords.VersionInfoKey(from);
        return table.getRecord(versionInfoKey, TableRecords.WALPositionValue.class)
                    .thenCompose(fromPos -> getSchemasInternal(fromPos.getPosition(),
                            x -> x.getRecord() instanceof Record.SchemaRecord &&
                                    ((Record.SchemaRecord) x.getRecord()).getSchemaInfo().getName().equals(objectTypeName)));
    }
    
    public CompletableFuture<SchemaInfo> getSchema(VersionInfo versionInfo) {
        return table.getRecord(new TableRecords.VersionInfoKey(versionInfo), TableRecords.SchemaInfoValue.class)
                    .thenApply(TableRecords.SchemaInfoValue::getSchemaInfo);
    }
    
    public CompletableFuture<VersionInfo> getVersion(SchemaInfo schemaInfo) {
        long fingerPrint = getFingerprint(schemaInfo);
        return getVersionInternal(schemaInfo, fingerPrint)
                .thenApply(version -> {
                    if (version == null) {
                        throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, String.format("Schema=%s", fingerPrint));
                    } else {
                        return version;
                    }
                });
    }
    
    public CompletableFuture<io.pravega.schemaregistry.contract.data.EncodingId> getOrCreateEncodingId(VersionInfo versionInfo, CompressionType compressionType) {
        return getEncodingId(versionInfo, compressionType)
                .thenCompose(either -> {
                    if (either.isLeft()) {
                        return CompletableFuture.completedFuture(either.getLeft());
                    } else {
                        return generateNewEncodingId(versionInfo, compressionType, either.getRight());
                    }
                });
    }
    
    public CompletableFuture<EncodingInfo> getEncodingInfo(EncodingId encodingId) {
        TableRecords.EncodingId encodingIdIndex = new TableRecords.EncodingId(encodingId);
        return table.getRecord(encodingIdIndex, TableRecords.EncodingInfo.class)
                    .thenCompose(encodingInfo -> {
                        return getSchema(encodingInfo.getVersionInfo())
                                .thenApply(schemaInfo -> new EncodingInfo(encodingInfo.getVersionInfo(), 
                                        schemaInfo, encodingInfo.getCompressionType()))
                    });
    }

    public CompletableFuture<SchemaWithVersion> getLatestSchema() {
        Predicate<TableRecords.Key> versionPredicate = x -> x instanceof TableRecords.VersionInfoKey;

        return getLatestEntryFor(versionPredicate, VERSION_COMPARATOR)
                .thenCompose(max -> {
                    if (max != null) {
                        TableRecords.VersionInfoKey key = (TableRecords.VersionInfoKey) max.getKey();
                        VersionInfo version = key.getVersionInfo();
                        return getSchema(version).thenApply(schema -> new SchemaWithVersion(schema, version));
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    public CompletableFuture<SchemaWithVersion> getLatestSchema(String objectTypeName) {
        Predicate<TableRecords.Key> versionForObjectType = x -> x instanceof TableRecords.VersionInfoKey &&
                ((TableRecords.VersionInfoKey) x).getVersionInfo().getSchemaName().equals(objectTypeName);

        return getLatestEntryFor(versionForObjectType, VERSION_COMPARATOR)
                .thenCompose(entry -> {
                    if (entry == null) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        TableRecords.VersionInfoKey key = (TableRecords.VersionInfoKey) entry.getKey();
                        VersionInfo version = key.getVersionInfo();
                        return getSchema(version).thenApply(schema -> new SchemaWithVersion(schema, version));
                    }
                });
    }

    public CompletableFuture<List<CompressionType>> getCompressions() {
        // 1. get latest encoding id
        // 2. get all encoding infos corresponding to all previous encoding ids
        return table.getRecord(LATEST_ENCODING_ID_KEY, TableRecords.LatestEncodingIdValue.class)
             .thenCompose(latest -> {

                 List<TableRecords.Key> keys = new LinkedList<>();
                 for (int i = 0; i < latest.getEncodingId().getId(); i++) {
                     keys.add(new TableRecords.EncodingId(new EncodingId(i)));
                 }
                 return table.getRecords(keys, TableRecords.EncodingInfo.class); 
             }).thenApply(list -> list.stream().map(TableRecords.EncodingInfo::getCompressionType).distinct().collect(Collectors.toList()));
    }

    public CompletableFuture<ListWithToken<SchemaEvolution>> getHistory() {
        AtomicReference<SchemaValidationRules> rulesRef = new AtomicReference<>();
        List<SchemaEvolution> epochs = new LinkedList<>();
        return wal.readFrom(null)
                  .thenApply(list -> {
                      list.forEach(x -> {
                          if (x.getRecord() instanceof Record.SchemaRecord) {
                              Record.SchemaRecord record = (Record.SchemaRecord) x.getRecord();
                              assert rulesRef.get() != null;
                              SchemaEvolution epoch = new SchemaEvolution(record.getSchemaInfo(), record.getVersionInfo(),
                                      rulesRef.get());
                              epochs.add(epoch);
                          } else if (x.getRecord() instanceof Record.ValidationRecord) {
                              rulesRef.set(((Record.ValidationRecord) x.getRecord()).getValidationRules());
                          } else if (x.getRecord() instanceof Record.GroupPropertiesRecord) {
                              rulesRef.set(((Record.GroupPropertiesRecord) x.getRecord()).getValidationRules());
                          }
                      });
                      return new ListWithToken<>(epochs, null);
                  });
    }

    public CompletableFuture<ListWithToken<SchemaEvolution>> getHistory(String objectTypeName) {
        return getHistory().thenApply(list -> 
                new ListWithToken<>(list.getList().stream().filter(x -> x.getSchema().getName().equals(objectTypeName)).collect(Collectors.toList()), null));
    }

    public CompletableFuture<VersionInfo> addSchemaToGroup(SchemaInfo schemaInfo, VersionInfo versionInfo, Position etag) {
        return addSchema(schemaInfo, versionInfo, etag);
    }
    
    @SuppressWarnings("unchecked")
    public CompletableFuture<Void> updateValidationPolicy(SchemaValidationRules policy, Position etag) {
        return writeToLog(new Record.ValidationRecord(policy), etag)
                .thenCompose(v -> {
                    Operation.GetAndSet getAndSet = new Operation.GetAndSet(new TableRecords.ValidationRulesKey(), new TableRecords.WALPositionValue(etag),
                            x -> etag.getPosition().compareTo(((TableRecords.WALPositionValue) x).getPosition().getPosition()) > 0);
                    return updateTable(Collections.singletonList(getAndSet), new TableRecords.WALPositionValue(etag));
                });
    }

    public CompletableFuture<GroupProperties> getGroupProperties() {
        CompletableFuture<TableRecords.GroupPropertiesValue> grpPropertiesFuture = getGroupPropertiesRecord();

        CompletableFuture<SchemaValidationRules> rulesFuture = getCurrentValidationRules();

        return CompletableFuture.allOf(grpPropertiesFuture, rulesFuture)
                                .thenApply(x -> {
                                    TableRecords.GroupPropertiesValue properties = grpPropertiesFuture.join();
                                    SchemaValidationRules rules = rulesFuture.join();
                                    return new GroupProperties(properties.getSchemaType(), rules,
                                            properties.isValidateByObjectType(), properties.getProperties());
                                });
    }

    private long getFingerprint(SchemaInfo schemaInfo) {
        return HASH.hashBytes(schemaInfo.getSchemaData()).asLong();
    }

//    @SuppressWarnings("unchecked")
//    private CompletableFuture<Position> syncIndex() {
//        return table.getRecordWithVersion(SYNCD_TILL, TableRecords.WALPositionValue.class)
//                    .thenCompose(value -> {
//                        Position pos = value == null ? null : value.getValue().getPosition();
//                        return wal.readFrom(pos)
//                                  .thenCompose(list -> {
//                                      List<Operation> operations = new LinkedList<>();
//                                      list.forEach(x -> {
//                                          if (x.getRecord() instanceof Record.SchemaRecord) {
//                                              Record.SchemaRecord record = (Record.SchemaRecord) x.getRecord();
//                                              Operation.Add add = new Operation.Add(new TableRecords.VersionInfoKey(record.getVersionInfo()), new TableRecords.WALPositionValue(x.getPosition()));
//                                              Operation.AddToList addToList = new Operation.AddToList(new TableRecords.SchemaFingerprintKey(getFingerprint(record.getSchemaInfo())),
//                                                      new TableRecords.SchemaVersionValue(Collections.singletonList(record.getVersionInfo())));
//                                              operations.add(add);
//                                              operations.add(addToList);
//                                          } else if (x.getRecord() instanceof Record.EncodingRecord) {
//                                              Record.EncodingRecord record = (Record.EncodingRecord) x.getRecord();
//                                              TableRecords.EncodingId idIndex = new TableRecords.EncodingId(record.getEncodingId());
//                                              TableRecords.EncodingInfo infoIndex = new TableRecords.EncodingInfo(record.getVersionInfo(), record.getCompressionType());
//                                              Operation.Add idToInfo = new Operation.Add(idIndex, infoIndex);
//                                              Operation.Add infoToId = new Operation.Add(infoIndex, idIndex);
//                                              operations.add(idToInfo);
//                                              operations.add(infoToId);
//                                          } else if (x.getRecord() instanceof Record.ValidationRecord) {
//                                              Operation.GetAndSet getAndSet = new Operation.GetAndSet(new TableRecords.ValidationRulesKey(),
//                                                      new TableRecords.WALPositionValue(x.getPosition()),
//                                                      m -> ((TableRecords.WALPositionValue) m).getPosition().getPosition()
//                                                                                             .compareTo(x.getPosition().getPosition()) < 0);
//                                              operations.add(getAndSet);
//                                          }
//                                      });
//                                      if (!list.isEmpty()) {
//                                          Position syncdTill = list.get(list.size() - 1).getNext();
//                                          return updateTable(operations, new TableRecords.WALPositionValue(syncdTill))
//                                                  .thenApply(v -> syncdTill);
//                                      } else {
//                                          return CompletableFuture.completedFuture(pos);
//                                      }
//                                  });
//                    });
//    }
    
    private CompletableFuture<Void> updateTable(List<Operation> operations) {
        List<CompletableFuture<Void>> futures = new LinkedList<>();
        operations.forEach(operation -> {
            if (operation instanceof Operation.Add) {
                futures.add(table.addEntry(((Operation.Add) operation).getKey(), ((Operation.Add) operation).getValue()));
            } else if (operation instanceof Operation.GetAndSet) {
                Operation.GetAndSet op = (Operation.GetAndSet) operation;
                futures.add(Retry.withExpBackoff(1, 2, Integer.MAX_VALUE, 100)
                     .retryWhen(x -> Exceptions.unwrap(x) instanceof StoreExceptions.WriteConflictException)
                     .runAsync(() -> table.getRecordWithVersion(op.getKey(), TableRecords.Record.class)
                                          .thenCompose(existing -> {
                                 if (existing == null) {
                                     return table.addEntry(op.getKey(), op.getValue());
                                 } else if (op.getCondition().test(existing.getValue())) {
                                     return table.updateEntry(op.getKey(), op.getValue(), existing.getVersion());
                                 } else {
                                     return CompletableFuture.completedFuture(null);
                                 }
                             }), executor));
            } else if (operation instanceof Operation.AddToList) {
                Operation.AddToList op = (Operation.AddToList) operation;
                futures.add(Retry.withExpBackoff(1, 2, Integer.MAX_VALUE, 100)
                     .retryWhen(x -> Exceptions.unwrap(x) instanceof StoreExceptions.WriteConflictException)
                     .runAsync(() -> table.getRecordWithVersion(op.getKey(), TableRecords.Record.class)
                                          .thenCompose(existing -> {
                              if (existing == null) {
                                  return table.addEntry(op.getKey(), op.getValue());
                              } else if (op.getValue() instanceof TableRecords.SchemaVersionValue) {
                                      TableRecords.SchemaVersionValue existingList = (TableRecords.SchemaVersionValue) existing.getValue();
                                      TableRecords.SchemaVersionValue toAdd = (TableRecords.SchemaVersionValue) op.getValue();
                                      Set<VersionInfo> set = new HashSet<>(existingList.getVersions());
                                      set.addAll(toAdd.getVersions());

                                      TableRecords.SchemaVersionValue newValue = new TableRecords.SchemaVersionValue(new ArrayList<>(set));
                                      return table.updateEntry(op.getKey(), newValue, existing.getVersion());
                              } else {
                                  return CompletableFuture.completedFuture(null);
                              }
                          }), executor));
            }
        });
        
        return Futures.allOf(futures)
                .thenCompose(v -> table.getRecordWithVersion(SYNCD_TILL, TableRecords.WALPositionValue.class)
                                       .thenCompose(syncdTillWithVersion -> addOrUpdateSyncdTillKey(syncdTill, syncdTillWithVersion)));
    }

    @SuppressWarnings("unchecked")
//    private CompletableFuture<Void> addOrUpdateSyncdTillKey(TableRecords.WALPositionValue syncdTill,
//                                                            Table.Value<TableRecords.WALPositionValue, V> syncdTillWithVersion) {
//        if (syncdTillWithVersion == null) {
//            return table.addEntry(SYNCD_TILL, syncdTill);
//        } else {
//            Position newPos = syncdTill.getPosition();
//            Position existingPos = syncdTillWithVersion.getValue().getPosition();
//            if (newPos.getPosition().compareTo(existingPos.getPosition()) > 0) {
//                return table.updateEntry(SYNCD_TILL, syncdTill, syncdTillWithVersion.getVersion());
//            } else {
//                return CompletableFuture.completedFuture(null);
//            }
//        }
//    }
    
//    private CompletableFuture<Void> writeToLog(Record record, Position etag) {
//        return Futures.toVoid(wal.writeToLog(record, etag));
//    }

    private CompletableFuture<VersionInfo> addSchema(SchemaInfo schemaInfo, VersionInfo next, Position etag) {
        return writeToLog(new Record.SchemaRecord(schemaInfo, next), etag)
                            .thenCompose(v -> {
                                Operation.Add add = new Operation.Add(new TableRecords.VersionInfoKey(next), new TableRecords.WALPositionValue(etag));
                                Operation.AddToList addToList = new Operation.AddToList(new TableRecords.SchemaFingerprintKey(getFingerprint(schemaInfo)),
                                        new TableRecords.SchemaVersionValue(Collections.singletonList(next)));
                                return updateTable(Lists.newArrayList(add, addToList), new TableRecords.WALPositionValue(etag))
                                        .thenApply(x -> next);
                            });
    }

    public CompletableFuture<VersionInfo> getLatestVersion() {
        Predicate<TableRecords.Key> predicate = x -> x instanceof TableRecords.VersionInfoKey;

        return getLatestVersion(predicate);
    }
    
    public CompletableFuture<VersionInfo> getLatestVersion(String objectTypeName) {
        Predicate<TableRecords.Key> predicate = x -> x instanceof TableRecords.VersionInfoKey &&
                ((TableRecords.VersionInfoKey) x).getVersionInfo().getSchemaName().equals(objectTypeName);

        return getLatestVersion(predicate);
    }

    private CompletableFuture<VersionInfo> getLatestVersion(Predicate<TableRecords.Key> versionPredicate) {
        return getLatestEntryFor(versionPredicate, VERSION_COMPARATOR)
                .thenApply(entry -> {
                    if (entry == null) {
                        return null;
                    } else {
                        return ((TableRecords.VersionInfoKey) entry.getKey()).getVersionInfo();                        
                    }
                });
    }
    
    private CompletableFuture<Table.Entry> getLatestEntryFor(Predicate<TableRecords.Key> predicate, Comparator<Table.Entry> entryComparator) {
        return syncIndex()
                .thenCompose(v -> table.getAllEntries(predicate)
                                       .thenApply(entries -> entries.stream().max(entryComparator).orElse(null)));
    }

    private CompletableFuture<io.pravega.schemaregistry.contract.data.EncodingId> generateNewEncodingId(VersionInfo versionInfo, CompressionType compressionType, Position position) {
        return getNextEncodingId()
                .thenCompose(id -> writeToLog(new Record.EncodingRecord(id, versionInfo, compressionType), position)
                        .thenCompose(v -> {
                            TableRecords.EncodingId idIndex = new TableRecords.EncodingId(id);
                            TableRecords.EncodingInfo infoIndex = new TableRecords.EncodingInfo(versionInfo, compressionType);
                            Operation.Add idToInfo = new Operation.Add(idIndex, infoIndex);
                            Operation.Add infoToId = new Operation.Add(infoIndex, idIndex);
                            return updateTable(Lists.newArrayList(idToInfo, infoToId), new TableRecords.WALPositionValue(position));
                        }).thenApply(v -> id));
    }

    private CompletableFuture<Either<io.pravega.schemaregistry.contract.data.EncodingId, Position>> getEncodingId(VersionInfo versionInfo, CompressionType compressionType) {
        TableRecords.EncodingInfo encodingInfoIndex = new TableRecords.EncodingInfo(versionInfo, compressionType);
        return table.getRecord(encodingInfoIndex, TableRecords.EncodingId.class)
                    .thenCompose(record -> {
                        if (record == null) {
                            return syncIndex().thenCompose(pos -> table.getRecord(encodingInfoIndex, TableRecords.EncodingId.class)
                                                                       .thenApply(rec -> rec != null ? Either.left(rec.getEncodingId()) : Either.right(pos)));
                        } else {
                            return CompletableFuture.completedFuture(Either.left(record.getEncodingId()));
                        }
                    });
    }

    private CompletableFuture<io.pravega.schemaregistry.contract.data.EncodingId> getNextEncodingId() {
        Predicate<TableRecords.Key> predicate = x -> x instanceof TableRecords.EncodingId;
        return getLatestEntryFor(predicate, ENCODING_ID_COMPARATOR)
                .thenApply(entry -> {
                    if (entry == null) {
                        return new io.pravega.schemaregistry.contract.data.EncodingId(0);
                    } else {
                        TableRecords.EncodingId index = (TableRecords.EncodingId) entry.getKey();
                        return new io.pravega.schemaregistry.contract.data.EncodingId(index.getEncodingId().getId() + 1);
                    }
                });
    }
    
    private CompletableFuture<TableRecords.GroupPropertiesValue> getGroupPropertiesRecord() {
        return table.getRecord(GROUP_PROPERTY_KEY, TableRecords.GroupPropertiesValue.class);
    }

    private CompletableFuture<SchemaValidationRules> getCurrentValidationRules() {
        return table.getRecord(VALIDATION_POLICY_KEY, TableRecords.ValidationRulesValue.class)
                    .thenApply(TableRecords.ValidationRulesValue::getValidationRules);
    }

    private CompletableFuture<VersionInfo> getVersionInternal(SchemaInfo schemaInfo, long fingerprint) {
        TableRecords.SchemaFingerprintKey key = new TableRecords.SchemaFingerprintKey(fingerprint);

        return table.getRecord(key, TableRecords.SchemaVersionValue.class)
                    .thenCompose(record -> {
                        if (record != null) {
                            return findVersion(record.getVersions(), schemaInfo);
                        } else {
                            return CompletableFuture.completedFuture(null);
                        }
                    });
    }

    private CompletableFuture<VersionInfo> findVersion(List<VersionInfo> versions, SchemaInfo toFind) {
        AtomicReference<VersionInfo> found = new AtomicReference<>();
        Iterator<VersionInfo> iterator = versions.iterator();
        return Futures.loop(() -> {
            return iterator.hasNext() && found.get() == null;
        }, () -> {
            VersionInfo version = iterator.next();
            return getSchema(version)
                    .thenAccept(schema -> {
                        if (Arrays.equals(schema.getSchemaData(), toFind.getSchemaData()) && schema.getName().equals(toFind.getName())) {
                            found.set(version);
                        }
                    });
        }, executor)
                      .thenApply(v -> found.get());
    }

    private CompletableFuture<ListWithToken<SchemaWithVersion>> getSchemasInternal() {
        // 1. get latest encoding id
        // 2. get all encoding infos corresponding to all previous encoding ids
        return table.getRecord(LATEST_ENCODING_ID_KEY, TableRecords.LatestEncodingIdValue.class)
                    .thenCompose(latest -> {

                        List<TableRecords.Key> keys = new LinkedList<>();
                        for (int i = 0; i < latest.getEncodingId().getId(); i++) {
                            keys.add(new TableRecords.EncodingId(new EncodingId(i)));
                        }
                        return table.getRecords(keys, TableRecords.EncodingInfo.class);
                    }).thenApply(list -> list.stream().map(TableRecords.EncodingInfo::getCompressionType)
                                             .distinct().collect(Collectors.toList()));

    }
}