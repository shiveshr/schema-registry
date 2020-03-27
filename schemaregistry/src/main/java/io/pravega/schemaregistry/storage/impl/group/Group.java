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
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaEvolution;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.contract.exceptions.CodecNotFoundException;
import io.pravega.schemaregistry.storage.Position;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.records.IndexRecord;
import io.pravega.schemaregistry.storage.records.Record;
import io.pravega.schemaregistry.storage.records.RecordWithPosition;

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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Class that implements all storage logic for a group. 
 * It makes use of two storage primitives, namely an append only write ahead log {@link Log} and an index {@link Index}.
 * The group's state is written into the log and then the index is updated for optimizing the queries. 
 * The source of truth is the log and the index may eventually catches up with the log. 
 * @param <V> Type of version used in the index. 
 */
public class Group<V> {
    private static final IndexRecord.SyncdTillKey SYNCD_TILL = new IndexRecord.SyncdTillKey();
    private static final IndexRecord.ValidationPolicyKey VALIDATION_POLICY_INDEX_KEY = new IndexRecord.ValidationPolicyKey();
    private static final IndexRecord.GroupPropertyKey GROUP_PROPERTY_INDEX_KEY = new IndexRecord.GroupPropertyKey();
    private static final IndexRecord.CodecsKey CODECS_KEY = new IndexRecord.CodecsKey();

    private static final Comparator<Index.Entry> VERSION_COMPARATOR = (v1, v2) -> {
        IndexRecord.VersionInfoKey version1 = (IndexRecord.VersionInfoKey) v1.getKey();
        IndexRecord.VersionInfoKey version2 = (IndexRecord.VersionInfoKey) v2.getKey();
        return Integer.compare(version1.getVersionInfo().getVersion(), version2.getVersionInfo().getVersion());
    };
    private static final HashFunction HASH = Hashing.murmur3_128();
    private static final Retry.RetryAndThrowConditionally WRITE_CONFLICT_RETRY = Retry.withExpBackoff(1, 2, Integer.MAX_VALUE, 100)
                                                                                      .retryWhen(x -> Exceptions.unwrap(x) instanceof StoreExceptions.WriteConflictException);

    private final Log wal;

    private final Index<V> index;

    private final ScheduledExecutorService executor;

    public Group(Log wal, Index<V> index, ScheduledExecutorService executor) {
        this.wal = wal;
        this.index = index;
        this.executor = executor;
    }

    public CompletableFuture<Void> create(SchemaType schemaType, Map<String, String> properties, boolean validateByObjectType, SchemaValidationRules schemaValidationRules) {
        return wal.getCurrentEtag()
                  .thenCompose(pos -> writeToLog(new Record.GroupPropertiesRecord(schemaType, validateByObjectType, properties, schemaValidationRules), pos)
                          .thenCompose(v -> {
                              IndexRecord.WALPositionValue walPosition = new IndexRecord.WALPositionValue(pos);
                              Operation.Add addGroupProp = new Operation.Add(GROUP_PROPERTY_INDEX_KEY, walPosition);
                              return updateIndex(Lists.newArrayList(addGroupProp), walPosition);
                          }));
    }

    public CompletableFuture<Position> getCurrentEtag() {
        return syncIndex();
    }

    public CompletableFuture<ListWithToken<String>> getObjectTypes() {
        return syncIndex().thenCompose(v -> index.getAllKeys())
                          .thenApply(list -> {
                              List<String> objectTypes = list
                                      .stream().filter(x -> x instanceof IndexRecord.VersionInfoKey)
                                      .map(x -> ((IndexRecord.VersionInfoKey) x).getVersionInfo().getSchemaName())
                                      .distinct().collect(Collectors.toList());
                              return new ListWithToken<>(objectTypes, null);
                          });
    }

    public CompletableFuture<ListWithToken<SchemaWithVersion>> getSchemas() {
        return getSchemasInternal(null, x -> x.getRecord() instanceof Record.SchemaRecord);
    }

    public CompletableFuture<ListWithToken<SchemaWithVersion>> getSchemas(VersionInfo from) {
        IndexRecord.VersionInfoKey versionInfoKey = new IndexRecord.VersionInfoKey(from);
        return index.getRecord(versionInfoKey, IndexRecord.WALPositionValue.class)
                    .thenCompose(fromPos -> getSchemasInternal(fromPos.getPosition(),
                            x -> x.getRecord() instanceof Record.SchemaRecord));
    }

    public CompletableFuture<ListWithToken<SchemaWithVersion>> getSchemas(String objectTypeName) {
        return getSchemasInternal(null,
                x -> x.getRecord() instanceof Record.SchemaRecord &&
                        ((Record.SchemaRecord) x.getRecord()).getSchemaInfo().getName().equals(objectTypeName));
    }

    public CompletableFuture<ListWithToken<SchemaWithVersion>> getSchemas(String objectTypeName, VersionInfo from) {
        IndexRecord.VersionInfoKey versionInfoKey = new IndexRecord.VersionInfoKey(from);
        return index.getRecord(versionInfoKey, IndexRecord.WALPositionValue.class)
                    .thenCompose(fromPos -> getSchemasInternal(fromPos.getPosition(),
                            x -> x.getRecord() instanceof Record.SchemaRecord &&
                                    ((Record.SchemaRecord) x.getRecord()).getSchemaInfo().getName().equals(objectTypeName)));
    }

    public CompletableFuture<SchemaInfo> getSchema(VersionInfo versionInfo) {
        return index.getRecord(new IndexRecord.VersionInfoKey(versionInfo), IndexRecord.WALPositionValue.class)
                    .thenCompose(record -> {
                        if (record == null) {
                            return syncIndex().thenCompose(v ->
                                    index.getRecord(new IndexRecord.VersionInfoKey(versionInfo), IndexRecord.WALPositionValue.class));
                        } else {
                            return CompletableFuture.completedFuture(record);
                        }
                    }).thenCompose(record -> wal.readAt(record.getPosition(), Record.SchemaRecord.class))
                    .thenApply(Record.SchemaRecord::getSchemaInfo);
    }

    public CompletableFuture<VersionInfo> getVersion(SchemaInfo schemaInfo) {
        long fingerPrint = getFingerprint(schemaInfo);
        return getVersionInternal(schemaInfo, fingerPrint)
                .thenCompose(found -> {
                    if (found == null) {
                        // syncIndex and search again
                        return syncIndex().thenCompose(v -> getVersionInternal(schemaInfo, fingerPrint));
                    } else {
                        return CompletableFuture.completedFuture(found);
                    }
                })
                .thenApply(version -> {
                    if (version == null) {
                        throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, String.format("Schema=%s", fingerPrint));
                    } else {
                        return version;
                    }
                });
    }

    public CompletableFuture<EncodingId> createEncodingId(VersionInfo versionInfo, CodecType codecType,
                                                          Position etag) {
        return generateNewEncodingId(versionInfo, codecType, etag);
    }

    public CompletableFuture<EncodingInfo> getEncodingInfo(EncodingId encodingId) {
        IndexRecord.EncodingIdIndex encodingIdIndex = new IndexRecord.EncodingIdIndex(encodingId);
        return index.getRecord(encodingIdIndex, IndexRecord.EncodingInfoIndex.class)
                    .thenCompose(encodingInfo -> {
                        if (encodingInfo == null) {
                            return syncIndex()
                                    .thenCompose(v -> index.getRecord(encodingIdIndex, IndexRecord.EncodingInfoIndex.class)
                                                           .thenApply(info -> {
                                                               if (info == null) {
                                                                   throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, encodingId.toString());
                                                               } else {
                                                                   return info;
                                                               }
                                                           }));
                        } else {
                            return CompletableFuture.completedFuture(encodingInfo);
                        }
                    }).thenCompose(encodingInfo -> getSchema(encodingInfo.getVersionInfo())
                        .thenApply(schemaInfo -> new EncodingInfo(encodingInfo.getVersionInfo(), schemaInfo, encodingInfo.getCodecType())));
    }

    public CompletableFuture<SchemaWithVersion> getLatestSchema(boolean sync) {
        return getLatestVersion(sync)
                .thenCompose(versionInfo -> {
                    if (versionInfo != null) {
                        IndexRecord.VersionInfoKey key = new IndexRecord.VersionInfoKey(versionInfo);
                        VersionInfo version = key.getVersionInfo();
                        return getSchema(version).thenApply(schema -> new SchemaWithVersion(schema, version));
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    public CompletableFuture<SchemaWithVersion> getLatestSchema(String objectTypeName, boolean sync) {
        return getLatestVersion(objectTypeName, sync)
                .thenCompose(versionInfo -> {
                    if (versionInfo != null) {
                        IndexRecord.VersionInfoKey key = new IndexRecord.VersionInfoKey(versionInfo);
                        VersionInfo version = key.getVersionInfo();
                        return getSchema(version).thenApply(schema -> new SchemaWithVersion(schema, version));
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    public CompletableFuture<List<CodecType>> getCodecTypes() {
        return syncIndex().thenCompose(v ->
                index.getRecord(CODECS_KEY, IndexRecord.CodecsListValue.class)
                     .thenApply(codecs -> {
                         if (codecs == null) {
                             return Collections.singletonList(CodecType.None);
                         } else {
                             return codecs.getCodecs();
                         }
                     }));
    }

    public CompletableFuture<Void> addCodec(CodecType codecType) {
        // get all codecs. if codec doesnt exist, add it to log. let it get synced to the table. 
        // generate encoding id will only generate if the codec is already registered.
        return WRITE_CONFLICT_RETRY.runAsync(() -> syncIndex()
                .thenCompose(etag -> index.getRecordWithVersion(CODECS_KEY, IndexRecord.CodecsListValue.class)
                                          .thenCompose(rec -> {
                                              if (rec == null || !rec.getValue().getCodecs().contains(codecType)) {
                                                  return addCodecToLogAndIndex(codecType, etag);
                                              } else {
                                                  return CompletableFuture.completedFuture(null);
                                              }
                                          })), executor);
    }

    private CompletionStage<Void> addCodecToLogAndIndex(CodecType codecType, Position etag) {
        return writeToLog(new Record.CodecRecord(codecType), etag)
                .thenCompose(v -> {
                    // add to index
                    IndexRecord.CodecsListValue codecsVal = new IndexRecord.CodecsListValue(Collections.singletonList(codecType));
                    Operation.AddToList addToList = new Operation.AddToList(CODECS_KEY, codecsVal);
                    return updateIndex(Collections.singletonList(addToList), new IndexRecord.WALPositionValue(etag));
                });
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
        return getGroupPropertiesRecord()
                .thenCompose(prop -> addSchema(schemaInfo, versionInfo, prop.isValidateByObjectType(), etag));
    }

    @SuppressWarnings("unchecked")
    public CompletableFuture<Void> updateValidationPolicy(SchemaValidationRules policy, Position etag) {
        return writeToLog(new Record.ValidationRecord(policy), etag)
                .thenCompose(v -> {
                    Operation.GetAndSet getAndSet = new Operation.GetAndSet(new IndexRecord.ValidationPolicyKey(), new IndexRecord.WALPositionValue(etag),
                            x -> etag.getPosition().compareTo(((IndexRecord.WALPositionValue) x).getPosition().getPosition()) > 0);
                    return updateIndex(Collections.singletonList(getAndSet), new IndexRecord.WALPositionValue(etag));
                });
    }

    public CompletableFuture<GroupProperties> getGroupProperties() {
        return syncIndex().thenCompose(v -> {
            CompletableFuture<Record.GroupPropertiesRecord> grpPropertiesFuture = getGroupPropertiesRecord();

            CompletableFuture<SchemaValidationRules> rulesFuture = getCurrentValidationRules();

            return CompletableFuture.allOf(grpPropertiesFuture, rulesFuture)
                                    .thenApply(x -> {
                                        Record.GroupPropertiesRecord properties = grpPropertiesFuture.join();
                                        SchemaValidationRules rules = rulesFuture.join();
                                        return new GroupProperties(properties.getSchemaType(), rules,
                                                properties.isValidateByObjectType(), properties.getProperties());
                                    });
        });
    }

    private long getFingerprint(SchemaInfo schemaInfo) {
        return HASH.hashBytes(schemaInfo.getSchemaData()).asLong();
    }

    @SuppressWarnings("unchecked")
    private CompletableFuture<Position> syncIndex() {
        return index.getRecordWithVersion(SYNCD_TILL, IndexRecord.WALPositionValue.class)
                    .thenCompose(value -> {
                        Position pos = value == null ? null : value.getValue().getPosition();
                        return wal.readFrom(pos)
                                  .thenCompose(list -> {
                                      List<Operation> operations = new LinkedList<>();
                                      list.forEach(x -> {
                                          if (x.getRecord() instanceof Record.SchemaRecord) {
                                              Record.SchemaRecord record = (Record.SchemaRecord) x.getRecord();
                                              Operation.Add add = new Operation.Add(new IndexRecord.VersionInfoKey(record.getVersionInfo()), new IndexRecord.WALPositionValue(x.getPosition()));
                                              Operation.AddToList addToList = new Operation.AddToList(new IndexRecord.SchemaInfoKey(getFingerprint(record.getSchemaInfo())),
                                                      new IndexRecord.SchemaVersionValue(Collections.singletonList(record.getVersionInfo())));
                                              operations.add(add);
                                              operations.add(addToList);
                                          } else if (x.getRecord() instanceof Record.EncodingRecord) {
                                              Record.EncodingRecord record = (Record.EncodingRecord) x.getRecord();
                                              IndexRecord.EncodingIdIndex idIndex = new IndexRecord.EncodingIdIndex(record.getEncodingId());
                                              IndexRecord.EncodingInfoIndex infoIndex = new IndexRecord.EncodingInfoIndex(record.getVersionInfo(), record.getCodecType());
                                              Operation.Add idToInfo = new Operation.Add(idIndex, infoIndex);
                                              Operation.Add infoToId = new Operation.Add(infoIndex, idIndex);
                                              operations.add(idToInfo);
                                              operations.add(infoToId);
                                          } else if (x.getRecord() instanceof Record.ValidationRecord) {
                                              Operation.GetAndSet getAndSet = new Operation.GetAndSet(new IndexRecord.ValidationPolicyKey(),
                                                      new IndexRecord.WALPositionValue(x.getPosition()),
                                                      m -> ((IndexRecord.WALPositionValue) m).getPosition().getPosition()
                                                                                             .compareTo(x.getPosition().getPosition()) < 0);
                                              operations.add(getAndSet);
                                          } else if (x.getRecord() instanceof Record.CodecRecord) {
                                              Record.CodecRecord record = (Record.CodecRecord) x.getRecord();
                                              Operation.AddToList addToList = new Operation.AddToList(CODECS_KEY,
                                                      new IndexRecord.CodecsListValue(Collections.singletonList(record.getCodecType())));
                                              operations.add(addToList);
                                          }
                                      });
                                      if (!list.isEmpty()) {
                                          Position syncdTill = list.get(list.size() - 1).getNext();
                                          return updateIndex(operations, new IndexRecord.WALPositionValue(syncdTill))
                                                  .thenApply(v -> syncdTill);
                                      } else {
                                          return CompletableFuture.completedFuture(pos);
                                      }
                                  });
                    });
    }

    private CompletableFuture<Void> updateIndex(List<Operation> operations, IndexRecord.WALPositionValue syncdTill) {
        List<CompletableFuture<Void>> futures = new LinkedList<>();
        operations.forEach(operation -> {
            if (operation instanceof Operation.Add) {
                futures.add(index.addEntry(((Operation.Add) operation).getKey(), ((Operation.Add) operation).getValue()));
            } else if (operation instanceof Operation.GetAndSet) {
                Operation.GetAndSet op = (Operation.GetAndSet) operation;
                futures.add(WRITE_CONFLICT_RETRY.runAsync(() -> index.getRecordWithVersion(op.getKey(), IndexRecord.IndexValue.class)
                                                                     .thenCompose(existing -> {
                                                                         if (existing == null) {
                                                                             return index.addEntry(op.getKey(), op.getValue());
                                                                         } else if (op.getCondition().test(existing.getValue())) {
                                                                             return index.updateEntry(op.getKey(), op.getValue(), existing.getVersion());
                                                                         } else {
                                                                             return CompletableFuture.completedFuture(null);
                                                                         }
                                                                     }), executor));
            } else if (operation instanceof Operation.AddToList) {
                Operation.AddToList op = (Operation.AddToList) operation;
                futures.add(WRITE_CONFLICT_RETRY.runAsync(() -> index.getRecordWithVersion(op.getKey(), IndexRecord.IndexValue.class)
                                                                     .thenCompose(existing -> {
                                                                         if (existing == null) {
                                                                             return index.addEntry(op.getKey(), op.getValue());
                                                                         } else if (op.getValue() instanceof IndexRecord.SchemaVersionValue) {
                                                                             IndexRecord.SchemaVersionValue existingList = (IndexRecord.SchemaVersionValue) existing.getValue();
                                                                             IndexRecord.SchemaVersionValue toAdd = (IndexRecord.SchemaVersionValue) op.getValue();
                                                                             Set<VersionInfo> set = new HashSet<>(existingList.getVersions());
                                                                             set.addAll(toAdd.getVersions());

                                                                             IndexRecord.SchemaVersionValue newValue = new IndexRecord.SchemaVersionValue(new ArrayList<>(set));
                                                                             return index.updateEntry(op.getKey(), newValue, existing.getVersion());
                                                                         } else if (op.getValue() instanceof IndexRecord.CodecsListValue) {
                                                                             IndexRecord.CodecsListValue existingList = (IndexRecord.CodecsListValue) existing.getValue();
                                                                             IndexRecord.CodecsListValue toAdd = (IndexRecord.CodecsListValue) op.getValue();
                                                                             Set<CodecType> set = new HashSet<>(existingList.getCodecs());
                                                                             set.addAll(toAdd.getCodecs());

                                                                             IndexRecord.CodecsListValue newValue = new IndexRecord.CodecsListValue(new ArrayList<>(set));
                                                                             return index.updateEntry(op.getKey(), newValue, existing.getVersion());
                                                                         } else {
                                                                             return CompletableFuture.completedFuture(null);
                                                                         }
                                                                     }), executor));
            }
        });

        return Futures.allOf(futures)
                      .thenCompose(v -> index.getRecordWithVersion(SYNCD_TILL, IndexRecord.WALPositionValue.class)
                                             .thenCompose(syncdTillWithVersion -> addOrUpdateSyncdTillKey(syncdTill, syncdTillWithVersion)));
    }

    @SuppressWarnings("unchecked")
    private CompletableFuture<Void> addOrUpdateSyncdTillKey(IndexRecord.WALPositionValue syncdTill,
                                                            Index.Value<IndexRecord.WALPositionValue, V> syncdTillWithVersion) {
        return WRITE_CONFLICT_RETRY.runAsync(() -> {
            if (syncdTillWithVersion == null) {
                return index.addEntry(SYNCD_TILL, syncdTill);
            } else {
                Position newPos = syncdTill.getPosition();
                Position existingPos = syncdTillWithVersion.getValue().getPosition();
                if (newPos.getPosition().compareTo(existingPos.getPosition()) > 0) {
                    return index.updateEntry(SYNCD_TILL, syncdTill, syncdTillWithVersion.getVersion());
                } else {
                    return CompletableFuture.completedFuture(null);
                }
            }
        }, executor);
    }

    private CompletableFuture<Void> writeToLog(Record record, Position etag) {
        return Futures.toVoid(wal.writeToLog(record, etag));
    }

    private CompletableFuture<VersionInfo> addSchema(SchemaInfo schemaInfo, VersionInfo next, boolean validateByObjectType, Position etag) {
        return writeToLog(new Record.SchemaRecord(schemaInfo, next), etag)
                .thenCompose(v -> {
                    Operation.Add add = new Operation.Add(new IndexRecord.VersionInfoKey(next), new IndexRecord.WALPositionValue(etag));

                    Operation.AddToList addToList = new Operation.AddToList(new IndexRecord.SchemaInfoKey(getFingerprint(schemaInfo)),
                            new IndexRecord.SchemaVersionValue(Collections.singletonList(next)));

                    IndexRecord.IndexKey key;
                    if (validateByObjectType) {
                        key = new IndexRecord.LatestSchemaVersionForObjectTypeKey(schemaInfo.getName());
                    } else {
                        key = new IndexRecord.LatestSchemaVersionKey();
                    }
                    Operation.GetAndSet getAndSet = new Operation.GetAndSet(key,
                            new IndexRecord.LatestSchemaVersionValue(next),
                            x -> ((IndexRecord.LatestSchemaVersionValue) x).getVersion().getVersion() < next.getVersion());

                    return updateIndex(Lists.newArrayList(add, addToList, getAndSet), new IndexRecord.WALPositionValue(etag))
                            .thenApply(x -> next);
                });
    }

    public CompletableFuture<VersionInfo> getLatestVersion(boolean sync) {
        IndexRecord.LatestSchemaVersionKey key = new IndexRecord.LatestSchemaVersionKey();

        CompletableFuture<Position> syncFuture = sync ? syncIndex() : CompletableFuture.completedFuture(null);
        return syncFuture.thenCompose(v -> index.getRecord(key, IndexRecord.LatestSchemaVersionValue.class)
                                                .thenApply(rec -> {
                                                    if (rec == null) {
                                                        return null;
                                                    } else {
                                                        return rec.getVersion();
                                                    }
                                                }));
    }

    public CompletableFuture<VersionInfo> getLatestVersion(String objectTypeName, boolean sync) {
        IndexRecord.LatestSchemaVersionForObjectTypeKey key = new IndexRecord.LatestSchemaVersionForObjectTypeKey(objectTypeName);

        CompletableFuture<Position> syncFuture = sync ? syncIndex() : CompletableFuture.completedFuture(null);
        return syncFuture.thenCompose(v -> index.getRecord(key, IndexRecord.LatestSchemaVersionValue.class)
                                                .thenApply(rec -> {
                                                    if (rec == null) {
                                                        return null;
                                                    } else {
                                                        return rec.getVersion();
                                                    }
                                                }));
    }

    private CompletableFuture<EncodingId> generateNewEncodingId(VersionInfo versionInfo, CodecType codecType, Position position) {
        return getCodecTypes()
                .thenCompose(codecs -> {
                    if (codecs.contains(codecType)) {
                        return getNextEncodingId()
                                .thenCompose(id -> writeToLog(new Record.EncodingRecord(id, versionInfo, codecType), position)
                                        .thenCompose(v -> {
                                            IndexRecord.EncodingIdIndex idIndex = new IndexRecord.EncodingIdIndex(id);
                                            IndexRecord.EncodingInfoIndex infoIndex = new IndexRecord.EncodingInfoIndex(versionInfo, codecType);
                                            Operation.Add idToInfo = new Operation.Add(idIndex, infoIndex);
                                            Operation.Add infoToId = new Operation.Add(infoIndex, idIndex);
                                            Operation.GetAndSet latest = new Operation.GetAndSet(new IndexRecord.LatestEncodingIdKey(),
                                                    new IndexRecord.LatestEncodingIdValue(idIndex.getEncodingId()),
                                                    x -> ((IndexRecord.LatestEncodingIdValue) x).getEncodingId().getId() < idIndex.getEncodingId().getId());
                                            return updateIndex(Lists.newArrayList(idToInfo, infoToId, latest), new IndexRecord.WALPositionValue(position));
                                        }).thenApply(v -> id));
                    } else {
                        throw new CodecNotFoundException(String.format("codec %s not registered", codecType));
                    }
                });
    }

    public CompletableFuture<Either<EncodingId, Position>> getEncodingId(VersionInfo versionInfo, CodecType codecType) {
        IndexRecord.EncodingInfoIndex encodingInfoIndex = new IndexRecord.EncodingInfoIndex(versionInfo, codecType);
        return index.getRecord(encodingInfoIndex, IndexRecord.EncodingIdIndex.class)
                    .thenCompose(record -> {
                        if (record == null) {
                            return syncIndex().thenCompose(pos -> index.getRecord(encodingInfoIndex, IndexRecord.EncodingIdIndex.class)
                                                                       .thenApply(rec -> rec != null ? Either.left(rec.getEncodingId()) : Either.right(pos)));
                        } else {
                            return CompletableFuture.completedFuture(Either.left(record.getEncodingId()));
                        }
                    });
    }

    private CompletableFuture<EncodingId> getNextEncodingId() {
        IndexRecord.LatestEncodingIdKey key = new IndexRecord.LatestEncodingIdKey();
        return index.getRecord(key, IndexRecord.LatestEncodingIdValue.class)
                    .thenApply(rec -> {
                        if (rec == null) {
                            return new EncodingId(0);
                        } else {
                            return new EncodingId(rec.getEncodingId().getId() + 1);
                        }
                    });
    }

    private CompletableFuture<Record.GroupPropertiesRecord> getGroupPropertiesRecord() {
        return index.getRecord(GROUP_PROPERTY_INDEX_KEY, IndexRecord.WALPositionValue.class)
                    .thenCompose(record -> wal.readAt(record.getPosition(), Record.GroupPropertiesRecord.class));
    }

    private CompletableFuture<SchemaValidationRules> getCurrentValidationRules() {
        return index.getRecord(VALIDATION_POLICY_INDEX_KEY, IndexRecord.WALPositionValue.class)
                    .thenCompose(validationRecordPosition -> {
                        if (validationRecordPosition == null) {
                            return index.getRecord(GROUP_PROPERTY_INDEX_KEY, IndexRecord.WALPositionValue.class)
                                        .thenCompose(groupPropPos -> {
                                            return wal.readAt(groupPropPos.getPosition(), Record.GroupPropertiesRecord.class)
                                                      .thenApply(Record.GroupPropertiesRecord::getValidationRules);
                                        });

                        } else {
                            return wal.readAt(validationRecordPosition.getPosition(), Record.ValidationRecord.class)
                                      .thenApply(Record.ValidationRecord::getValidationRules);
                        }
                    });
    }

    private CompletableFuture<VersionInfo> getVersionInternal(SchemaInfo schemaInfo, long fingerprint) {
        IndexRecord.SchemaInfoKey key = new IndexRecord.SchemaInfoKey(fingerprint);

        return index.getRecord(key, IndexRecord.SchemaVersionValue.class)
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

    private CompletableFuture<ListWithToken<SchemaWithVersion>> getSchemasInternal(Position position, Predicate<RecordWithPosition> predicate) {
        return wal.readFrom(position)
                  .thenApply(records -> {
                      List<SchemaWithVersion> schemas = records.stream().filter(predicate).map(x -> {
                          Record.SchemaRecord record = (Record.SchemaRecord) x.getRecord();
                          return new SchemaWithVersion(record.getSchemaInfo(), record.getVersionInfo());
                      }).collect(Collectors.toList());

                      return new ListWithToken<>(schemas, null);
                  });
    }
}