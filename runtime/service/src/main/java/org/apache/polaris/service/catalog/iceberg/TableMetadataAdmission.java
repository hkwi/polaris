/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.service.catalog.iceberg;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_HASH_PROP;

import com.google.common.base.Objects;
import com.google.common.hash.Hashing;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.jspecify.annotations.Nullable;

/**
 * Admits Iceberg table metadata into trusted Polaris persistence.
 *
 * <p>The metadata location, canonical metadata digest, table UUID, and encryption key state are
 * persisted in one entity revision. All metadata-location transitions must pass through this class
 * so that a location cannot be updated without updating its trusted digest.
 */
public final class TableMetadataAdmission {
  static final String INTEGRITY_VERSION = "metadata-integrity-version";
  static final String INTEGRITY_VERSION_1 = "1";
  static final String METADATA_HASH = METADATA_HASH_PROP;
  static final String ENCRYPTION_KEY_STATE = "metadata-encryption-key-state";
  static final String ENCRYPTION_KEY_NONE = "none";
  static final String ENCRYPTION_KEY_PRESENT = "present";

  private static final Set<String> MANAGED_PROPERTIES =
      Set.of(
          INTEGRITY_VERSION,
          METADATA_HASH,
          ENCRYPTION_KEY_STATE,
          TableProperties.ENCRYPTION_TABLE_KEY,
          IcebergTableLikeEntity.LOCATION,
          IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY,
          IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY,
          IcebergTableLikeEntity.FORMAT_VERSION,
          IcebergTableLikeEntity.TABLE_UUID,
          IcebergTableLikeEntity.CURRENT_SCHEMA_ID,
          IcebergTableLikeEntity.CURRENT_SNAPSHOT_ID,
          IcebergTableLikeEntity.LAST_COLUMN_ID,
          IcebergTableLikeEntity.NEXT_ROW_ID,
          IcebergTableLikeEntity.LAST_SEQUENCE_NUMBER,
          IcebergTableLikeEntity.LAST_UPDATED_MILLIS,
          IcebergTableLikeEntity.DEFAULT_SORT_ORDER_ID,
          IcebergTableLikeEntity.DEFAULT_SPEC_ID,
          IcebergTableLikeEntity.LAST_PARTITION_ID);

  enum Mode {
    STANDARD,
    LEGACY_ATTESTATION
  }

  private TableMetadataAdmission() {}

  /**
   * Validates and admits a candidate metadata revision.
   *
   * <p>A legacy entity without a trusted pointer can only be admitted by an explicit register-table
   * overwrite. Normal commits and notifications fail closed until that attestation occurs. The
   * returned map preserves non-metadata internal state and replaces all metadata-derived values as
   * one unit.
   */
  static Map<String, String> admit(
      @Nullable IcebergTableLikeEntity entity, TableMetadata candidate, Mode mode) {
    if (entity != null) {
      validateUpdate(entity, candidate, mode);
    }

    Map<String, String> admittedProperties =
        entity == null ? new HashMap<>() : new HashMap<>(entity.getInternalPropertiesAsMap());
    MANAGED_PROPERTIES.forEach(admittedProperties::remove);
    admittedProperties.putAll(buildProperties(candidate));
    return admittedProperties;
  }

  private static void validateUpdate(
      IcebergTableLikeEntity entity, TableMetadata candidate, Mode mode) {
    Map<String, String> trustedProperties = entity.getInternalPropertiesAsMap();
    if (!hasTrustedPointer(trustedProperties)) {
      if (mode != Mode.LEGACY_ATTESTATION) {
        throw new CommitFailedException(
            "Cannot update table with unverified legacy metadata; "
                + "attest it with register-table overwrite first");
      }

      String trustedUuid = trustedProperties.get(IcebergTableLikeEntity.TABLE_UUID);
      if (trustedUuid != null && !trustedUuid.equals(candidate.uuid())) {
        throw new CommitFailedException("Cannot attest table metadata with a different table UUID");
      }
      return;
    }

    validatePointerIsComplete(trustedProperties);
    if (!trustedProperties.get(IcebergTableLikeEntity.TABLE_UUID).equals(candidate.uuid())) {
      throw new CommitFailedException("Cannot commit: Cannot change the table UUID");
    }

    String trustedKeyId = trustedProperties.get(TableProperties.ENCRYPTION_TABLE_KEY);
    String candidateKeyId = candidate.properties().get(TableProperties.ENCRYPTION_TABLE_KEY);
    if (!Objects.equal(trustedKeyId, candidateKeyId)) {
      throw new CommitFailedException(
          "Cannot add, change, or remove encryption key ID after table creation");
    }
  }

  /** Verifies metadata loaded from object storage against its trusted pointer. */
  public static void validateCurrentMetadata(
      IcebergTableLikeEntity entity, TableMetadata metadata) {
    Map<String, String> trustedProperties = entity.getInternalPropertiesAsMap();
    if (!hasTrustedPointer(trustedProperties)) {
      throw new IllegalStateException(
          "Table metadata has not been attested; use register-table overwrite before loading it");
    }

    validatePointerIsComplete(trustedProperties);
    String trustedKeyId = trustedProperties.get(TableProperties.ENCRYPTION_TABLE_KEY);
    String metadataKeyId = metadata.properties().get(TableProperties.ENCRYPTION_TABLE_KEY);
    if (!trustedProperties.get(METADATA_HASH).equals(metadataHash(metadata))
        || !trustedProperties.get(IcebergTableLikeEntity.TABLE_UUID).equals(metadata.uuid())
        || !Objects.equal(trustedKeyId, metadataKeyId)) {
      throw new IllegalStateException("Table metadata does not match the trusted catalog pointer");
    }
  }

  static boolean hasTrustedPointer(IcebergTableLikeEntity entity) {
    return hasTrustedPointer(entity.getInternalPropertiesAsMap());
  }

  /** Rejects table entities that could persist a bare metadata location. */
  static void validatePersistable(IcebergTableLikeEntity entity) {
    if (entity.getMetadataLocation() == null) {
      throw new IllegalStateException("Iceberg table entity is missing its metadata location");
    }
    if (!hasTrustedPointer(entity)) {
      throw new IllegalStateException("Iceberg table entity is missing its trusted pointer");
    }
    validatePointerIsComplete(entity.getInternalPropertiesAsMap());
  }

  private static Map<String, String> buildProperties(TableMetadata metadata) {
    Map<String, String> storedProperties = new HashMap<>();
    storedProperties.put(INTEGRITY_VERSION, INTEGRITY_VERSION_1);
    storedProperties.put(METADATA_HASH, metadataHash(metadata));

    storedProperties.put(IcebergTableLikeEntity.LOCATION, metadata.location());
    if (metadata.properties().containsKey(TableProperties.WRITE_DATA_LOCATION)) {
      storedProperties.put(
          IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY,
          metadata.properties().get(TableProperties.WRITE_DATA_LOCATION));
    }
    if (metadata.properties().containsKey(TableProperties.WRITE_METADATA_LOCATION)) {
      storedProperties.put(
          IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY,
          metadata.properties().get(TableProperties.WRITE_METADATA_LOCATION));
    }
    storedProperties.put(
        IcebergTableLikeEntity.FORMAT_VERSION, String.valueOf(metadata.formatVersion()));
    storedProperties.put(IcebergTableLikeEntity.TABLE_UUID, metadata.uuid());
    storedProperties.put(
        IcebergTableLikeEntity.CURRENT_SCHEMA_ID, String.valueOf(metadata.currentSchemaId()));
    if (metadata.currentSnapshot() != null) {
      storedProperties.put(
          IcebergTableLikeEntity.CURRENT_SNAPSHOT_ID,
          String.valueOf(metadata.currentSnapshot().snapshotId()));
    }
    storedProperties.put(
        IcebergTableLikeEntity.LAST_COLUMN_ID, String.valueOf(metadata.lastColumnId()));
    storedProperties.put(IcebergTableLikeEntity.NEXT_ROW_ID, String.valueOf(metadata.nextRowId()));
    storedProperties.put(
        IcebergTableLikeEntity.LAST_SEQUENCE_NUMBER, String.valueOf(metadata.lastSequenceNumber()));
    storedProperties.put(
        IcebergTableLikeEntity.LAST_UPDATED_MILLIS, String.valueOf(metadata.lastUpdatedMillis()));
    if (metadata.sortOrder() != null) {
      storedProperties.put(
          IcebergTableLikeEntity.DEFAULT_SORT_ORDER_ID,
          String.valueOf(metadata.defaultSortOrderId()));
    }
    if (metadata.spec() != null) {
      storedProperties.put(
          IcebergTableLikeEntity.DEFAULT_SPEC_ID, String.valueOf(metadata.defaultSpecId()));
      storedProperties.put(
          IcebergTableLikeEntity.LAST_PARTITION_ID,
          String.valueOf(metadata.lastAssignedPartitionId()));
    }

    String keyId = metadata.properties().get(TableProperties.ENCRYPTION_TABLE_KEY);
    if (keyId != null) {
      storedProperties.put(ENCRYPTION_KEY_STATE, ENCRYPTION_KEY_PRESENT);
      storedProperties.put(TableProperties.ENCRYPTION_TABLE_KEY, keyId);
    } else {
      storedProperties.put(ENCRYPTION_KEY_STATE, ENCRYPTION_KEY_NONE);
    }
    return storedProperties;
  }

  static String metadataHash(TableMetadata metadata) {
    byte[] sha256 =
        Hashing.sha256().hashString(TableMetadataParser.toJson(metadata), UTF_8).asBytes();
    return Base64.getEncoder().encodeToString(sha256);
  }

  private static boolean hasTrustedPointer(Map<String, String> properties) {
    return properties.containsKey(INTEGRITY_VERSION) || properties.containsKey(METADATA_HASH);
  }

  private static void validatePointerIsComplete(Map<String, String> properties) {
    if (!INTEGRITY_VERSION_1.equals(properties.get(INTEGRITY_VERSION))
        || !properties.containsKey(METADATA_HASH)
        || !properties.containsKey(IcebergTableLikeEntity.TABLE_UUID)) {
      throw new IllegalStateException("Trusted table metadata pointer is incomplete");
    }

    String keyState = properties.get(ENCRYPTION_KEY_STATE);
    boolean keyStateIsComplete =
        (ENCRYPTION_KEY_NONE.equals(keyState)
                && !properties.containsKey(TableProperties.ENCRYPTION_TABLE_KEY))
            || (ENCRYPTION_KEY_PRESENT.equals(keyState)
                && properties.containsKey(TableProperties.ENCRYPTION_TABLE_KEY));
    if (!keyStateIsComplete) {
      throw new IllegalStateException("Trusted table metadata pointer has an invalid key state");
    }
  }
}
