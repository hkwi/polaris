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

import com.google.common.hash.Hashing;
import java.util.Base64;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.jspecify.annotations.Nullable;

/**
 * Stores and verifies encrypted table metadata against values kept in Polaris persistence.
 *
 * <p>The digest is computed from Iceberg's canonical JSON representation rather than the bytes of a
 * particular metadata file. This follows the same model as Iceberg's Hive catalog integrity checks
 * and permits metadata compression to change without changing the digest.
 */
public final class TableMetadataIntegrity {
  public static final String METADATA_HASH = METADATA_HASH_PROP;

  private TableMetadataIntegrity() {}

  /** Adds integrity properties when table encryption is enabled. */
  public static void addIntegrityProperties(
      Map<String, String> storedProperties, TableMetadata metadata) {
    String keyId = metadata.properties().get(TableProperties.ENCRYPTION_TABLE_KEY);
    if (keyId != null) {
      storedProperties.put(TableProperties.ENCRYPTION_TABLE_KEY, keyId);
      storedProperties.put(METADATA_HASH, metadataHash(metadata));
    }
  }

  /**
   * Verifies metadata read from object storage using the trusted values stored with the entity.
   *
   * <p>Tables created before integrity properties were introduced continue to load. Their integrity
   * properties are populated by the next successful commit.
   */
  public static void validateCurrentMetadata(
      IcebergTableLikeEntity entity, TableMetadata metadata) {
    Map<String, String> internalProperties = entity.getInternalPropertiesAsMap();
    String storedKeyId = internalProperties.get(TableProperties.ENCRYPTION_TABLE_KEY);
    String storedMetadataHash = internalProperties.get(METADATA_HASH);

    if (storedKeyId == null && storedMetadataHash == null) {
      return;
    }
    if (storedKeyId == null || storedMetadataHash == null) {
      throw new IllegalStateException("Encrypted table integrity properties are incomplete");
    }
    if (!storedMetadataHash.equals(metadataHash(metadata))
        || !storedKeyId.equals(metadata.properties().get(TableProperties.ENCRYPTION_TABLE_KEY))) {
      throw new IllegalStateException(
          "Encrypted table metadata does not match the trusted catalog state");
    }
  }

  /** Prevents an encrypted table's key ID from being changed or removed. */
  public static void validateKeyIdForUpdate(
      @Nullable IcebergTableLikeEntity entity,
      @Nullable TableMetadata base,
      TableMetadata metadata) {
    String trustedKeyId =
        entity == null
            ? null
            : entity.getInternalPropertiesAsMap().get(TableProperties.ENCRYPTION_TABLE_KEY);
    if (trustedKeyId == null && base != null) {
      trustedKeyId = base.properties().get(TableProperties.ENCRYPTION_TABLE_KEY);
    }

    String newKeyId = metadata.properties().get(TableProperties.ENCRYPTION_TABLE_KEY);
    if (trustedKeyId != null && !trustedKeyId.equals(newKeyId)) {
      throw new CommitFailedException(
          "Cannot change or remove encryption key ID for an encrypted table");
    }
  }

  static String metadataHash(TableMetadata metadata) {
    byte[] sha256 =
        Hashing.sha256().hashString(TableMetadataParser.toJson(metadata), UTF_8).asBytes();
    return Base64.getEncoder().encodeToString(sha256);
  }
}
