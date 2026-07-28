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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.junit.jupiter.api.Test;

class TableMetadataIntegrityTest {
  private static final String KEY_ID = "test-key";

  @Test
  void pinsAndValidatesEncryptedMetadata() {
    TableMetadata metadata = metadata(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID));
    Map<String, String> trustedProperties = pinnedProperties(KEY_ID);

    TableMetadataIntegrity.pin(trustedProperties, metadata);

    assertThat(trustedProperties)
        .containsEntry(
            TableMetadataIntegrity.METADATA_HASH_PROPERTY,
            TableMetadataIntegrity.metadataHash(metadata))
        .containsEntry(
            TableMetadataIntegrity.METADATA_HASH_VERSION_PROPERTY,
            TableMetadataIntegrity.METADATA_HASH_VERSION);
    assertThatCode(() -> TableMetadataIntegrity.validate(entity(trustedProperties), metadata))
        .doesNotThrowAnyException();
  }

  @Test
  void doesNotPinPlaintextMetadata() {
    Map<String, String> trustedProperties = pinnedProperties(null);
    trustedProperties.put(TableMetadataIntegrity.METADATA_HASH_PROPERTY, "obsolete");

    TableMetadataIntegrity.pin(trustedProperties, metadata(Map.of()));

    assertThat(trustedProperties).doesNotContainKey(TableMetadataIntegrity.METADATA_HASH_PROPERTY);
    assertThat(trustedProperties)
        .doesNotContainKey(TableMetadataIntegrity.METADATA_HASH_VERSION_PROPERTY);
    assertThatCode(
            () -> TableMetadataIntegrity.validate(entity(trustedProperties), metadata(Map.of())))
        .doesNotThrowAnyException();
  }

  @Test
  void doesNotEnrollLegacyEncryptedMetadata() {
    Map<String, String> trustedProperties = new HashMap<>();

    TableMetadataIntegrity.pin(
        trustedProperties, metadata(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID)));

    assertThat(trustedProperties)
        .doesNotContainKey(TableMetadataIntegrity.METADATA_HASH_PROPERTY)
        .doesNotContainKey(TableMetadataIntegrity.METADATA_HASH_VERSION_PROPERTY)
        .doesNotContainKey(TableMetadataTransitionValidator.KEY_ID_PROPERTY);
  }

  @Test
  void rejectsKeyIdAddedToPinnedPlaintextMetadata() {
    Map<String, String> trustedProperties = pinnedProperties(null);
    TableMetadata modified = metadata(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, "added-key"));

    assertThatThrownBy(() -> TableMetadataIntegrity.validate(entity(trustedProperties), modified))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Iceberg table metadata encryption key ID does not match trusted catalog state");
  }

  @Test
  void rejectsModifiedEncryptedMetadata() {
    TableMetadata metadata = metadata(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID));
    Map<String, String> trustedProperties = pinnedProperties(KEY_ID);
    TableMetadataIntegrity.pin(trustedProperties, metadata);
    TableMetadata modified =
        metadata(
            Map.of(
                TableProperties.ENCRYPTION_TABLE_KEY,
                KEY_ID,
                TableProperties.COMMIT_NUM_RETRIES,
                "1"));

    assertThatThrownBy(() -> TableMetadataIntegrity.validate(entity(trustedProperties), modified))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("metadata loaded from storage has been modified");
  }

  @Test
  void rejectsMissingOrInvalidDigestForPinnedEncryptedTable() {
    Map<String, String> missingDigest = pinnedProperties(KEY_ID);

    assertThatThrownBy(
            () ->
                TableMetadataIntegrity.validate(
                    entity(missingDigest),
                    metadata(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("catalog state has no metadata digest");

    Map<String, String> invalidDigest = pinnedProperties(KEY_ID);
    invalidDigest.put(TableMetadataIntegrity.METADATA_HASH_PROPERTY, "not-base64!");
    invalidDigest.put(
        TableMetadataIntegrity.METADATA_HASH_VERSION_PROPERTY,
        TableMetadataIntegrity.METADATA_HASH_VERSION);
    assertThatThrownBy(
            () ->
                TableMetadataIntegrity.validate(
                    entity(invalidDigest),
                    metadata(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("catalog metadata digest is invalid");
  }

  @Test
  void rejectsUnsupportedDigestVersion() {
    TableMetadata metadata = metadata(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID));
    Map<String, String> trustedProperties = pinnedProperties(KEY_ID);
    trustedProperties.put(
        TableMetadataIntegrity.METADATA_HASH_PROPERTY,
        TableMetadataIntegrity.metadataHash(metadata));
    trustedProperties.put(TableMetadataIntegrity.METADATA_HASH_VERSION_PROPERTY, "future-version");

    assertThatThrownBy(() -> TableMetadataIntegrity.validate(entity(trustedProperties), metadata))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("catalog metadata digest version is unsupported");
  }

  @Test
  void leavesLegacyUnpinnedEntityOutsideInvariant() {
    assertThatCode(
            () ->
                TableMetadataIntegrity.validate(
                    entity(Map.of()),
                    metadata(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID))))
        .doesNotThrowAnyException();
  }

  @Test
  void canonicalHashIsStableAcrossMetadataFileRoundTrip() {
    TableMetadata metadata = metadata(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID));
    InMemoryFileIO fileIO = new InMemoryFileIO();
    String location = "file:/tmp/table/metadata/v1.metadata.json";

    TableMetadataParser.write(metadata, fileIO.newOutputFile(location));
    TableMetadata parsed = TableMetadataParser.read(fileIO, location);

    assertThat(TableMetadataIntegrity.metadataHash(parsed))
        .isEqualTo(TableMetadataIntegrity.metadataHash(metadata));
  }

  private static Map<String, String> pinnedProperties(String keyId) {
    Map<String, String> properties = new HashMap<>();
    properties.put(TableMetadataTransitionValidator.KEY_ID_PINNED_PROPERTY, "true");
    if (keyId != null) {
      properties.put(TableMetadataTransitionValidator.KEY_ID_PROPERTY, keyId);
    }
    return properties;
  }

  private static TableMetadata metadata(Map<String, String> properties) {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    Map<String, String> tableProperties = new HashMap<>(properties);
    if (tableProperties.containsKey(TableProperties.ENCRYPTION_TABLE_KEY)) {
      tableProperties.put(TableProperties.FORMAT_VERSION, "3");
    }
    return TableMetadata.newTableMetadata(
        schema, PartitionSpec.unpartitioned(), "file:/tmp/table", tableProperties);
  }

  private static IcebergTableLikeEntity entity(Map<String, String> properties) {
    return new IcebergTableLikeEntity.Builder(
            PolarisEntitySubType.ICEBERG_TABLE,
            TableIdentifier.of("namespace", "table"),
            Map.of(),
            properties,
            "file:/tmp/table/metadata/v1.metadata.json")
        .build();
  }
}
