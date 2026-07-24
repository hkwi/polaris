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
import java.util.Set;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.junit.jupiter.api.Test;

class TableMetadataAdmissionTest {
  private static final String KEY_ID = "test-key";

  @Test
  void buildsAndValidatesTrustedPointerForEveryTable() {
    TableMetadata metadata = metadata(Map.of());
    Map<String, String> storedProperties =
        TableMetadataAdmission.admit(null, metadata, TableMetadataAdmission.Mode.STANDARD);

    assertThat(storedProperties)
        .containsEntry(
            TableMetadataAdmission.INTEGRITY_VERSION, TableMetadataAdmission.INTEGRITY_VERSION_1)
        .containsEntry(
            TableMetadataAdmission.METADATA_HASH, TableMetadataAdmission.metadataHash(metadata))
        .containsEntry(
            TableMetadataAdmission.ENCRYPTION_KEY_STATE, TableMetadataAdmission.ENCRYPTION_KEY_NONE)
        .doesNotContainKey(TableProperties.ENCRYPTION_TABLE_KEY);
    assertThatCode(
            () ->
                TableMetadataAdmission.validateCurrentMetadata(entity(storedProperties), metadata))
        .doesNotThrowAnyException();
  }

  @Test
  void recordsPresentEncryptionKeyState() {
    TableMetadata metadata = metadata(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID));

    assertThat(TableMetadataAdmission.admit(null, metadata, TableMetadataAdmission.Mode.STANDARD))
        .containsEntry(
            TableMetadataAdmission.ENCRYPTION_KEY_STATE,
            TableMetadataAdmission.ENCRYPTION_KEY_PRESENT)
        .containsEntry(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID);
  }

  @Test
  void metadataHashIsStableAfterFileRoundTrip() {
    TableMetadata metadata = metadata(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID));
    InMemoryFileIO fileIO = new InMemoryFileIO();
    String metadataLocation = "file:/tmp/table/metadata/v1.metadata.json";

    TableMetadataParser.write(metadata, fileIO.newOutputFile(metadataLocation));
    TableMetadata parsed = TableMetadataParser.read(fileIO, metadataLocation);

    assertThat(TableMetadataAdmission.metadataHash(parsed))
        .isEqualTo(TableMetadataAdmission.metadataHash(metadata));
  }

  @Test
  void rejectsMetadataThatDoesNotMatchTrustedDigest() {
    TableMetadata metadata = metadata(Map.of());
    IcebergTableLikeEntity entity =
        entity(TableMetadataAdmission.admit(null, metadata, TableMetadataAdmission.Mode.STANDARD));
    TableMetadata modified = updated(metadata, Map.of(TableProperties.COMMIT_NUM_RETRIES, "1"));

    assertThatThrownBy(() -> TableMetadataAdmission.validateCurrentMetadata(entity, modified))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Table metadata does not match the trusted catalog pointer");
  }

  @Test
  void rejectsIncompleteTrustedPointer() {
    TableMetadata metadata = metadata(Map.of());
    Map<String, String> incomplete =
        TableMetadataAdmission.admit(null, metadata, TableMetadataAdmission.Mode.STANDARD);
    incomplete.remove(TableMetadataAdmission.ENCRYPTION_KEY_STATE);

    assertThatThrownBy(
            () -> TableMetadataAdmission.validateCurrentMetadata(entity(incomplete), metadata))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Trusted table metadata pointer has an invalid key state");
  }

  @Test
  void rejectsPersistingBareMetadataLocation() {
    TableMetadata metadata = metadata(Map.of());
    IcebergTableLikeEntity bareEntity =
        entity(Map.of(IcebergTableLikeEntity.TABLE_UUID, metadata.uuid()));

    assertThatThrownBy(() -> TableMetadataAdmission.validatePersistable(bareEntity))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Iceberg table entity is missing its trusted pointer");
  }

  @Test
  void preservesNonMetadataInternalStateAcrossAdmission() {
    TableMetadata metadata = metadata(Map.of());
    Map<String, String> originalProperties =
        TableMetadataAdmission.admit(null, metadata, TableMetadataAdmission.Mode.STANDARD);
    originalProperties.put("idempotency-state", "preserve-me");
    IcebergTableLikeEntity original = entity(originalProperties);
    TableMetadata updated = updated(metadata, Map.of(TableProperties.COMMIT_NUM_RETRIES, "1"));

    assertThat(
            TableMetadataAdmission.admit(original, updated, TableMetadataAdmission.Mode.STANDARD))
        .containsEntry("idempotency-state", "preserve-me")
        .containsEntry(
            TableMetadataAdmission.METADATA_HASH, TableMetadataAdmission.metadataHash(updated))
        .doesNotContainEntry(
            TableMetadataAdmission.METADATA_HASH, TableMetadataAdmission.metadataHash(metadata));
  }

  @Test
  void legacyMetadataFailsClosedExceptForExplicitAttestation() {
    TableMetadata metadata = metadata(Map.of());
    IcebergTableLikeEntity legacy =
        entity(Map.of(IcebergTableLikeEntity.TABLE_UUID, metadata.uuid()));

    assertThatThrownBy(() -> TableMetadataAdmission.validateCurrentMetadata(legacy, metadata))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("has not been attested");
    assertThatThrownBy(
            () ->
                TableMetadataAdmission.admit(
                    legacy, metadata, TableMetadataAdmission.Mode.STANDARD))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("register-table overwrite");
    assertThatCode(
            () ->
                TableMetadataAdmission.admit(
                    legacy, metadata, TableMetadataAdmission.Mode.LEGACY_ATTESTATION))
        .doesNotThrowAnyException();
  }

  @Test
  void rejectsEveryEncryptionKeyStateTransition() {
    TableMetadata plaintext = metadata(Map.of());
    IcebergTableLikeEntity plaintextEntity =
        entity(TableMetadataAdmission.admit(null, plaintext, TableMetadataAdmission.Mode.STANDARD));
    TableMetadata addKey = updated(plaintext, Map.of(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID));

    assertThatThrownBy(
            () ->
                TableMetadataAdmission.admit(
                    plaintextEntity, addKey, TableMetadataAdmission.Mode.STANDARD))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("Cannot add, change, or remove");

    TableMetadata encrypted = metadata(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID));
    IcebergTableLikeEntity encryptedEntity =
        entity(TableMetadataAdmission.admit(null, encrypted, TableMetadataAdmission.Mode.STANDARD));
    TableMetadata changeKey =
        updated(encrypted, Map.of(TableProperties.ENCRYPTION_TABLE_KEY, "another-key"));
    TableMetadata removeKey =
        TableMetadata.buildFrom(encrypted)
            .removeProperties(Set.of(TableProperties.ENCRYPTION_TABLE_KEY))
            .build();

    assertThatThrownBy(
            () ->
                TableMetadataAdmission.admit(
                    encryptedEntity, changeKey, TableMetadataAdmission.Mode.STANDARD))
        .isInstanceOf(CommitFailedException.class);
    assertThatThrownBy(
            () ->
                TableMetadataAdmission.admit(
                    encryptedEntity, removeKey, TableMetadataAdmission.Mode.STANDARD))
        .isInstanceOf(CommitFailedException.class);
  }

  @Test
  void rejectsChangingTableUuid() {
    TableMetadata metadata = metadata(Map.of());
    IcebergTableLikeEntity entity =
        entity(TableMetadataAdmission.admit(null, metadata, TableMetadataAdmission.Mode.STANDARD));

    assertThatThrownBy(
            () ->
                TableMetadataAdmission.admit(
                    entity, metadata(Map.of()), TableMetadataAdmission.Mode.STANDARD))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Cannot commit: Cannot change the table UUID");
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

  private static TableMetadata updated(TableMetadata base, Map<String, String> properties) {
    return TableMetadata.buildFrom(base).setProperties(properties).build();
  }

  private static IcebergTableLikeEntity entity(Map<String, String> internalProperties) {
    return new IcebergTableLikeEntity.Builder(
            PolarisEntitySubType.ICEBERG_TABLE,
            TableIdentifier.of("namespace", "table"),
            Map.of(),
            internalProperties,
            "file:/tmp/table/metadata/v1.metadata.json")
        .build();
  }
}
