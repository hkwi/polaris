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
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.junit.jupiter.api.Test;

class TableMetadataIntegrityTest {
  private static final String KEY_ID = "test-key";

  @Test
  void addsAndValidatesIntegrityProperties() {
    TableMetadata metadata = metadata(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID));
    Map<String, String> storedProperties = new HashMap<>();

    TableMetadataIntegrity.addIntegrityProperties(storedProperties, metadata);

    assertThat(storedProperties)
        .containsEntry(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID)
        .containsEntry(
            TableMetadataIntegrity.METADATA_HASH, TableMetadataIntegrity.metadataHash(metadata));
    assertThatCode(
            () ->
                TableMetadataIntegrity.validateCurrentMetadata(entity(storedProperties), metadata))
        .doesNotThrowAnyException();
  }

  @Test
  void metadataHashIsStableAfterFileRoundTrip() {
    TableMetadata metadata = metadata(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID));
    InMemoryFileIO fileIO = new InMemoryFileIO();
    String metadataLocation = "file:/tmp/table/metadata/v1.metadata.json";

    TableMetadataParser.write(metadata, fileIO.newOutputFile(metadataLocation));
    TableMetadata parsed = TableMetadataParser.read(fileIO, metadataLocation);

    assertThat(TableMetadataIntegrity.metadataHash(parsed))
        .isEqualTo(TableMetadataIntegrity.metadataHash(metadata));
  }

  @Test
  void rejectsMetadataThatDoesNotMatchTrustedDigest() {
    TableMetadata metadata = metadata(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID));
    Map<String, String> storedProperties = new HashMap<>();
    TableMetadataIntegrity.addIntegrityProperties(storedProperties, metadata);
    TableMetadata modified =
        metadata(
            Map.of(
                TableProperties.ENCRYPTION_TABLE_KEY,
                KEY_ID,
                TableProperties.COMMIT_NUM_RETRIES,
                "1"));

    assertThatThrownBy(
            () ->
                TableMetadataIntegrity.validateCurrentMetadata(entity(storedProperties), modified))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Encrypted table metadata does not match the trusted catalog state");
  }

  @Test
  void allowsLegacyTableWithoutIntegrityProperties() {
    assertThatCode(
            () ->
                TableMetadataIntegrity.validateCurrentMetadata(
                    entity(Map.of()),
                    metadata(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID))))
        .doesNotThrowAnyException();
  }

  @Test
  void rejectsChangingOrRemovingEncryptionKeyId() {
    TableMetadata base = metadata(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID));
    IcebergTableLikeEntity entity = entity(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID));

    assertThatCode(
            () ->
                TableMetadataIntegrity.validateKeyIdForUpdate(
                    entity, base, metadata(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID))))
        .doesNotThrowAnyException();
    assertThatThrownBy(
            () ->
                TableMetadataIntegrity.validateKeyIdForUpdate(
                    entity,
                    base,
                    metadata(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, "another-key"))))
        .isInstanceOf(CommitFailedException.class);
    assertThatThrownBy(
            () -> TableMetadataIntegrity.validateKeyIdForUpdate(entity, base, metadata(Map.of())))
        .isInstanceOf(CommitFailedException.class);
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
