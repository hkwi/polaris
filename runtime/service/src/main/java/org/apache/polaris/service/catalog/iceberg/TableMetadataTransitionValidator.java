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

import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.CommitFailedException;

final class TableMetadataTransitionValidator {
  static final String KEY_ID_PINNED_PROPERTY = "polaris.encryption.key-id-pinned";

  private TableMetadataTransitionValidator() {}

  static void validate(Map<String, String> trustedProperties, TableMetadata candidate) {
    if (!trustedProperties.containsKey(KEY_ID_PINNED_PROPERTY)) {
      throw new CommitFailedException(
          "Cannot update table because encryption key ID is not pinned in catalog state");
    }
    String expectedKeyId = trustedProperties.get(TableProperties.ENCRYPTION_TABLE_KEY);
    String candidateKeyId = candidate.properties().get(TableProperties.ENCRYPTION_TABLE_KEY);
    if (!Objects.equals(expectedKeyId, candidateKeyId)) {
      throw new CommitFailedException(
          "Cannot add, change, or remove encryption key ID after table creation");
    }
  }

  static void pin(Map<String, String> trustedProperties, TableMetadata metadata) {
    trustedProperties.put(KEY_ID_PINNED_PROPERTY, Boolean.TRUE.toString());
    String keyId = metadata.properties().get(TableProperties.ENCRYPTION_TABLE_KEY);
    if (keyId == null) {
      trustedProperties.remove(TableProperties.ENCRYPTION_TABLE_KEY);
    } else {
      trustedProperties.put(TableProperties.ENCRYPTION_TABLE_KEY, keyId);
    }
  }
}
