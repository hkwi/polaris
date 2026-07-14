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
package org.apache.polaris.service.catalog.io;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.EncryptedKeyParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.encryption.StandardEncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.polaris.core.entity.PolarisTaskConstants;

/** Utilities for connecting Polaris file operations to Iceberg table encryption. */
public final class PolarisEncryptionUtil {

  private PolarisEncryptionUtil() {}

  /**
   * Returns the table encryption manager, or Iceberg's plaintext manager for an unencrypted table.
   */
  public static EncryptionManager encryptionManager(
      TableMetadata metadata, KeyManagementClient keyManagementClient) {
    if (metadata == null
        || metadata.properties().get(TableProperties.ENCRYPTION_TABLE_KEY) == null) {
      return PlaintextEncryptionManager.instance();
    }

    if (keyManagementClient == null) {
      throw new IllegalArgumentException(
          "Cannot create encryption manager without a key management client. "
              + "Configure encryption.kms-type or encryption.kms-impl on the catalog");
    }

    return EncryptionUtil.createEncryptionManager(
        metadata.encryptionKeys(), metadata.properties(), keyManagementClient);
  }

  /**
   * Copies the catalog KMS configuration and the table's wrapped encryption keys into a cleanup
   * task. Cleanup tasks run after the catalog request has ended, so they cannot reconstruct these
   * values from the live table operations object.
   */
  public static void addCleanupTaskEncryptionProperties(
      Map<String, String> taskProperties,
      Map<String, String> catalogProperties,
      TableMetadata metadata) {
    if (metadata == null
        || metadata.properties().get(TableProperties.ENCRYPTION_TABLE_KEY) == null) {
      return;
    }

    // A custom KeyManagementClient may use additional catalog properties. Keep them in a
    // separate task namespace so they cannot overwrite the FileIO and storage properties that
    // were already resolved for this cleanup task.
    catalogProperties.forEach(
        (key, value) ->
            taskProperties.put(PolarisTaskConstants.ENCRYPTION_KMS_PROPERTY_PREFIX + key, value));

    List<EncryptedKey> keys = metadata.encryptionKeys();
    taskProperties.put(PolarisTaskConstants.ENCRYPTION_KEY_COUNT, String.valueOf(keys.size()));
    for (int i = 0; i < keys.size(); i++) {
      taskProperties.put(
          PolarisTaskConstants.ENCRYPTION_KEY_PREFIX + i, EncryptedKeyParser.toJson(keys.get(i)));
    }
  }

  /**
   * Wraps a task FileIO with Iceberg's EncryptingFileIO when the task represents an encrypted
   * table. The KMS client is owned by the returned FileIO and is closed with it.
   */
  public static FileIO encryptTaskFileIO(FileIO fileIO, Map<String, String> taskProperties) {
    if (taskProperties.get(TableProperties.ENCRYPTION_TABLE_KEY) == null) {
      return fileIO;
    }

    Map<String, String> catalogKmsProperties = catalogKmsProperties(taskProperties);
    if (!catalogKmsProperties.containsKey(CatalogProperties.ENCRYPTION_KMS_TYPE)
        && !catalogKmsProperties.containsKey(CatalogProperties.ENCRYPTION_KMS_IMPL)) {
      throw new IllegalArgumentException(
          "Cannot purge an encrypted table without encryption.kms-type or encryption.kms-impl");
    }

    KeyManagementClient keyManagementClient = EncryptionUtil.createKmsClient(catalogKmsProperties);
    EncryptionManager encryptionManager =
        new CloseableStandardEncryptionManager(
            encryptedKeys(taskProperties),
            taskProperties.get(TableProperties.ENCRYPTION_TABLE_KEY),
            PropertyUtil.propertyAsInt(
                taskProperties,
                TableProperties.ENCRYPTION_DEK_LENGTH,
                TableProperties.ENCRYPTION_DEK_LENGTH_DEFAULT),
            keyManagementClient);

    // EncryptingFileIO closes an EncryptionManager when it is Closeable. The subclass preserves
    // StandardEncryptionManager's type because Iceberg uses that type to decrypt manifest-list
    // key metadata, while also closing the task-owned KMS client.
    return EncryptingFileIO.combine(fileIO, encryptionManager);
  }

  private static Map<String, String> catalogKmsProperties(Map<String, String> taskProperties) {
    Map<String, String> catalogKmsProperties = new HashMap<>();
    taskProperties.forEach(
        (key, value) -> {
          if (key.startsWith(PolarisTaskConstants.ENCRYPTION_KMS_PROPERTY_PREFIX)) {
            catalogKmsProperties.put(
                key.substring(PolarisTaskConstants.ENCRYPTION_KMS_PROPERTY_PREFIX.length()), value);
          }
        });
    return catalogKmsProperties;
  }

  private static List<EncryptedKey> encryptedKeys(Map<String, String> taskProperties) {
    int keyCount =
        Integer.parseInt(
            taskProperties.getOrDefault(PolarisTaskConstants.ENCRYPTION_KEY_COUNT, "0"));
    List<EncryptedKey> keys = new ArrayList<>(keyCount);
    for (int i = 0; i < keyCount; i++) {
      String keyJson = taskProperties.get(PolarisTaskConstants.ENCRYPTION_KEY_PREFIX + i);
      if (keyJson == null) {
        throw new IllegalArgumentException(
            "Missing encrypted key " + i + " in cleanup task properties");
      }
      keys.add(EncryptedKeyParser.fromJson(keyJson));
    }
    return keys;
  }

  /** Makes the task-owned KMS client closeable without hiding Iceberg's standard manager type. */
  private static final class CloseableStandardEncryptionManager extends StandardEncryptionManager
      implements Closeable {
    private final KeyManagementClient keyManagementClient;

    private CloseableStandardEncryptionManager(
        List<EncryptedKey> keys,
        String tableKeyId,
        int dataKeyLength,
        KeyManagementClient keyManagementClient) {
      super(keys, tableKeyId, dataKeyLength, keyManagementClient);
      this.keyManagementClient = keyManagementClient;
    }

    @Override
    public void close() throws IOException {
      keyManagementClient.close();
    }
  }
}
