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

package edu.uci.ics.amber.core.storage

import edu.uci.ics.amber.util.IcebergUtil
import org.apache.iceberg.catalog.Catalog

/**
  * IcebergCatalogInstance is a singleton that manages the Iceberg catalog instance.
  * - Provides a single shared catalog for all Iceberg table-related operations in the Texera application.
  * - Lazily initializes the catalog on first access.
  * - Supports replacing the catalog instance primarily for testing or reconfiguration.
  */
object IcebergCatalogInstance {

  private var instance: Option[Catalog] = None

  /**
    * Retrieves the singleton Iceberg catalog instance.
    * - If the catalog is not initialized, it is lazily created using the configured properties.
    *
    * @return the Iceberg catalog instance.
    */
  def getInstance(): Catalog = {
    instance match {
      case Some(catalog) => catalog
      case None =>
        val catalog = StorageConfig.icebergCatalogType match {
          case "hadoop" =>
            IcebergUtil.createHadoopCatalog(
              "texera_iceberg",
              StorageConfig.fileStorageDirectoryPath
            )
          case "rest" =>
            IcebergUtil.createRestCatalog(
              "texera_iceberg",
              StorageConfig.fileStorageDirectoryPath
            )
          case "postgres" =>
            IcebergUtil.createPostgresCatalog(
              "texera_iceberg",
              StorageConfig.fileStorageDirectoryPath
            )
          case unsupported =>
            throw new IllegalArgumentException(s"Unsupported catalog type: $unsupported")
        }
        instance = Some(catalog)
        catalog
    }
  }

  /**
    * Replaces the existing Iceberg catalog instance.
    * - This method is useful for testing or dynamically updating the catalog.
    *
    * @param catalog the new Iceberg catalog instance to replace the current one.
    */
  def replaceInstance(catalog: Catalog): Unit = {
    instance = Some(catalog)
  }
}
