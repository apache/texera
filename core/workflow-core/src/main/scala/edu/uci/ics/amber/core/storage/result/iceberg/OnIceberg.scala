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

package edu.uci.ics.amber.core.storage.result.iceberg

import edu.uci.ics.amber.util.IcebergUtil
import org.apache.iceberg.exceptions.NoSuchTableException

trait OnIceberg {
  def catalog: org.apache.iceberg.catalog.Catalog
  def tableNamespace: String
  def tableName: String

  /**
    * Expire snapshots for the table.
    */
  def expireSnapshots(): Unit = {
    val table = IcebergUtil
      .loadTableMetadata(catalog, tableNamespace, tableName)
      .getOrElse(throw new NoSuchTableException(s"table $tableNamespace.$tableName doesn't exist"))

    // Begin the snapshot expiration process:
    table
      .expireSnapshots() // Initiate snapshot expiration.
      .retainLast(1) // Retain only the most recent snapshot.
      .expireOlderThan(
        System.currentTimeMillis()
      ) // Expire all snapshots older than the current time.
      .cleanExpiredFiles(true) // Remove the files associated with expired snapshots.
      .commit() // Commit the changes to make expiration effective.
  }
}
