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

package org.apache.texera.amber.core.storage.result.iceberg

import org.apache.iceberg.Table
import org.apache.iceberg.catalog.{Catalog, Namespace, TableIdentifier}

import java.util.concurrent.atomic.AtomicInteger

/**
  * Test helper: delegates to `delegate` while counting `loadTable` calls -- the
  * discriminator for per-operation table resolution (#7290). A holder that pins a
  * `Table` (or refreshes a pinned one) touches the catalog once, at construction;
  * per-operation resolution touches it again on every flush/seek.
  */
class CountingCatalog(delegate: Catalog) extends Catalog {
  val loadTableCalls = new AtomicInteger()
  override def name(): String = "counting"
  override def loadTable(identifier: TableIdentifier): Table = {
    loadTableCalls.incrementAndGet()
    delegate.loadTable(identifier)
  }
  override def tableExists(identifier: TableIdentifier): Boolean =
    delegate.tableExists(identifier)
  override def listTables(namespace: Namespace): java.util.List[TableIdentifier] =
    delegate.listTables(namespace)
  override def dropTable(identifier: TableIdentifier, purge: Boolean): Boolean =
    delegate.dropTable(identifier, purge)
  override def renameTable(from: TableIdentifier, to: TableIdentifier): Unit =
    delegate.renameTable(from, to)
}
