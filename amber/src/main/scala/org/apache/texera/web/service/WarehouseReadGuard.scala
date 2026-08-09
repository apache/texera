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

package org.apache.texera.web.service

import org.apache.texera.amber.core.storage.VFSURIFactory
import org.apache.texera.common.config.StorageConfig

import java.net.URI

/**
  * Guards reads of results that live in a per-user warehouse while the feature is off (#6930).
  *
  * The warehouse switch is a kill switch: turning it off must disable reads too, and it must
  * fail *explicitly*. Without this guard a `/wh/<name>/…` URI would resolve to the shared
  * default warehouse and surface "table not found" — indistinguishable from data loss. No data
  * is lost; re-enabling the switch restores access.
  */
object WarehouseReadGuard {

  def assertReadable(
      uri: URI,
      enabled: Boolean = StorageConfig.warehouseEnabled
  ): Unit = {
    val warehouse = VFSURIFactory.decodeURI(uri).warehouse
    // A `/wh/` prefix that decodes to no warehouse is unresolvable (illegal name); opening
    // it would silently fall back to the shared warehouse — refuse instead (#6930).
    if (warehouse.isEmpty && hasWarehousePrefix(uri)) {
      throw new IllegalStateException(s"unresolvable warehouse URI: $uri")
    }
    warehouse.filterNot(_ => enabled).foreach { name =>
      throw new IllegalStateException(
        s"this result is stored in warehouse '$name'; per-user warehouses are disabled in this deployment"
      )
    }
  }

  /**
    * Whether a cleanup path should skip this URI: while the feature is off, best-effort
    * cleanup must not reach into (and delete from) a disabled per-user warehouse. Loud
    * failure is wrong here — cleanup runs on unrelated actions — so callers skip instead.
    */
  def skipWhileDisabled(
      uri: URI,
      enabled: Boolean = StorageConfig.warehouseEnabled
  ): Boolean =
    !enabled && (VFSURIFactory.decodeURI(uri).warehouse.isDefined || hasWarehousePrefix(uri))

  private def hasWarehousePrefix(uri: URI): Boolean =
    Option(uri.getRawPath).exists(_.startsWith("/wh/"))
}
