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

import org.apache.texera.common.config.StorageConfig

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
      warehouse: Option[String],
      enabled: Boolean = StorageConfig.warehouseEnabled
  ): Unit =
    warehouse.filterNot(_ => enabled).foreach { name =>
      throw new IllegalStateException(
        s"this result is stored in warehouse '$name'; per-user warehouses are disabled in this deployment"
      )
    }
}
