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

package org.apache.texera.service.resource

import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.Dataset.DATASET
import org.apache.texera.dao.jooq.generated.tables.DatasetUserAccess.DATASET_USER_ACCESS
import org.apache.texera.dao.jooq.generated.tables.records.{DatasetRecord, DatasetUserAccessRecord}
import org.jooq.{Record, Table, TableField}

/**
  * Describes one user-owned, name-scoped, shareable resource by the columns that carry its
  * identity, ownership, and per-user grants.
  *
  * @param label how the resource is named in user-facing messages ("dataset", "model")
  * @tparam R record type of the resource table
  * @tparam A record type of the companion user-access table
  */
case class ManagedResource[R <: Record, A <: Record](
    label: String,
    idField: TableField[R, Integer],
    ownerUidField: TableField[R, Integer],
    nameField: TableField[R, String],
    isPublicField: TableField[R, java.lang.Boolean],
    accessIdField: TableField[A, Integer],
    accessUidField: TableField[A, Integer],
    privilegeField: TableField[A, PrivilegeEnum]
) {
  def table: Table[R] = idField.getTable
  def accessTable: Table[A] = accessIdField.getTable
}

object ManagedResource {

  val Dataset: ManagedResource[DatasetRecord, DatasetUserAccessRecord] =
    ManagedResource(
      label = "dataset",
      idField = DATASET.DID,
      ownerUidField = DATASET.OWNER_UID,
      nameField = DATASET.NAME,
      isPublicField = DATASET.IS_PUBLIC,
      accessIdField = DATASET_USER_ACCESS.DID,
      accessUidField = DATASET_USER_ACCESS.UID,
      privilegeField = DATASET_USER_ACCESS.PRIVILEGE
    )

}
