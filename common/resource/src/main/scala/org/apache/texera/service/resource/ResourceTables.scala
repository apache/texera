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
import org.apache.texera.dao.jooq.generated.tables.Model.MODEL
import org.apache.texera.dao.jooq.generated.tables.ModelUserAccess.MODEL_USER_ACCESS
import org.apache.texera.dao.jooq.generated.tables.records.{
  DatasetRecord,
  DatasetUserAccessRecord,
  ModelRecord,
  ModelUserAccessRecord
}
import org.jooq.{Record, Table, TableField}

/**
  * The tables and columns that define one resource type, so the rules in [[ResourceAccess]] and
  * [[ResourceNaming]] can be written once and told which columns to read.
  *
  * A resource fits only if its own table carries the owner, the name and the `is_public` flag, and
  * it has a companion `*_user_access` table — today that is `dataset` and `model`. This is not a
  * model of every shareable resource: `workflow` keeps ownership in `workflow_of_user`, and
  * neither `project` nor `workflow_computing_unit` has `is_public`.
  *
  * @param label how the resource is named in user-facing messages ("dataset", "model")
  * @tparam R record type of the resource table
  * @tparam A record type of the companion user-access table
  */
case class ResourceTables[R <: Record, A <: Record](
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

object ResourceTables {

  val Dataset: ResourceTables[DatasetRecord, DatasetUserAccessRecord] =
    ResourceTables(
      label = "dataset",
      idField = DATASET.DID,
      ownerUidField = DATASET.OWNER_UID,
      nameField = DATASET.NAME,
      isPublicField = DATASET.IS_PUBLIC,
      accessIdField = DATASET_USER_ACCESS.DID,
      accessUidField = DATASET_USER_ACCESS.UID,
      privilegeField = DATASET_USER_ACCESS.PRIVILEGE
    )

  val Model: ResourceTables[ModelRecord, ModelUserAccessRecord] =
    ResourceTables(
      label = "model",
      idField = MODEL.MID,
      ownerUidField = MODEL.OWNER_UID,
      nameField = MODEL.NAME,
      isPublicField = MODEL.IS_PUBLIC,
      accessIdField = MODEL_USER_ACCESS.MID,
      accessUidField = MODEL_USER_ACCESS.UID,
      privilegeField = MODEL_USER_ACCESS.PRIVILEGE
    )

}
