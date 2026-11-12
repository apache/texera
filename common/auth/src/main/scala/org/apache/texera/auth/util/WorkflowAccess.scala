// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.texera.auth.util

import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables._
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.jooq.impl.DSL
import org.jooq.{DSLContext, Record1, Select}

object WorkflowAccess {
  private def context: DSLContext =
    SqlServer
      .getInstance()
      .createDSLContext()

  /**
    * Identifies whether the given user has read-only access over the given workflow
    *
    * @param wid workflow id
    * @param uid user id, works with workflow id as primary keys in database
    * @return boolean value indicating yes/no
    */
  def hasReadAccess(wid: Integer, uid: Integer): Boolean = {
    isPublic(wid) || !getPrivilege(wid, uid).eq(PrivilegeEnum.NONE)
  }

  /**
    * Identifies whether the given user has write access over the given workflow
    *
    * @param wid workflow id
    * @param uid user id, works with workflow id as primary keys in database
    * @return boolean value indicating yes/no
    */
  def hasWriteAccess(wid: Integer, uid: Integer): Boolean = {
    getPrivilege(wid, uid).eq(PrivilegeEnum.WRITE)
  }

  /**
    * @param wid workflow id
    * @param uid user id, works with workflow id as primary keys in database
    * @return PrivilegeEnum value indicating NONE/READ/WRITE
    */
  def getPrivilege(wid: Integer, uid: Integer): PrivilegeEnum = {
    // One round trip: the caller's own ownership row and grant, read together. Both joins are
    // narrowed to the caller, so a workflow with several owners still yields one row.
    val row = context
      .select(WORKFLOW_OF_USER.UID, WORKFLOW_USER_ACCESS.PRIVILEGE)
      .from(WORKFLOW)
      .leftJoin(WORKFLOW_OF_USER)
      .on(WORKFLOW_OF_USER.WID.eq(WORKFLOW.WID).and(WORKFLOW_OF_USER.UID.eq(uid)))
      .leftJoin(WORKFLOW_USER_ACCESS)
      .on(WORKFLOW_USER_ACCESS.WID.eq(WORKFLOW.WID).and(WORKFLOW_USER_ACCESS.UID.eq(uid)))
      .where(WORKFLOW.WID.eq(wid))
      .fetchOne()
    if (row == null) PrivilegeEnum.NONE
    else OwnerAccess.privilege(row.value1() != null, row.value2())
  }

  def isOwner(wid: Integer, uid: Integer): Boolean =
    context.fetchExists(
      WORKFLOW_OF_USER,
      WORKFLOW_OF_USER.WID.eq(wid).and(WORKFLOW_OF_USER.UID.eq(uid))
    )

  /** Ids of the workflows `uid` owns or was granted. */
  def accessibleWids(uid: Integer): Select[Record1[Integer]] =
    DSL
      .select(WORKFLOW_OF_USER.WID)
      .from(WORKFLOW_OF_USER)
      .where(WORKFLOW_OF_USER.UID.eq(uid))
      .union(
        DSL
          .select(WORKFLOW_USER_ACCESS.WID)
          .from(WORKFLOW_USER_ACCESS)
          .where(WORKFLOW_USER_ACCESS.UID.eq(uid))
      )

  /**
    * Whether the user was granted access to the workflow itself, rather than merely being able to
    * read it because it is public. Granted access sees the author's working copy; public access is
    * held at the pinned copy while one is pinned.
    */
  def hasGrantedAccess(wid: Integer, uid: Integer): Boolean = {
    !getPrivilege(wid, uid).eq(PrivilegeEnum.NONE)
  }

  def isPublic(wid: Integer): Boolean = {
    context
      .select(WORKFLOW.IS_PUBLIC)
      .from(WORKFLOW)
      .where(WORKFLOW.WID.eq(wid))
      .fetchOneInto(classOf[Boolean])
  }
}
