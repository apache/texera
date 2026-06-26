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

package org.apache.texera.service.resource

import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables.WORKFLOW_USER_ACCESS
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.jooq.DSLContext

object WorkflowAccessResource {
  private def context: DSLContext =
    SqlServer.getInstance().createDSLContext()

  /**
    * Whether the given user holds a direct WRITE grant on the given workflow.
    *
    * Only the direct WORKFLOW_USER_ACCESS grant is consulted — no project or
    * public-visibility fallback — so the notebook endpoints stay self-contained.
    *
    * @param wid workflow id
    * @param uid user id
    */
  def hasWriteAccess(wid: Integer, uid: Integer): Boolean = {
    val privilege = context
      .select(WORKFLOW_USER_ACCESS.PRIVILEGE)
      .from(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.WID.eq(wid).and(WORKFLOW_USER_ACCESS.UID.eq(uid)))
      .fetchOne(WORKFLOW_USER_ACCESS.PRIVILEGE)
    privilege == PrivilegeEnum.WRITE
  }
}
