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
import org.apache.texera.dao.jooq.generated.Tables.{
  COMPUTING_UNIT_USER_ACCESS,
  WORKFLOW_COMPUTING_UNIT
}
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.jooq.DSLContext

object ComputingUnitAccess {
  private def context: DSLContext =
    SqlServer
      .getInstance()
      .createDSLContext()

  def getComputingUnitAccess(cuid: Integer, uid: Integer): PrivilegeEnum = {
    // At most one row: cuid is the PK of workflow_computing_unit and (cuid, uid)
    // is the PK of computing_unit_user_access, so the left join cannot fan out.
    val record = context
      .select(WORKFLOW_COMPUTING_UNIT.UID, COMPUTING_UNIT_USER_ACCESS.PRIVILEGE)
      .from(WORKFLOW_COMPUTING_UNIT)
      .leftJoin(COMPUTING_UNIT_USER_ACCESS)
      .on(
        COMPUTING_UNIT_USER_ACCESS.CUID
          .eq(WORKFLOW_COMPUTING_UNIT.CUID)
          .and(COMPUTING_UNIT_USER_ACCESS.UID.eq(uid))
      )
      .where(WORKFLOW_COMPUTING_UNIT.CUID.eq(cuid))
      .fetchOne()

    if (record == null) {
      PrivilegeEnum.NONE // no such unit
    } else if (record.value1().equals(uid)) {
      PrivilegeEnum.WRITE // owner
    } else {
      Option(record.value2()).getOrElse(PrivilegeEnum.NONE)
    }
  }
}
