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

package edu.uci.ics.texera.auth.util

import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.dao.jooq.generated.enums.PrivilegeEnum
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.{
  ComputingUnitUserAccessDao,
  WorkflowComputingUnitDao
}
import ComputingUnitAccess._
import org.jooq.DSLContext

import scala.jdk.CollectionConverters._

object ComputingUnitAccess {
  private lazy val context: DSLContext = SqlServer
    .getInstance()
    .createDSLContext()

  def getComputingUnitAccess(cuid: Integer, uid: Integer): PrivilegeEnum = {
    val workflowComputingUnitDao = new WorkflowComputingUnitDao(context.configuration())
    val unit = workflowComputingUnitDao.fetchOneByCuid(cuid)

    if (unit.getUid.equals(uid)) {
      return PrivilegeEnum.WRITE // owner has write access
    }

    val computingUnitUserAccessDao = new ComputingUnitUserAccessDao(context.configuration())
    val accessOpt = computingUnitUserAccessDao
      .fetchByUid(uid)
      .asScala
      .find(_.getCuid.equals(cuid))

    accessOpt match {
      case Some(access) => access.getPrivilege
      case None         => PrivilegeEnum.NONE
    }
  }
}
