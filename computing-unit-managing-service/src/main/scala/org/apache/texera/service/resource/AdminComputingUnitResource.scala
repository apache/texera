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

import io.dropwizard.auth.Auth
import jakarta.annotation.security.RolesAllowed
import jakarta.ws.rs.{GET, Path, Produces}
import jakarta.ws.rs.core.MediaType
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.SqlServer.withTransaction
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.daos.{UserDao, WorkflowComputingUnitDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.WorkflowComputingUnit
import org.apache.texera.service.resource.ComputingUnitManagingResource.DashboardWorkflowComputingUnit
import org.apache.texera.service.util.ComputingUnitHelpers
import org.jooq.DSLContext

import scala.jdk.CollectionConverters.CollectionHasAsScala

object AdminComputingUnitResource {
  private def context: DSLContext =
    SqlServer
      .getInstance()
      .createDSLContext()

  /**
    * Assemble the dashboard rows for the admin view from the raw computing units and their
    * owners' display info. Terminated units (non-null `terminate_time`) are excluded, and the
    * remaining units keep the same [[DashboardWorkflowComputingUnit]] shape produced by the
    * per-user listing endpoint.
    *
    * @param units     all computing units to consider (across every owner)
    * @param ownerInfo map of owner uid -> (googleAvatar, userName)
    * @param callerUid the uid of the requesting admin, used to populate `isOwner`
    */
  def buildDashboardUnits(
      units: List[WorkflowComputingUnit],
      ownerInfo: Map[Integer, (String, String)],
      callerUid: Integer
  ): List[DashboardWorkflowComputingUnit] = {
    units
      .filter(_.getTerminateTime == null)
      .map { unit =>
        val (avatar, name) = ownerInfo.getOrElse(unit.getUid, (null, null))
        DashboardWorkflowComputingUnit(
          computingUnit = unit,
          status = ComputingUnitHelpers.getComputingUnitStatus(unit).toString,
          metrics = ComputingUnitHelpers.getComputingUnitMetrics(unit),
          isOwner = unit.getUid.equals(callerUid),
          // Admins have full control over every computing unit they can see.
          accessPrivilege = PrivilegeEnum.WRITE,
          ownerGoogleAvatar = avatar,
          ownerName = name
        )
      }
  }
}

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/computing-unit/admin")
@RolesAllowed(Array("ADMIN"))
class AdminComputingUnitResource {

  import AdminComputingUnitResource._

  /**
    * List every non-terminated computing unit across all users. ADMIN-only.
    *
    * @return the computing units (owned by any user) that have not been terminated.
    */
  @GET
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/list")
  def listAllComputingUnits(
      @Auth user: SessionUser
  ): List[DashboardWorkflowComputingUnit] = {
    withTransaction(context) { ctx =>
      val computingUnitDao = new WorkflowComputingUnitDao(ctx.configuration())
      val userDao = new UserDao(ctx.configuration())

      val activeUnits =
        computingUnitDao.findAll().asScala.toList.filter(_.getTerminateTime == null)

      val ownerUids: List[Integer] = activeUnits.map(_.getUid).distinct
      val ownerInfo: Map[Integer, (String, String)] =
        if (ownerUids.isEmpty) Map.empty
        else
          userDao
            .fetchByUid(ownerUids: _*)
            .asScala
            .map { u =>
              val avatar = Option(u.getGoogleAvatar).filter(_.nonEmpty).orNull
              val name = Option(u.getName).filter(_.nonEmpty).orNull
              u.getUid -> (avatar, name)
            }
            .toMap

      buildDashboardUnits(activeUnits, ownerInfo, user.getUid)
    }
  }
}
