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
import org.apache.texera.dao.jooq.generated.Tables.WORKFLOW_COMPUTING_UNIT
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
    * Render already-active `units` as admin dashboard rows (pure — no filtering). Every row gets
    * WRITE access (an admin controls every unit it sees); `isOwner` reflects `callerUid`.
    */
  def buildDashboardUnits(
      units: List[WorkflowComputingUnit],
      ownerInfo: Map[Integer, (String, String)],
      callerUid: Integer,
      podPhases: Map[String, String],
      podMetrics: Map[String, Map[String, String]]
  ): List[DashboardWorkflowComputingUnit] =
    units.map { unit =>
      ComputingUnitHelpers.buildDashboardUnit(
        unit,
        isOwner = unit.getUid.equals(callerUid),
        accessPrivilege = PrivilegeEnum.WRITE,
        ownerInfo = ownerInfo,
        podPhases = podPhases,
        podMetrics = podMetrics
      )
    }
}

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/computing-unit/admin")
@RolesAllowed(Array("ADMIN"))
class AdminComputingUnitResource {

  import AdminComputingUnitResource._

  /**
    * List every non-terminated computing unit across all users (ADMIN-only). Like the per-user
    * endpoint, a Kubernetes unit whose pod has vanished is marked terminated and dropped, so ghost
    * units don't accumulate. Status/metrics use one namespace-wide `list`/`top` each (O(1) round trips).
    */
  @GET
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/list")
  def listAllComputingUnits(
      @Auth user: SessionUser
  ): List[DashboardWorkflowComputingUnit] = {
    val ctx = context

    // Filter to active units in SQL so terminated rows are never loaded.
    val activeUnits: List[WorkflowComputingUnit] =
      ctx
        .selectFrom(WORKFLOW_COMPUTING_UNIT)
        .where(WORKFLOW_COMPUTING_UNIT.TERMINATE_TIME.isNull)
        .fetchInto(classOf[WorkflowComputingUnit])
        .asScala
        .toList

    // Pod phases decide which Kubernetes units are still alive.
    val podPhases = ComputingUnitHelpers.podPhasesFor(activeUnits)

    val liveUnits = ComputingUnitHelpers.reconcileVanishedKubernetesUnits(
      new WorkflowComputingUnitDao(ctx.configuration()),
      activeUnits,
      podPhases
    )

    // Metrics only for survivors, so fetch after reconciliation.
    val podMetrics = ComputingUnitHelpers.podMetricsFor(liveUnits)

    val userDao = new UserDao(ctx.configuration())
    val ownerInfo = ComputingUnitHelpers.resolveOwnerInfo(userDao, liveUnits.map(_.getUid).distinct)

    buildDashboardUnits(liveUnits, ownerInfo, user.getUid, podPhases, podMetrics)
  }
}
