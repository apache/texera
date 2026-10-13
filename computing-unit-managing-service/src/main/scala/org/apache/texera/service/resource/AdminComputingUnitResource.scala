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
}

// Path is /computing-unit/admin, not the /admin/<domain> shape other admin resources use: the
// gateway only routes /api/computing-unit here, so /api/admin/... never reaches this service.
// Jersey matches the literal /admin/list ahead of ComputingUnitManagingResource's @Path("/{cuid}"),
// so don't add another two-segment literal path under /computing-unit without re-checking this.
@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/computing-unit/admin")
@RolesAllowed(Array("ADMIN"))
class AdminComputingUnitResource {

  import AdminComputingUnitResource._

  /**
    * List every non-terminated computing unit across all users (ADMIN-only).
    */
  @GET
  @Path("/list")
  def listAllComputingUnits(
      @Auth user: SessionUser
  ): List[DashboardWorkflowComputingUnit] = {
    val ctx = context

    // Filter active units in SQL. Explicit `IS NULL`, not the DAO's fetchByTerminateTime(null),
    // which renders `terminate_time IN (null)` and matches nothing.
    val activeUnits =
      ctx
        .selectFrom(WORKFLOW_COMPUTING_UNIT)
        .where(WORKFLOW_COMPUTING_UNIT.TERMINATE_TIME.isNull)
        .fetchInto(classOf[WorkflowComputingUnit])
        .asScala
        .toList

    val podSnapshots = ComputingUnitHelpers.podSnapshotsFor(activeUnits)

    // Wrap only the reconcile write, not the Kubernetes round trips above: keeps a pooled
    // connection from being held open across them, while the transaction makes the batchUpdate
    // all-or-nothing (autocommit would retire only some vanished units on a mid-batch failure).

    val liveUnits = SqlServer.withTransaction(ctx) { txCtx =>
      ComputingUnitHelpers.reconcileVanishedKubernetesUnits(
        new WorkflowComputingUnitDao(txCtx.configuration()),
        activeUnits,
        podSnapshots
      )
    }

    // Metrics only for survivors, so fetch after reconciliation.
    val podMetrics = ComputingUnitHelpers.podMetricsFor(liveUnits)

    val userDao = new UserDao(ctx.configuration())
    val ownerInfo = ComputingUnitHelpers.resolveOwnerInfo(userDao, liveUnits.map(_.getUid).distinct)

    liveUnits.map { unit =>
      ComputingUnitHelpers.buildDashboardUnit(
        unit,
        isOwner = unit.getUid.equals(user.getUid),
        canViewStatusReason = true,
        accessPrivilege = PrivilegeEnum.WRITE,
        ownerInfo = ownerInfo,
        podSnapshots = podSnapshots,
        podMetrics = podMetrics
      )
    }
  }
}
