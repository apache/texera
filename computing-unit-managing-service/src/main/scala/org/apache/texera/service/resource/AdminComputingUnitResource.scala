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

// Nested under /computing-unit rather than the /admin/<domain> shape used by AdminUserResource and
// AdminExecutionResource: the gateway only routes /api/computing-unit and
// /api/access/computing-unit to this service, so /api/admin/... would fall through to the
// /api catch-all instead. Jersey resolves /computing-unit/admin/list here rather than against
// ComputingUnitManagingResource's @Path("/{cuid}"), because a literal path segment outranks a
// template one; nothing else may add a two-segment literal path under /computing-unit without
// re-checking that.
@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/computing-unit/admin")
@RolesAllowed(Array("ADMIN"))
class AdminComputingUnitResource {

  import AdminComputingUnitResource._

  /**
    * List every non-terminated computing unit across all users (ADMIN-only).
    *
    * NOTE: every row is reported as WRITE, but nothing yet honours that on the write side. The
    * mutating endpoints on [[ComputingUnitManagingResource]] gate on ownership alone
    * (`userOwnComputingUnit`, or `ComputingUnitAccessResource.hasWriteAccess`, neither of which
    * has an ADMIN bypass), so an admin acting on a unit it does not own gets 400/403 from
    * terminate, rename, /metrics and /limits. A client must not present these rows as writable
    * until those endpoints grow an admin bypass.
    */
  @GET
  @Path("/list")
  def listAllComputingUnits(
      @Auth user: SessionUser
  ): List[DashboardWorkflowComputingUnit] = {
    val ctx = context

    // Filter to active units in SQL so terminated rows are never loaded. Spelled as an explicit
    // `IS NULL` predicate rather than the DAO's fetchByTerminateTime(null), which would render
    // `terminate_time IN (null)` and match no row at all.
    val activeUnits =
      ctx
        .selectFrom(WORKFLOW_COMPUTING_UNIT)
        .where(WORKFLOW_COMPUTING_UNIT.TERMINATE_TIME.isNull)
        .fetchInto(classOf[WorkflowComputingUnit])
        .asScala
        .toList

    // Same reconcile-then-render sequence as ComputingUnitManagingResource.listComputingUnits, but
    // the Kubernetes round trips deliberately stay outside the transaction so a pooled connection
    // is not held open across them. Only the write is wrapped: reconcileVanishedKubernetesUnits
    // retires vanished units via jOOQ's batchUpdate, which under autocommit commits per
    // statement — a mid-batch failure would otherwise leave some units retired and the rest not.

    // Pod phases decide which Kubernetes units are still alive.
    val podPhases = ComputingUnitHelpers.podPhasesFor(activeUnits)

    val liveUnits = SqlServer.withTransaction(ctx) { txCtx =>
      ComputingUnitHelpers.reconcileVanishedKubernetesUnits(
        new WorkflowComputingUnitDao(txCtx.configuration()),
        activeUnits,
        podPhases
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
        accessPrivilege = PrivilegeEnum.WRITE,
        ownerInfo = ownerInfo,
        podPhases = podPhases,
        podMetrics = podMetrics
      )
    }
  }
}
