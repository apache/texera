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
    * Assemble the admin dashboard rows for a set of active computing units. The `units` are
    * expected to already be the active (non-terminated, pod-still-present) set; this is a pure
    * mapping and does no filtering of its own. Every row is marked with WRITE access — admins
    * have full control over every unit they can see — and `isOwner` reflects the requesting
    * admin. Kubernetes status/metrics are resolved from the pre-fetched maps (no per-unit call).
    *
    * @param units      active computing units to render (across every owner)
    * @param ownerInfo  map of owner uid -> (googleAvatar, userName)
    * @param callerUid  the uid of the requesting admin, used to populate `isOwner`
    * @param podPhases  map of pod name -> phase (see KubernetesClient.getAllPodPhases)
    * @param podMetrics map of pod name -> (metric -> value) (see KubernetesClient.getAllPodMetrics)
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
    * List every non-terminated computing unit across all users. ADMIN-only.
    *
    * Mirrors the reconciliation done by the per-user listing endpoint: a Kubernetes unit whose
    * pod has vanished (manually deleted or TTL GC-ed by the cluster) is eagerly marked
    * terminated in the database and excluded from the response, so ghost units do not
    * accumulate in the admin view.
    *
    * Kubernetes status/metrics are resolved from a single namespace-wide `list`/`top` call each,
    * so the number of cluster round trips is constant rather than proportional to the number of
    * units.
    *
    * @return the computing units (owned by any user) that are active and whose pods still exist.
    */
  @GET
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/list")
  def listAllComputingUnits(
      @Auth user: SessionUser
  ): List[DashboardWorkflowComputingUnit] = {
    val ctx = context

    // Filter to active units in SQL so historically-terminated rows are never loaded.
    val activeUnits: List[WorkflowComputingUnit] =
      ctx
        .selectFrom(WORKFLOW_COMPUTING_UNIT)
        .where(WORKFLOW_COMPUTING_UNIT.TERMINATE_TIME.isNull)
        .fetchInto(classOf[WorkflowComputingUnit])
        .asScala
        .toList

    // Pod phases (one namespace-wide `list`) are needed to decide which units are still alive;
    // only fetched when there is a Kubernetes unit to resolve.
    val podPhases = ComputingUnitHelpers.podPhasesFor(activeUnits)

    // A Kubernetes unit whose pod is gone is stamped terminated and dropped from the response.
    val liveUnits = ComputingUnitHelpers.reconcileVanishedKubernetesUnits(
      new WorkflowComputingUnitDao(ctx.configuration()),
      activeUnits,
      podPhases
    )

    // Metrics (one namespace-wide `top`) are only rendered for surviving units, so this is
    // deferred until after reconciliation and skipped when no live Kubernetes unit remains.
    val podMetrics = ComputingUnitHelpers.podMetricsFor(liveUnits)

    val userDao = new UserDao(ctx.configuration())
    val ownerInfo = ComputingUnitHelpers.resolveOwnerInfo(userDao, liveUnits.map(_.getUid).distinct)

    buildDashboardUnits(liveUnits, ownerInfo, user.getUid, podPhases, podMetrics)
  }
}
