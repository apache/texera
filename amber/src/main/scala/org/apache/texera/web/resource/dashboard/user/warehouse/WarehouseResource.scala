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

package org.apache.texera.web.resource.dashboard.user.warehouse

import com.typesafe.scalalogging.LazyLogging
import io.dropwizard.auth.Auth
import org.apache.texera.amber.core.storage.VFSURIFactory
import org.apache.texera.auth.SessionUser
import org.apache.texera.common.config.StorageConfig
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables.{USER, USER_WAREHOUSE}
import org.apache.texera.dao.jooq.generated.enums.UserWarehouseFlavorEnum
import org.apache.texera.dao.jooq.generated.tables.records.UserWarehouseRecord
import org.apache.texera.web.resource.dashboard.user.warehouse.WarehouseResource._
import org.apache.texera.web.service.LakekeeperClient

import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import scala.jdk.CollectionConverters.ListHasAsScala

object WarehouseResource {
  private def context =
    SqlServer
      .getInstance()
      .createDSLContext()

  // A warehouse's user-facing name becomes part of the Lakekeeper catalog name
  // `user-<uid>-<name>`, which in turn becomes a VFS URI path segment — so the
  // character rule is delegated to VFSURIFactory (the layer that parses it); the
  // length cap is this registration layer's own constraint.
  private[warehouse] def isValidWarehouseName(name: String): Boolean =
    name.length <= 64 && VFSURIFactory.isValidWarehouseName(name)

  case class DashboardWarehouse(
      whid: Integer,
      name: String,
      warehouseName: String,
      flavor: String,
      createdAtMillis: Long,
      // Owner display info, mirroring DashboardWorkflowComputingUnit: today every
      // warehouse belongs to the caller, but the UI binds to the entry rather than
      // the session user so shared warehouses render the right person (#7743).
      ownerName: String,
      ownerAvatar: String
  )

  // (name, avatar) per uid; null when the user has no name / avatar set, matching
  // how computing units resolve their owner info.
  private def resolveOwners(uids: Seq[Integer]): Map[Integer, (String, String)] =
    if (uids.isEmpty) Map.empty
    else
      context
        .select(USER.UID, USER.NAME, USER.AVATAR)
        .from(USER)
        .where(USER.UID.in(uids: _*))
        .fetch()
        .asScala
        .map(r =>
          r.get(USER.UID) -> (
            (
              Option(r.get(USER.NAME)).filter(_.nonEmpty).orNull,
              Option(r.get(USER.AVATAR)).filter(_.nonEmpty).orNull
            )
          )
        )
        .toMap

  private def toDashboardWarehouse(
      row: UserWarehouseRecord,
      owner: (String, String)
  ): DashboardWarehouse =
    DashboardWarehouse(
      row.getWhid,
      row.getName,
      row.getWarehouseName,
      row.getFlavor.getLiteral,
      row.getCreatedAt.toInstant.toEpochMilli,
      ownerName = owner._1,
      ownerAvatar = owner._2
    )

  case class WarehouseStatus(enabled: Boolean, warehouses: List[DashboardWarehouse])

  case class CreateWarehouseRequest(name: String)
}

/**
  * Per-user warehouse management (#6870): list the feature state and the caller's
  * warehouses, create a Local-flavor warehouse on the deployment's own object store,
  * and delete one (empty-first in Lakekeeper, purging its data files).
  *
  * Everything except `/status` is gated by the warehouse feature flag; the mutating
  * endpoints return 403 while it is off. `/status` always answers so the frontend can
  * decide whether to show the feature at all.
  */
@Path("/warehouse")
@Produces(Array(MediaType.APPLICATION_JSON))
class WarehouseResource(client: LakekeeperClient, enabled: Boolean) extends LazyLogging {

  // Jersey builds the resource through this constructor. The flag is captured once,
  // which is equivalent to reading it per call: storage.conf is resolved at class load
  // and never changes at runtime.
  def this() = this(new LakekeeperClient(), StorageConfig.warehouseEnabled)

  @GET
  @Path("/status")
  def status(@Auth current_user: SessionUser): WarehouseStatus = {
    if (!enabled) {
      return WarehouseStatus(enabled = false, warehouses = List())
    }
    val rows = context
      .selectFrom(USER_WAREHOUSE)
      .where(USER_WAREHOUSE.UID.eq(current_user.getUid))
      .orderBy(USER_WAREHOUSE.CREATED_AT.asc())
      .fetch()
      .asScala
      .toList
    val owners = resolveOwners(rows.map(_.getUid).distinct)
    WarehouseStatus(
      enabled = true,
      warehouses =
        rows.map(row => toDashboardWarehouse(row, owners.getOrElse(row.getUid, (null, null))))
    )
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def create(
      request: CreateWarehouseRequest,
      @Auth current_user: SessionUser
  ): DashboardWarehouse = {
    requireEnabled()
    val name = Option(request.name).map(_.trim).getOrElse("")
    if (!isValidWarehouseName(name)) {
      throw new BadRequestException(
        "warehouse name must start with a letter or digit and contain only letters, " +
          "digits, '-' and '_' (at most 64 characters)"
      )
    }
    val uid = current_user.getUid
    if (
      context.fetchExists(
        context
          .selectFrom(USER_WAREHOUSE)
          .where(USER_WAREHOUSE.UID.eq(uid).and(USER_WAREHOUSE.NAME.eq(name)))
      )
    ) {
      throw new WebApplicationException(s"a warehouse named '$name' already exists", 409)
    }

    val warehouseName = s"user-$uid-$name"
    // Create in Lakekeeper first, record after: a failed creation leaves no orphaned row.
    val warehouseId =
      try {
        client.createWarehouse(warehouseName)
      } catch {
        case e: Exception =>
          throw new WebApplicationException(e.getMessage, 502)
      }

    val row = context.newRecord(USER_WAREHOUSE)
    row.setUid(uid)
    row.setName(name)
    row.setWarehouseName(warehouseName)
    row.setLakekeeperWarehouseId(warehouseId)
    row.setFlavor(UserWarehouseFlavorEnum.local)
    row.setS3Bucket(StorageConfig.icebergRESTCatalogS3Bucket)
    row.setS3Endpoint(StorageConfig.s3Endpoint)
    row.setS3Region(StorageConfig.s3Region)
    try {
      row.store()
      // created_at is filled by the DB default; fetch it back before serializing.
      row.refresh()
    } catch {
      case e: Exception =>
        // Compensate: without the row the user could neither list nor delete the
        // just-created warehouse, so remove it (it is empty at this point).
        try {
          client.deleteWarehouseEmptyFirst(warehouseId)
        } catch {
          case cleanup: Exception =>
            logger.error(
              s"failed to clean up Lakekeeper warehouse $warehouseId after a failed create",
              cleanup
            )
        }
        throw new WebApplicationException(e.getMessage, 500)
    }
    toDashboardWarehouse(row, resolveOwners(Seq(uid)).getOrElse(uid, (null, null)))
  }

  @DELETE
  @Path("/{whid}")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def delete(@PathParam("whid") whid: Integer, @Auth current_user: SessionUser): Unit = {
    requireEnabled()
    val row = context
      .selectFrom(USER_WAREHOUSE)
      .where(USER_WAREHOUSE.WHID.eq(whid).and(USER_WAREHOUSE.UID.eq(current_user.getUid)))
      .fetchOne()
    if (row == null) {
      throw new NotFoundException(s"no warehouse with id $whid")
    }
    try {
      client.deleteWarehouseEmptyFirst(row.getLakekeeperWarehouseId)
    } catch {
      case e: Exception =>
        throw new WebApplicationException(e.getMessage, 502)
    }
    context
      .deleteFrom(USER_WAREHOUSE)
      .where(USER_WAREHOUSE.WHID.eq(whid))
      .execute()
  }

  private def requireEnabled(): Unit =
    if (!enabled) {
      throw new ForbiddenException("per-user warehouses are disabled in this deployment")
    }
}
