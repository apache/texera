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
import org.apache.texera.auth.SessionUser
import org.apache.texera.common.config.StorageConfig
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables.USER_WAREHOUSE
import org.apache.texera.dao.jooq.generated.enums.UserWarehouseFlavorEnum
import org.apache.texera.web.resource.dashboard.user.warehouse.WarehouseResource._
import org.apache.texera.web.service.LakekeeperClient

import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.MediaType

object WarehouseResource {
  private def context =
    SqlServer
      .getInstance()
      .createDSLContext()

  // A warehouse's user-facing name becomes part of the Lakekeeper catalog name
  // `user-<uid>-<name>`, which in turn becomes a VFS URI path segment — so it is
  // restricted to the same characters VFSURIFactory accepts for a warehouse name.
  private val warehouseNamePattern = "[A-Za-z0-9][A-Za-z0-9_-]*".r

  private[warehouse] def isValidWarehouseName(name: String): Boolean =
    name.length <= 64 && warehouseNamePattern.pattern.matcher(name).matches()

  case class DashboardWarehouse(
      whid: Integer,
      name: String,
      warehouseName: String,
      flavor: String,
      createdAtMillis: Long
  )

  case class WarehouseStatus(enabled: Boolean, warehouses: List[DashboardWarehouse])

  case class CreateWarehouseRequest(name: String)
}

/**
  * Per-user warehouse management (#6870): list the feature state and the caller's
  * warehouses, create a Local-flavor warehouse on the deployment's own object store,
  * and delete one (empty-first in Lakekeeper, purging its data files).
  *
  * Everything except `/status` is gated by [[StorageConfig.warehouseEnabled]]; the
  * mutating endpoints return 403 while the flag is off. `/status` always answers so the
  * frontend can decide whether to show the feature at all.
  */
@Path("/warehouse")
@Produces(Array(MediaType.APPLICATION_JSON))
class WarehouseResource(client: LakekeeperClient) extends LazyLogging {

  def this() = this(new LakekeeperClient())

  @GET
  @Path("/status")
  def status(@Auth current_user: SessionUser): WarehouseStatus = {
    if (!StorageConfig.warehouseEnabled) {
      return WarehouseStatus(enabled = false, warehouses = List())
    }
    val warehouses = context
      .selectFrom(USER_WAREHOUSE)
      .where(USER_WAREHOUSE.UID.eq(current_user.getUid))
      .orderBy(USER_WAREHOUSE.CREATED_AT.asc())
      .fetch()
      .map(row =>
        DashboardWarehouse(
          row.getWhid,
          row.getName,
          row.getWarehouseName,
          row.getFlavor.getLiteral,
          row.getCreatedAt.toInstant.toEpochMilli
        )
      )
    WarehouseStatus(
      enabled = true,
      warehouses = warehouses.toArray(Array[DashboardWarehouse]()).toList
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
    DashboardWarehouse(
      row.getWhid,
      row.getName,
      row.getWarehouseName,
      row.getFlavor.getLiteral,
      row.getCreatedAt.toInstant.toEpochMilli
    )
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
    if (!StorageConfig.warehouseEnabled) {
      throw new ForbiddenException("per-user warehouses are disabled in this deployment")
    }
}
