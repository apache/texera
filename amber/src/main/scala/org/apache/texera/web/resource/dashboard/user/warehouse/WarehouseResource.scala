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
import org.apache.commons.lang3.StringUtils
import org.apache.texera.amber.core.storage.VFSURIFactory
import org.apache.texera.auth.SessionUser
import org.apache.texera.common.config.StorageConfig
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables.{USER, USER_WAREHOUSE}
import org.apache.texera.dao.jooq.generated.enums.UserWarehouseFlavorEnum
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.apache.texera.dao.jooq.generated.tables.records.UserWarehouseRecord
import org.apache.texera.web.resource.dashboard.user.warehouse.WarehouseResource._
import org.apache.texera.web.service.LakekeeperClient
import org.jooq.DSLContext
import org.jooq.impl.DSL

import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import scala.jdk.CollectionConverters.ListHasAsScala

object WarehouseResource {
  private def context =
    SqlServer
      .getInstance()
      .createDSLContext()

  // The display name no longer reaches Lakekeeper or any URI — the catalog name is
  // minted from uid and whid — but it stays under VFSURIFactory's character rule
  // rather than growing a second charset; the length cap is this registration
  // layer's own constraint.
  private[warehouse] def isValidWarehouseName(name: String): Boolean =
    name.length <= 64 && VFSURIFactory.isValidWarehouseName(name)

  // The Lakekeeper catalog name minted for a warehouse row: stable, short, and
  // mapping straight back to the row.
  private[warehouse] def lakekeeperWarehouseName(uid: Integer, whid: Integer): String =
    s"user-$uid-$whid"

  // Draws the id the next insert will use from the table's sequence, without consuming
  // an insert. The sequence is looked up rather than named literally -- its generated
  // name is not a stable contract.
  private[warehouse] def drawNextWhid(context: DSLContext): Integer =
    context.fetchValue(
      DSL.field(
        "nextval(pg_get_serial_sequence({0}, {1}))",
        classOf[Integer],
        DSL.inline(s"${USER_WAREHOUSE.getSchema.getName}.${USER_WAREHOUSE.getName}"),
        DSL.inline(USER_WAREHOUSE.WHID.getName)
      )
    )

  case class DashboardWarehouse(
      whid: Integer,
      // The display name, unique per user and free to change.
      name: String,
      // The Lakekeeper catalog name, `user-<uid>-<whid>`.
      lakekeeperWarehouseName: String,
      flavor: String,
      createdAtMillis: Long,
      // Owner display info, mirroring DashboardWorkflowComputingUnit: today every
      // warehouse belongs to the caller, but the UI binds to the entry rather than
      // the session user so shared warehouses render the right person (#7743).
      ownerName: String,
      ownerAvatar: String
  )

  // (name, avatar), null for either when the user has not set it.
  private type Owner = (String, String)

  private def ownerOf(name: String, avatar: String): Owner =
    (StringUtils.trimToNull(name), StringUtils.trimToNull(avatar))

  private def ownerOf(user: User): Owner = ownerOf(user.getName, user.getAvatar)

  private def toDashboardWarehouse(
      row: UserWarehouseRecord,
      owner: Owner
  ): DashboardWarehouse =
    DashboardWarehouse(
      row.getWhid,
      row.getName,
      row.getLakekeeperWarehouseName,
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
    // Joined rather than resolved in a second query: one round trip, and every row
    // carries its own owner once warehouses can be shared.
    val rows = context
      .select(USER_WAREHOUSE.fields() ++ Seq(USER.NAME, USER.AVATAR): _*)
      .from(USER_WAREHOUSE)
      .leftJoin(USER)
      .on(USER.UID.eq(USER_WAREHOUSE.UID))
      .where(USER_WAREHOUSE.UID.eq(current_user.getUid))
      .orderBy(USER_WAREHOUSE.CREATED_AT.asc())
      .fetch()
      .asScala
      .toList
    WarehouseStatus(
      enabled = true,
      warehouses = rows.map(r =>
        toDashboardWarehouse(
          r.into(USER_WAREHOUSE),
          ownerOf(r.get(USER.NAME), r.get(USER.AVATAR))
        )
      )
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

    // Never derive the catalog name from `name`: it is also the S3 key prefix and a
    // component of every result URI an execution wrote, so a name-derived one could
    // never change again. Drawing the id up front keeps the creation order below intact.
    val whid: Integer = drawNextWhid(context)
    val mintedName = lakekeeperWarehouseName(uid, whid)
    // Create in Lakekeeper first, record after: a failed creation leaves no orphaned row.
    val lakekeeperWarehouseId =
      try {
        client.createWarehouse(mintedName)
      } catch {
        case e: Exception =>
          throw new WebApplicationException(e.getMessage, 502)
      }

    val row = context.newRecord(USER_WAREHOUSE)
    row.setWhid(whid)
    row.setUid(uid)
    row.setName(name)
    row.setLakekeeperWarehouseName(mintedName)
    row.setLakekeeperWarehouseId(lakekeeperWarehouseId)
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
          client.deleteWarehouseEmptyFirst(lakekeeperWarehouseId)
        } catch {
          case cleanup: Exception =>
            logger.error(
              s"failed to clean up Lakekeeper warehouse $lakekeeperWarehouseId after a failed create",
              cleanup
            )
        }
        throw new WebApplicationException(e.getMessage, 500)
    }
    // The caller owns what they just created, and SessionUser already carries their
    // display info -- no lookup needed.
    toDashboardWarehouse(row, ownerOf(current_user.getUser))
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
