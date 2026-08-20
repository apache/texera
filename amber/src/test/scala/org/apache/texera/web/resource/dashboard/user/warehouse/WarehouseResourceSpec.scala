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

import org.apache.texera.auth.SessionUser
import org.apache.texera.common.config.StorageConfig
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.USER_WAREHOUSE
import org.apache.texera.dao.jooq.generated.tables.daos.UserDao
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.apache.texera.web.resource.dashboard.user.warehouse.WarehouseResource.{
  lakekeeperWarehouseName,
  CreateWarehouseRequest
}
import org.apache.texera.web.service.LakekeeperClient
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.util.UUID
import javax.ws.rs.{
  BadRequestException,
  ForbiddenException,
  NotFoundException,
  WebApplicationException
}
import scala.collection.mutable

/**
  * Spec for [[WarehouseResource]] (#6932): the disabled-gate behavior, and the
  * create/list/delete flow against MockTexeraDB with a stubbed [[LakekeeperClient]].
  *
  * The feature flag is a constructor dependency, so the two gate states are two resource
  * instances — no global state is touched, and suites cannot interfere with each other.
  */
class WarehouseResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private val createdNames = mutable.Buffer[String]()
  private val deletedIds = mutable.Buffer[UUID]()
  private val stubWarehouseId = UUID.randomUUID()

  @volatile private var createFailure: Option[Exception] = None
  @volatile private var deleteFailure: Option[Exception] = None

  private val stubClient: LakekeeperClient = new LakekeeperClient() {
    override def createWarehouse(warehouseName: String): UUID = {
      createFailure.foreach(throw _)
      createdNames += warehouseName
      stubWarehouseId
    }
    override def deleteWarehouseEmptyFirst(lakekeeperWarehouseId: UUID): Unit = {
      deleteFailure.foreach(throw _)
      deletedIds += lakekeeperWarehouseId
    }
  }

  private val resource = new WarehouseResource(stubClient, enabled = true)
  private val disabledResource = new WarehouseResource(stubClient, enabled = false)
  private var sessionUser: SessionUser = _
  private var otherUser: SessionUser = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()

    val userDao = new UserDao(getDSLContext.configuration())
    val user = new User
    user.setName("warehouse_spec_user")
    user.setEmail(s"user_${UUID.randomUUID()}@example.com")
    userDao.insert(user)
    sessionUser = new SessionUser(user)

    val other = new User
    other.setName("warehouse_spec_other")
    other.setEmail(s"user_${UUID.randomUUID()}@example.com")
    userDao.insert(other)
    otherUser = new SessionUser(other)
  }

  override protected def afterAll(): Unit = closeConnectionPool()

  override protected def beforeEach(): Unit = {
    createFailure = None
    deleteFailure = None
    createdNames.clear()
    deletedIds.clear()
    getDSLContext.deleteFrom(USER_WAREHOUSE).execute()
  }

  // ---------------------------------------------------------------------------
  // Disabled gate
  // ---------------------------------------------------------------------------

  "status" should "report disabled with no warehouses while the flag is off" in {
    val status = disabledResource.status(sessionUser)
    status.enabled shouldBe false
    status.warehouses shouldBe empty
  }

  "create and delete" should "be refused while the flag is off" in {
    a[ForbiddenException] should be thrownBy
      disabledResource.create(CreateWarehouseRequest("mybucket"), sessionUser)
    a[ForbiddenException] should be thrownBy disabledResource.delete(1, sessionUser)
  }

  // ---------------------------------------------------------------------------
  // Create / list / delete
  // ---------------------------------------------------------------------------

  "create" should "create in Lakekeeper, record the row, and mint user-<uid>-<whid>" in {
    val created = resource.create(CreateWarehouseRequest("mybucket"), sessionUser)

    created.name shouldBe "mybucket"
    created.lakekeeperWarehouseName shouldBe lakekeeperWarehouseName(
      sessionUser.getUid,
      created.whid
    )
    created.lakekeeperWarehouseName should not include "mybucket"
    created.flavor shouldBe "local"
    createdNames.toList shouldBe List(lakekeeperWarehouseName(sessionUser.getUid, created.whid))

    val status = resource.status(sessionUser)
    status.enabled shouldBe true
    status.warehouses.map(_.whid) shouldBe List(created.whid)
  }

  it should "mint a fresh catalog name when the same display name is reused" in {
    // A reused catalog name would let the new warehouse inherit the old one's storage
    // path, since stored result URIs embed it.
    val first = resource.create(CreateWarehouseRequest("recycled"), sessionUser)
    resource.delete(first.whid, sessionUser)
    val second = resource.create(CreateWarehouseRequest("recycled"), sessionUser)

    second.name shouldBe first.name
    second.lakekeeperWarehouseName should not be first.lakekeeperWarehouseName
  }

  it should "reject an unsafe or duplicate name" in {
    a[BadRequestException] should be thrownBy
      resource.create(CreateWarehouseRequest("a/b"), sessionUser)

    resource.create(CreateWarehouseRequest("dup"), sessionUser)
    val conflict = intercept[WebApplicationException] {
      resource.create(CreateWarehouseRequest("dup"), sessionUser)
    }
    conflict.getResponse.getStatus shouldBe 409
  }

  "delete" should "empty the warehouse in Lakekeeper and remove the row" in {
    val created = resource.create(CreateWarehouseRequest("doomed"), sessionUser)

    resource.delete(created.whid, sessionUser)

    deletedIds.toList shouldBe List(stubWarehouseId)
    resource.status(sessionUser).warehouses shouldBe empty
  }

  // Pre-claims the catalog name create() will mint next, under the other user, so the
  // caller's store() trips the global UNIQUE(lakekeeper_warehouse_name) after the
  // (stubbed) Lakekeeper creation succeeded: one sequence number is taken for the
  // squatter itself (set explicitly, so storing it consumes nothing further), and the
  // squatter registers the name belonging to the next.
  private def squatOnNextMintedName(squatterDisplayName: String): Unit = {
    val squatter = getDSLContext.newRecord(USER_WAREHOUSE)
    squatter.setUid(otherUser.getUid)
    squatter.setName(squatterDisplayName)
    val takenWhid = WarehouseResource.drawNextWhid(getDSLContext)
    squatter.setWhid(takenWhid)
    squatter.setLakekeeperWarehouseName(lakekeeperWarehouseName(sessionUser.getUid, takenWhid + 1))
    squatter.setLakekeeperWarehouseId(UUID.randomUUID())
    squatter.setFlavor(
      org.apache.texera.dao.jooq.generated.enums.UserWarehouseFlavorEnum.local
    )
    squatter.store()
  }

  "a failed record write after Lakekeeper creation" should "compensate by deleting the warehouse" in {
    squatOnNextMintedName("unrelated")

    val error = intercept[WebApplicationException] {
      resource.create(CreateWarehouseRequest("boom"), sessionUser)
    }
    error.getResponse.getStatus shouldBe 500
    // The just-created (empty) Lakekeeper warehouse must not be orphaned.
    deletedIds.toList shouldBe List(stubWarehouseId)
    resource.status(sessionUser).warehouses shouldBe empty
  }

  "a Lakekeeper failure" should "surface as 502 on create and delete" in {
    createFailure = Some(new RuntimeException("Lakekeeper create failed (HTTP 500): boom"))
    val createError = intercept[WebApplicationException] {
      resource.create(CreateWarehouseRequest("unlucky"), sessionUser)
    }
    createError.getResponse.getStatus shouldBe 502
    resource.status(sessionUser).warehouses shouldBe empty

    createFailure = None
    val created = resource.create(CreateWarehouseRequest("undeletable"), sessionUser)
    deleteFailure = Some(new RuntimeException("Lakekeeper delete failed (HTTP 500): boom"))
    val deleteError = intercept[WebApplicationException] {
      resource.delete(created.whid, sessionUser)
    }
    deleteError.getResponse.getStatus shouldBe 502
    // The row must survive a failed Lakekeeper delete, so the user can retry.
    resource.status(sessionUser).warehouses.map(_.whid) shouldBe List(created.whid)
  }

  "a failed compensation" should "be logged and still surface the original failure" in {
    squatOnNextMintedName("unrelated-2")

    deleteFailure = Some(new RuntimeException("cleanup also failed"))
    val error = intercept[WebApplicationException] {
      resource.create(CreateWarehouseRequest("doublefault"), sessionUser)
    }
    error.getResponse.getStatus shouldBe 500
    resource.status(sessionUser).warehouses shouldBe empty
  }

  "the no-arg constructor" should "read the configured flag and client" in {
    // Jersey instantiates the resource reflectively via this constructor; storage.conf
    // keeps the feature off by default, so this exercises it without a Lakekeeper call.
    new WarehouseResource().status(sessionUser).enabled shouldBe StorageConfig.warehouseEnabled
  }

  it should "not let a user delete someone else's warehouse" in {
    val created = resource.create(CreateWarehouseRequest("mine"), sessionUser)

    a[NotFoundException] should be thrownBy resource.delete(created.whid, otherUser)
    resource.status(sessionUser).warehouses.map(_.whid) shouldBe List(created.whid)
  }
}
