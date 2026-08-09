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
import org.apache.texera.web.resource.dashboard.user.warehouse.WarehouseResource.CreateWarehouseRequest
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
  * `StorageConfig.warehouseEnabled` is a test-overridable var (like `s3Endpoint`); the
  * only other readers take the flag as an explicit parameter, so flipping it here cannot
  * interfere with any other suite.
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

  private val stubClient: LakekeeperClient = new LakekeeperClient() {
    override def createWarehouse(warehouseName: String): UUID = {
      createdNames += warehouseName
      stubWarehouseId
    }
    override def deleteWarehouseEmptyFirst(warehouseId: UUID): Unit =
      deletedIds += warehouseId
  }

  private val resource = new WarehouseResource(stubClient)
  private var sessionUser: SessionUser = _
  private var otherUser: SessionUser = _
  private var originalEnabled: Boolean = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    originalEnabled = StorageConfig.warehouseEnabled

    val userDao = new UserDao(getDSLContext.configuration())
    val user = new User
    user.setName("warehouse_spec_user")
    user.setEmail(s"user_${UUID.randomUUID()}@example.com")
    user.setPassword("password")
    userDao.insert(user)
    sessionUser = new SessionUser(user)

    val other = new User
    other.setName("warehouse_spec_other")
    other.setEmail(s"user_${UUID.randomUUID()}@example.com")
    other.setPassword("password")
    userDao.insert(other)
    otherUser = new SessionUser(other)
  }

  override protected def afterAll(): Unit = {
    StorageConfig.warehouseEnabled = originalEnabled
    closeConnectionPool()
  }

  override protected def beforeEach(): Unit = {
    StorageConfig.warehouseEnabled = true
    createdNames.clear()
    deletedIds.clear()
    getDSLContext.deleteFrom(USER_WAREHOUSE).execute()
  }

  // ---------------------------------------------------------------------------
  // Disabled gate
  // ---------------------------------------------------------------------------

  "status" should "report disabled with no warehouses while the flag is off" in {
    StorageConfig.warehouseEnabled = false
    val status = resource.status(sessionUser)
    status.enabled shouldBe false
    status.warehouses shouldBe empty
  }

  "create and delete" should "be refused while the flag is off" in {
    StorageConfig.warehouseEnabled = false
    a[ForbiddenException] should be thrownBy
      resource.create(CreateWarehouseRequest("mybucket"), sessionUser)
    a[ForbiddenException] should be thrownBy resource.delete(1, sessionUser)
  }

  // ---------------------------------------------------------------------------
  // Create / list / delete
  // ---------------------------------------------------------------------------

  "create" should "create in Lakekeeper, record the row, and mint user-<uid>-<name>" in {
    val created = resource.create(CreateWarehouseRequest("mybucket"), sessionUser)

    created.name shouldBe "mybucket"
    created.warehouseName shouldBe s"user-${sessionUser.getUid}-mybucket"
    created.flavor shouldBe "local"
    createdNames.toList shouldBe List(s"user-${sessionUser.getUid}-mybucket")

    val status = resource.status(sessionUser)
    status.enabled shouldBe true
    status.warehouses.map(_.whid) shouldBe List(created.whid)
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

  it should "not let a user delete someone else's warehouse" in {
    val created = resource.create(CreateWarehouseRequest("mine"), sessionUser)

    a[NotFoundException] should be thrownBy resource.delete(created.whid, otherUser)
    resource.status(sessionUser).warehouses.map(_.whid) shouldBe List(created.whid)
  }
}
