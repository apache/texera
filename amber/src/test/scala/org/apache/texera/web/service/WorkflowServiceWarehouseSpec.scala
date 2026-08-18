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

package org.apache.texera.web.service

import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.USER_WAREHOUSE
import org.apache.texera.dao.jooq.generated.enums.UserWarehouseFlavorEnum
import org.apache.texera.dao.jooq.generated.tables.daos.UserDao
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

/**
  * Spec for [[WorkflowService.resolveWarehouseName]] (#6932): the ownership check and the
  * never-silently-fall-back rule for an execution's chosen warehouse.
  */
class WorkflowServiceWarehouseSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with MockTexeraDB {

  private var ownerUid: Integer = _
  private var intruderUid: Integer = _
  private var whid: Integer = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()

    val userDao = new UserDao(getDSLContext.configuration())
    val owner = new User
    owner.setName("resolve_spec_owner")
    owner.setEmail(s"user_${UUID.randomUUID()}@example.com")
    userDao.insert(owner)
    ownerUid = owner.getUid

    val intruder = new User
    intruder.setName("resolve_spec_intruder")
    intruder.setEmail(s"user_${UUID.randomUUID()}@example.com")
    userDao.insert(intruder)
    intruderUid = intruder.getUid

    val row = getDSLContext.newRecord(USER_WAREHOUSE)
    row.setUid(ownerUid)
    row.setName("mybucket")
    row.setWarehouseName(s"user-$ownerUid-mybucket")
    row.setLakekeeperWarehouseId(UUID.randomUUID())
    row.setFlavor(UserWarehouseFlavorEnum.local)
    row.store()
    whid = row.getWhid
  }

  override protected def afterAll(): Unit = closeConnectionPool()

  "resolveWarehouseName" should "require a pick while warehouses are enabled" in {
    // A run must name the warehouse it writes into, the same way it names a computing
    // unit; falling back to the shared default would route a caller that forgot to pick
    // into shared storage (#7751).
    val error = intercept[IllegalArgumentException] {
      WorkflowService.resolveWarehouseName(None, ownerUid, enabled = true)
    }
    error.getMessage should include("warehouse")
  }

  it should "keep the shared default warehouse when nothing is picked and the feature is off" in {
    WorkflowService.resolveWarehouseName(None, ownerUid, enabled = false) shouldBe None
  }

  it should "resolve an owned warehouse to its Lakekeeper name" in {
    WorkflowService.resolveWarehouseName(Some(whid), ownerUid, enabled = true) shouldBe
      Some(s"user-$ownerUid-mybucket")
  }

  it should "refuse another user's warehouse" in {
    val error = intercept[IllegalArgumentException] {
      WorkflowService.resolveWarehouseName(Some(whid), intruderUid, enabled = true)
    }
    error.getMessage should include(whid.toString)
  }

  it should "refuse an explicit pick while warehouses are disabled" in {
    // Never route the run into the shared warehouse silently (#6930).
    val error = intercept[IllegalArgumentException] {
      WorkflowService.resolveWarehouseName(Some(whid), ownerUid, enabled = false)
    }
    error.getMessage should include("disabled")
  }
}
