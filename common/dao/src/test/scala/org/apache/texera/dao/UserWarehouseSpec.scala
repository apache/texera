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

package org.apache.texera.dao

import org.apache.texera.dao.jooq.generated.Tables.{USER, USER_WAREHOUSE}
import org.apache.texera.dao.jooq.generated.enums.UserWarehouseFlavorEnum
import org.jooq.exception.DataAccessException

import java.util.UUID
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Spec for the `user_warehouse` table (#6931). Schema only — nothing reads or writes the
  * table in production yet — so this pins the DDL's structural properties (columns, the
  * per-user name uniqueness, and the ownership cascade) against the generated jOOQ classes.
  */
class UserWarehouseSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with MockTexeraDB {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    initializeDBAndReplaceDSLContext()
  }

  override protected def afterAll(): Unit =
    try closeConnectionPool()
    finally super.afterAll()

  // Credentials live in auth_provider now, and the ck_nulltest constraint that used to require
  // one on the user row went with them, so a warehouse owner needs nothing but a name.
  private def insertUser(name: String): Integer =
    getDSLContext
      .insertInto(USER, USER.NAME)
      .values(name)
      .returning(USER.UID)
      .fetchOne()
      .getUid

  // Any globally-unique catalog name will do: this spec covers storage and constraints,
  // not how WarehouseResource mints the name.
  private def insertWarehouse(
      uid: Integer,
      name: String,
      lakekeeperWarehouseName: String
  ): Integer =
    getDSLContext
      .insertInto(
        USER_WAREHOUSE,
        USER_WAREHOUSE.UID,
        USER_WAREHOUSE.NAME,
        USER_WAREHOUSE.LAKEKEEPER_WAREHOUSE_NAME,
        USER_WAREHOUSE.LAKEKEEPER_WAREHOUSE_ID,
        USER_WAREHOUSE.FLAVOR
      )
      .values(
        uid,
        name,
        lakekeeperWarehouseName,
        UUID.randomUUID(),
        UserWarehouseFlavorEnum.local
      )
      .returning(USER_WAREHOUSE.WHID)
      .fetchOne()
      .getWhid

  "user_warehouse" should "store a registered warehouse and return it by owner" in {
    val uid = insertUser("warehouse-owner")
    insertWarehouse(uid, "mybucket", s"user-$uid-mybucket")

    val row = getDSLContext
      .selectFrom(USER_WAREHOUSE)
      .where(USER_WAREHOUSE.UID.eq(uid))
      .fetchOne()
    row.getName shouldBe "mybucket"
    row.getLakekeeperWarehouseName shouldBe s"user-$uid-mybucket"
    row.getFlavor shouldBe UserWarehouseFlavorEnum.local
    row.getLakekeeperWarehouseId should not be null
    row.getCreatedAt should not be null
  }

  it should "reject a duplicate display name for the same user" in {
    val uid = insertUser("duplicate-name-owner")
    insertWarehouse(uid, "dup", s"user-$uid-dup")

    a[DataAccessException] should be thrownBy
      insertWarehouse(uid, "dup", s"user-$uid-dup-2")
  }

  it should "reject a duplicate lakekeeper_warehouse_name across users" in {
    val first = insertUser("catalog-name-owner")
    val second = insertUser("catalog-name-intruder")
    insertWarehouse(first, "shared", "user-shared-catalog-name")

    a[DataAccessException] should be thrownBy
      insertWarehouse(second, "unrelated", "user-shared-catalog-name")
  }

  it should "cascade-delete a user's warehouses with the user" in {
    val uid = insertUser("cascade-owner")
    insertWarehouse(uid, "doomed", s"user-$uid-doomed")

    getDSLContext.deleteFrom(USER).where(USER.UID.eq(uid)).execute()

    getDSLContext
      .selectFrom(USER_WAREHOUSE)
      .where(USER_WAREHOUSE.UID.eq(uid))
      .fetch()
      .size shouldBe 0
  }
}
