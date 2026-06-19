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

package org.apache.texera.auth

import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.security.Principal

class SessionUserSpec extends AnyFlatSpec with Matchers {

  private def buildUser(role: UserRoleEnum): User = {
    val user = new User
    user.setUid(42)
    user.setName("alice")
    user.setEmail("alice@example.com")
    user.setGoogleId("google-alice")
    user.setRole(role)
    user
  }

  "SessionUser" should "wrap a jOOQ User and expose it as a Principal" in {
    val user = buildUser(UserRoleEnum.ADMIN)
    val sessionUser = new SessionUser(user)
    val principal: Principal = sessionUser

    principal.getName shouldBe user.getName
    sessionUser.getUid shouldBe user.getUid
    sessionUser.getEmail shouldBe user.getEmail
    sessionUser.getGoogleId shouldBe user.getGoogleId
    sessionUser.getUser shouldBe theSameInstanceAs(user)
  }

  it should "check roles by exact UserRoleEnum equality" in {
    val adminSessionUser = new SessionUser(buildUser(UserRoleEnum.ADMIN))
    val regularSessionUser = new SessionUser(buildUser(UserRoleEnum.REGULAR))

    adminSessionUser.isRoleOf(UserRoleEnum.ADMIN) shouldBe true
    adminSessionUser.isRoleOf(UserRoleEnum.REGULAR) shouldBe false
    regularSessionUser.isRoleOf(UserRoleEnum.ADMIN) shouldBe false
    regularSessionUser.isRoleOf(UserRoleEnum.REGULAR) shouldBe true
  }
}
