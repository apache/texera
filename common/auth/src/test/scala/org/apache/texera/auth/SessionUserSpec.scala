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

class SessionUserSpec extends AnyFlatSpec with Matchers {

  private def buildUser(role: UserRoleEnum = UserRoleEnum.REGULAR): User = {
    val user = new User
    user.setUid(42)
    user.setName("alice")
    user.setEmail("alice@example.com")
    user.setGoogleId("g-123")
    user.setRole(role)
    user
  }

  "SessionUser" should "expose the underlying User's name via getName" in {
    val user = buildUser()
    val session = new SessionUser(user)
    session.getName shouldBe user.getName
  }

  it should "expose the underlying User's uid via getUid" in {
    val user = buildUser()
    val session = new SessionUser(user)
    session.getUid shouldBe user.getUid
  }

  it should "expose the underlying User's email via getEmail" in {
    val user = buildUser()
    val session = new SessionUser(user)
    session.getEmail shouldBe user.getEmail
  }

  it should "expose the underlying User's googleId via getGoogleId" in {
    val user = buildUser()
    val session = new SessionUser(user)
    session.getGoogleId shouldBe user.getGoogleId
  }

  it should "return the same User instance via getUser" in {
    val user = buildUser()
    val session = new SessionUser(user)
    session.getUser should be theSameInstanceAs user
  }

  "SessionUser.isRoleOf" should "return true when the user's role matches" in {
    val session = new SessionUser(buildUser(UserRoleEnum.ADMIN))
    session.isRoleOf(UserRoleEnum.ADMIN) shouldBe true
  }

  it should "return false when the user's role does not match" in {
    val session = new SessionUser(buildUser(UserRoleEnum.REGULAR))
    session.isRoleOf(UserRoleEnum.ADMIN) shouldBe false
  }
}
