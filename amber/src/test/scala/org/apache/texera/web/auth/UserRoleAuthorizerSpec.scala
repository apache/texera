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

package org.apache.texera.web.auth

import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UserRoleAuthorizerSpec extends AnyFlatSpec with Matchers {

  // The dropwizard Authorizer contract hands the authenticator a
  // SessionUser; reproduce that with the jooq-generated POJO so the test
  // exercises the same isRoleOf path the production filter does.
  private def sessionFor(role: UserRoleEnum): SessionUser = {
    val u = new User()
    u.setRole(role)
    new SessionUser(u)
  }

  "authorize" should "return true when the role string matches the user's role" in {
    UserRoleAuthorizer.authorize(sessionFor(UserRoleEnum.ADMIN), "ADMIN") shouldBe true
    UserRoleAuthorizer.authorize(sessionFor(UserRoleEnum.REGULAR), "REGULAR") shouldBe true
  }

  it should "return false when the user's role differs from the requested one" in {
    UserRoleAuthorizer.authorize(sessionFor(UserRoleEnum.REGULAR), "ADMIN") shouldBe false
    UserRoleAuthorizer.authorize(sessionFor(UserRoleEnum.INACTIVE), "REGULAR") shouldBe false
  }

  it should "throw IllegalArgumentException when the requested role is not a UserRoleEnum value" in {
    // Bubbled up from UserRoleEnum.valueOf. Documenting the behavior so a
    // future @RolesAllowed typo can't silently downgrade to "always deny".
    an[IllegalArgumentException] should be thrownBy
      UserRoleAuthorizer.authorize(sessionFor(UserRoleEnum.ADMIN), "SUPER_ADMIN")
  }

  it should "treat the role string as case-sensitive (enum names are uppercase)" in {
    an[IllegalArgumentException] should be thrownBy
      UserRoleAuthorizer.authorize(sessionFor(UserRoleEnum.ADMIN), "admin")
  }
}
