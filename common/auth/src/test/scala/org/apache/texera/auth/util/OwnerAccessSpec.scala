// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.texera.auth.util

import jakarta.ws.rs.ForbiddenException
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OwnerAccessSpec extends AnyFlatSpec with Matchers {

  "privilege" should "give the owner WRITE whatever they were granted" in {
    OwnerAccess.privilege(isOwner = true, PrivilegeEnum.READ) shouldBe PrivilegeEnum.WRITE
    OwnerAccess.privilege(isOwner = true, null) shouldBe PrivilegeEnum.WRITE
  }

  it should "give anyone else their grant, or NONE without one" in {
    OwnerAccess.privilege(isOwner = false, PrivilegeEnum.READ) shouldBe PrivilegeEnum.READ
    OwnerAccess.privilege(isOwner = false, null) shouldBe PrivilegeEnum.NONE
  }

  it should "not look up the grant for the owner" in {
    var lookups = 0
    def lookup(): PrivilegeEnum = { lookups += 1; PrivilegeEnum.READ }

    OwnerAccess.privilege(isOwner = true, lookup())
    lookups shouldBe 0
    OwnerAccess.privilege(isOwner = false, lookup())
    lookups shouldBe 1
  }

  "requireNotOwner" should "refuse a change to the owner's access with a 403" in {
    val ex = intercept[ForbiddenException](OwnerAccess.requireNotOwner(isOwner = true))
    ex.getResponse.getStatus shouldBe 403
    ex.getMessage shouldBe OwnerAccess.ownerChangeRefused
  }

  it should "allow a change to anyone else's access" in {
    noException should be thrownBy OwnerAccess.requireNotOwner(isOwner = false)
  }
}
