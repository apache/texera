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

import org.scalatest.flatspec.AnyFlatSpec

import java.net.URI
import org.scalatest.matchers.should.Matchers

class WarehouseReadGuardSpec extends AnyFlatSpec with Matchers {

  private def uri(path: String) = new URI(s"vfs://$path")

  "assertReadable" should "pass results that live in the shared default warehouse" in {
    noException should be thrownBy
      WarehouseReadGuard.assertReadable(uri("/wid/1/eid/2/result"), enabled = false)
    noException should be thrownBy
      WarehouseReadGuard.assertReadable(uri("/wid/1/eid/2/result"), enabled = true)
  }

  it should "pass warehouse results while the feature is enabled" in {
    noException should be thrownBy
      WarehouseReadGuard.assertReadable(
        uri("/wh/user-7-mybucket/wid/1/eid/2/result"),
        enabled = true
      )
  }

  it should "refuse a warehouse result explicitly while the feature is off" in {
    // Naming the situation matters: resolving the URI against the shared warehouse
    // would surface "table not found" — indistinguishable from data loss (#6930).
    val error = intercept[IllegalStateException] {
      WarehouseReadGuard.assertReadable(
        uri("/wh/user-7-mybucket/wid/1/eid/2/result"),
        enabled = false
      )
    }
    error.getMessage should include("user-7-mybucket")
    error.getMessage should include("disabled")
  }

  it should "refuse an unresolvable /wh/ prefix instead of falling back silently" in {
    // decodeURI reports None for an illegal warehouse name; opening such a URI would
    // silently resolve against the shared warehouse — in either flag state.
    an[IllegalStateException] should be thrownBy
      WarehouseReadGuard.assertReadable(uri("/wh/a%2Fb/wid/1/eid/2/result"), enabled = true)
    an[IllegalStateException] should be thrownBy
      WarehouseReadGuard.assertReadable(uri("/wh/a%2Fb/wid/1/eid/2/result"), enabled = false)
  }

  it should "refuse with a typed exception callers can let through their catch-alls" in {
    // SyncExecutionResource degrades other failures into empty results; the kill-switch
    // refusal is typed so it can be rethrown there instead of vanishing (#6930).
    a[WarehouseUnavailableException] should be thrownBy
      WarehouseReadGuard.assertReadable(
        uri("/wh/user-7-mybucket/wid/1/eid/2/result"),
        enabled = false
      )
    a[WarehouseUnavailableException] should be thrownBy
      WarehouseReadGuard.assertReadable(uri("/wh/a%2Fb/wid/1/eid/2/result"), enabled = true)
  }

  "the default enabled argument" should "read the configured flag" in {
    // Covers the default-argument methods; a default-warehouse URI passes and is
    // never skipped in either flag state, so this is deterministic regardless of
    // the configured value.
    noException should be thrownBy WarehouseReadGuard.assertReadable(uri("/wid/1/eid/2/result"))
    WarehouseReadGuard.skipWhileDisabled(uri("/wid/1/eid/2/result")) shouldBe false
  }

  "skipWhileDisabled" should "skip exactly the warehouse-scoped URIs while the feature is off" in {
    WarehouseReadGuard.skipWhileDisabled(
      uri("/wh/user-7-mybucket/wid/1/eid/2/result"),
      enabled = false
    ) shouldBe true
    WarehouseReadGuard.skipWhileDisabled(
      uri("/wh/a%2Fb/wid/1/eid/2/result"),
      enabled = false
    ) shouldBe true
    WarehouseReadGuard.skipWhileDisabled(
      uri("/wid/1/eid/2/result"),
      enabled = false
    ) shouldBe false
    WarehouseReadGuard.skipWhileDisabled(
      uri("/wh/user-7-mybucket/wid/1/eid/2/result"),
      enabled = true
    ) shouldBe false
  }
}
