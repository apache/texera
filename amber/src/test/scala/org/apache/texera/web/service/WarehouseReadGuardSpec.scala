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
import org.scalatest.matchers.should.Matchers

class WarehouseReadGuardSpec extends AnyFlatSpec with Matchers {

  "assertReadable" should "pass results that live in the shared default warehouse" in {
    noException should be thrownBy WarehouseReadGuard.assertReadable(None, enabled = false)
    noException should be thrownBy WarehouseReadGuard.assertReadable(None, enabled = true)
  }

  it should "pass warehouse results while the feature is enabled" in {
    noException should be thrownBy
      WarehouseReadGuard.assertReadable(Some("user-7-mybucket"), enabled = true)
  }

  it should "refuse a warehouse result explicitly while the feature is off" in {
    // Naming the situation matters: resolving the URI against the shared warehouse
    // would surface "table not found" — indistinguishable from data loss (#6930).
    val error = intercept[IllegalStateException] {
      WarehouseReadGuard.assertReadable(Some("user-7-mybucket"), enabled = false)
    }
    error.getMessage should include("user-7-mybucket")
    error.getMessage should include("disabled")
  }
}
