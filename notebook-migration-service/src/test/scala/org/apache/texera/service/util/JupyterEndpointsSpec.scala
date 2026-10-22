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

package org.apache.texera.service.util

import org.apache.texera.common.config.StorageConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JupyterEndpointsSpec extends AnyFlatSpec with Matchers {

  private val endpoints = JupyterEndpoints("http://internal:8888", "http://public", "tok")

  "JupyterEndpoints.configured" should "mirror the static Jupyter configuration" in {
    JupyterEndpoints.configured shouldBe JupyterEndpoints(
      StorageConfig.jupyterInternalURL,
      StorageConfig.jupyterPublicURL,
      StorageConfig.jupyterToken
    )
  }

  "JupyterEndpoints" should "compare by value" in {
    endpoints shouldBe JupyterEndpoints("http://internal:8888", "http://public", "tok")
    endpoints.hashCode shouldBe
      JupyterEndpoints("http://internal:8888", "http://public", "tok").hashCode
  }

  it should "differ when any field differs" in {
    // The token is part of identity: two users share a URL in the fallback case but never a token.
    endpoints should not be endpoints.copy(token = "other")
    endpoints should not be endpoints.copy(internalUrl = "http://other:8888")
    endpoints should not be endpoints.copy(publicUrl = "http://other")
  }

  it should "not equal a value of another type" in {
    endpoints should not be "http://internal:8888"
    endpoints.toString should include("http://internal:8888")
  }

  it should "destructure into its three parts" in {
    val JupyterEndpoints(internal, public, token) = endpoints
    internal shouldBe "http://internal:8888"
    public shouldBe "http://public"
    token shouldBe "tok"
  }
}
