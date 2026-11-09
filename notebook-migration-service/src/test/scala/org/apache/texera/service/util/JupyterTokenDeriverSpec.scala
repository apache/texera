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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JupyterTokenDeriverSpec extends AnyFlatSpec with Matchers {

  private val secret = "test-secret"

  "JupyterTokenDeriver.derive" should "return the same token for a uid across calls" in {
    // The token is never stored, so every call has to reproduce it or a running pod
    // becomes unreachable.
    JupyterTokenDeriver.derive(7, secret) shouldBe JupyterTokenDeriver.derive(7, secret)
  }

  it should "return a different token for each uid" in {
    // The isolation property: one user's token must not open another user's Jupyter.
    val tokens = (1 to 50).map(JupyterTokenDeriver.derive(_, secret))
    tokens.distinct.size shouldBe 50
  }

  it should "return a different token when the secret changes" in {
    // Rotation: changing the secret has to invalidate previously issued tokens.
    JupyterTokenDeriver.derive(7, secret) should not be JupyterTokenDeriver.derive(7, "other")
  }

  it should "return a fixed-length lowercase hex token" in {
    JupyterTokenDeriver.derive(7, secret) should fullyMatch regex "[0-9a-f]{32}"
  }

  it should "encode every digest byte as exactly two hex characters" in {
    // Pinned against an independently computed HMAC-SHA256. This digest starts 0xa5, which is
    // negative as a Byte, so a formatter that sign-extended would emit "ffffffa5" and shift
    // the whole token. A length or charset check alone would not notice.
    JupyterTokenDeriver.derive(7, "golden-secret") shouldBe "a5d36f59073ef59843384d5411d765e5"
  }

  it should "reject an empty secret" in {
    an[IllegalArgumentException] should be thrownBy JupyterTokenDeriver.derive(7, "")
  }

  it should "fall back to the configured secret when none is passed" in {
    // Exercises the default argument. The configured secret is empty unless a deployment
    // sets one, so there is nothing to derive from and the require fires.
    if (sys.env.get("JUPYTER_TOKEN_SECRET").isEmpty) {
      an[IllegalArgumentException] should be thrownBy JupyterTokenDeriver.derive(7)
    }
  }

  "JupyterTokenDeriver.validateConfiguration" should "reject an empty secret when per-user Jupyter is on" in {
    val thrown = the[IllegalStateException] thrownBy JupyterTokenDeriver.validateConfiguration(
      jupyterEnabled = true,
      secret = ""
    )
    thrown.getMessage should include("storage.jupyter.token-secret")
  }

  it should "allow an empty secret when per-user Jupyter is off" in {
    // Single-node and local dev run one shared Jupyter from static config and set no secret.
    noException should be thrownBy JupyterTokenDeriver.validateConfiguration(
      jupyterEnabled = false,
      secret = ""
    )
  }

  it should "allow a configured secret when per-user Jupyter is on" in {
    noException should be thrownBy JupyterTokenDeriver.validateConfiguration(
      jupyterEnabled = true,
      secret = secret
    )
  }
}
