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

package org.apache.texera.common.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Spec for [[AuthConfig]]. Reading each value forces resolution from auth.conf, so a renamed or
  * mistyped key surfaces here as a ConfigException. Exact-value assertions are guarded on the env
  * override being unset; the random-secret path is exercised directly via getRandomHexString.
  */
class AuthConfigSpec extends AnyFlatSpec with Matchers {

  // `${?VAR}` in HOCON can be satisfied by an OS env var or a JVM system property.
  private def isOverridden(name: String): Boolean =
    sys.env.contains(name) || sys.props.contains(name)

  "AuthConfig.jwtExpirationMinutes" should "resolve from auth.conf" in {
    if (!isOverridden("AUTH_JWT_EXPIRATION_IN_MINUTES")) {
      AuthConfig.jwtExpirationMinutes shouldBe 10080
    } else {
      AuthConfig.jwtExpirationMinutes should be > 0
    }
  }

  "AuthConfig.jwtSecretKey" should "return the configured secret (lowercased) and memoize it" in {
    val first = AuthConfig.jwtSecretKey
    if (!isOverridden("AUTH_JWT_SECRET")) {
      first shouldBe "8a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d"
    } else {
      first should not be empty
    }
    first shouldBe first.toLowerCase
    AuthConfig.jwtSecretKey shouldBe first // second call hits the memoized path
  }

  "AuthConfig.getRandomHexString" should "generate a fresh 32-char lowercase-hex string" in {
    val method = AuthConfig.getClass.getDeclaredMethod("getRandomHexString")
    method.setAccessible(true)
    val hex1 = method.invoke(AuthConfig).asInstanceOf[String]
    hex1 should have length 32
    hex1 should fullyMatch regex "[0-9a-f]{32}"
    val hex2 = method.invoke(AuthConfig).asInstanceOf[String]
    hex2 should not be hex1
  }
}
