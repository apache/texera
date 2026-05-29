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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TokenEncryptionServiceSpec extends AnyFlatSpec with Matchers {

  "TokenEncryptionService" should "round-trip a plaintext string" in {
    val plaintext =
      """{"refreshToken":"1//0gtoken","scopes":"https://www.googleapis.com/auth/drive"}"""
    TokenEncryptionService.decrypt(TokenEncryptionService.encrypt(plaintext)) shouldBe plaintext
  }

  it should "throw when decrypting a non-JWE string" in {
    an[Exception] should be thrownBy TokenEncryptionService.decrypt("not-a-jwe-token")
  }
}
