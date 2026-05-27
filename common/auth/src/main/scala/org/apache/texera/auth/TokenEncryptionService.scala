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

import org.apache.texera.config.AuthConfig
import org.jose4j.jwe.{ContentEncryptionAlgorithmIdentifiers, JsonWebEncryption, KeyManagementAlgorithmIdentifiers}
import org.jose4j.keys.AesKey

import java.nio.charset.StandardCharsets

object TokenEncryptionService {
  private val key = new AesKey(AuthConfig.encryptionSecretKey.getBytes(StandardCharsets.UTF_8))

  def encrypt(plaintext: String): String = {
    val jwe = new JsonWebEncryption()
    jwe.setAlgorithmHeaderValue(KeyManagementAlgorithmIdentifiers.DIRECT)
    jwe.setEncryptionMethodHeaderParameter(ContentEncryptionAlgorithmIdentifiers.AES_256_GCM)
    jwe.setKey(key)
    jwe.setPayload(plaintext)
    jwe.getCompactSerialization
  }

  def decrypt(ciphertext: String): String = {
    val jwe = new JsonWebEncryption()
    jwe.setKey(key)
    jwe.setCompactSerialization(ciphertext)
    jwe.getPayload
  }
}
