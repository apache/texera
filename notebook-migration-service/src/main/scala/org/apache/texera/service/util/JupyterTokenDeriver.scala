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

import org.apache.texera.common.config.{KubernetesConfig, StorageConfig}

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.nio.charset.StandardCharsets.UTF_8

/**
  * Derives each user's JupyterLab token from a server-held secret instead of storing one.
  * The value is stable for a uid until the secret changes, so any replica of this service
  * derives the same token and no credential is kept at rest.
  */
object JupyterTokenDeriver {

  private val algorithm = "HmacSHA256"

  // 128 bits of the digest, which is ample for a token and keeps the URL short.
  private val tokenLength = 32

  def derive(uid: Int, secret: String = StorageConfig.jupyterTokenSecret): String = {
    require(secret.nonEmpty, "cannot derive a Jupyter token from an empty secret")
    val mac = Mac.getInstance(algorithm)
    mac.init(new SecretKeySpec(secret.getBytes(UTF_8), algorithm))
    mac.doFinal(uid.toString.getBytes(UTF_8)).map("%02x".format(_)).mkString.take(tokenLength)
  }

  /**
    * Refuses to start per-user Jupyter without a secret: an empty key is public, so anyone
    * could derive another user's token. Only enforced when the feature is on, so the
    * single-node and local-dev deployments are unaffected.
    */
  def validateConfiguration(
      jupyterEnabled: Boolean = KubernetesConfig.jupyterEnabled,
      secret: String = StorageConfig.jupyterTokenSecret
  ): Unit =
    if (jupyterEnabled && secret.isEmpty) {
      throw new IllegalStateException(
        "kubernetes.jupyter-enabled requires a non-empty storage.jupyter.token-secret"
      )
    }
}
