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

package org.apache.texera.amber.core.storage

import org.apache.texera.amber.config.EnvironmentalVariable

import java.net.{HttpURLConnection, URI, URL, URLEncoder}
import java.nio.charset.StandardCharsets

/**
  * Resolves a dataset file path (`/ownerEmail/datasetName/versionName/relativePath`) to its
  * `dataset:///` URI by calling the file-service over HTTP, instead of querying Postgres directly.
  *
  * This lets a computing-unit pod — which runs user-defined functions and therefore must not hold
  * database credentials (issue #5011) — resolve datasets without a JDBC connection.
  *
  * Following the same convention as [[org.apache.texera.amber.core.storage.model.DatasetFileDocument]],
  * the remote path is taken only when a forwarded `USER_JWT_TOKEN` is present (i.e. inside a CU pod).
  * Backend services (which own the database) have no such token, so resolution stays local there and
  * the file-service endpoint never recurses back onto itself.
  */
object RemoteDatasetResolver {

  private lazy val userJwtToken: String =
    sys.env.getOrElse(EnvironmentalVariable.ENV_USER_JWT_TOKEN, "").trim

  /**
    * The file-service endpoint that resolves a dataset path. If not set explicitly, it is derived
    * from the presigned-URL endpoint the CU pod already receives, so no extra configuration is
    * required for the resolution to work wherever file reads already work.
    */
  private lazy val resolveEndpoint: String =
    sys.env
      .get(EnvironmentalVariable.ENV_FILE_SERVICE_RESOLVE_PATH_ENDPOINT)
      .map(_.trim)
      .filter(_.nonEmpty)
      .orElse(
        sys.env
          .get(EnvironmentalVariable.ENV_FILE_SERVICE_GET_PRESIGNED_URL_ENDPOINT)
          .map(_.trim.replace("/presign-download", "/resolve"))
      )
      .getOrElse("http://localhost:9092/api/dataset/resolve")

  /** Remote resolution is active only when this process carries a forwarded user token. */
  def enabled: Boolean = userJwtToken.nonEmpty

  /**
    * @throws java.io.IOException / RuntimeException if the file-service cannot resolve the path.
    */
  def resolve(datasetPath: String): URI = resolve(resolveEndpoint, userJwtToken, datasetPath)

  /** Endpoint/token are injectable so the HTTP round-trip can be unit-tested in isolation. */
  private[storage] def resolve(endpoint: String, token: String, datasetPath: String): URI = {
    val requestUrl =
      s"$endpoint?path=${URLEncoder.encode(datasetPath, StandardCharsets.UTF_8.name())}"
    val connection = new URL(requestUrl).openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("GET")
    connection.setRequestProperty("Authorization", s"Bearer $token")
    try {
      val code = connection.getResponseCode
      if (code != HttpURLConnection.HTTP_OK) {
        throw new RuntimeException(
          s"file-service failed to resolve dataset path '$datasetPath' (HTTP $code)"
        )
      }
      val body = new String(connection.getInputStream.readAllBytes(), StandardCharsets.UTF_8).trim
      new URI(body)
    } finally {
      connection.disconnect()
    }
  }
}
