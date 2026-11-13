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

package org.apache.texera.service.resource

import jakarta.ws.rs.core._

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.{Collections, Date, Locale}
import scala.util.Random

/**
  * Plumbing shared by the resource specs that drive the JAX-RS methods directly
  * rather than over HTTP: query-parameter encoding, collision-free resource names,
  * and the HttpHeaders the upload paths read.
  */
trait ResourceTestHelpers {

  /** Endpoints take file paths as a single URL-encoded query parameter. */
  protected def urlEnc(raw: String): String =
    URLEncoder.encode(raw, StandardCharsets.UTF_8.name())

  /** Names are unique per owner, so specs sharing a DB must not collide. */
  protected def uniqueName(prefix: String): String =
    s"$prefix-${System.nanoTime()}-${Random.alphanumeric.take(6).mkString.toLowerCase}"

  /** Minimal HttpHeaders exposing only Content-Length, which the upload paths read. */
  protected def mkHeaders(contentLength: Long): HttpHeaders =
    new HttpHeaders {
      private val headers = new MultivaluedHashMap[String, String]()
      headers.putSingle(HttpHeaders.CONTENT_LENGTH, contentLength.toString)
      override def getHeaderString(name: String): String = headers.getFirst(name)
      override def getRequestHeaders: MultivaluedMap[String, String] = headers
      override def getRequestHeader(name: String): java.util.List[String] =
        Option(headers.get(name)).getOrElse(Collections.emptyList[String]())
      override def getAcceptableMediaTypes: java.util.List[MediaType] = Collections.emptyList()
      override def getAcceptableLanguages: java.util.List[Locale] = Collections.emptyList()
      override def getMediaType: MediaType = null
      override def getLanguage: Locale = null
      override def getCookies: java.util.Map[String, Cookie] = Collections.emptyMap()
      override def getDate: Date = null
      override def getLength: Int = contentLength.toInt
    }
}
