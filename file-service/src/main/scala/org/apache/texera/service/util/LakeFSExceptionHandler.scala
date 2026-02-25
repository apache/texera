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

package org.apache.texera.service.util

import jakarta.ws.rs._
import jakarta.ws.rs.core.{MediaType, Response}
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._

object LakeFSExceptionHandler {
  private val logger = LoggerFactory.getLogger(getClass)

  private val fallbackMessages = Map(
    400 -> "LakeFS rejected the request. Please verify the parameters (repository/branch/path) and try again.",
    401 -> "Authentication with LakeFS failed.",
    403 -> "Permission denied by LakeFS.",
    404 -> "LakeFS resource not found. The repository/branch/object may not exist.",
    409 -> "LakeFS reported a conflict. Another operation may be in progress.",
    420 -> "Too many requests to LakeFS."
  ).withDefaultValue(
    "LakeFS request failed due to an unexpected server error."
  )

  /**
    * Wraps a LakeFS call with centralized error handling.
    */
  def withLakeFSErrorHandling[T](call: => T): T = {
    try {
      call
    } catch {
      case e: io.lakefs.clients.sdk.ApiException => handleException(e)
    }
  }

  /**
    * Converts LakeFS ApiException to appropriate HTTP exception
    */
  private def handleException(e: io.lakefs.clients.sdk.ApiException): Nothing = {
    val code = e.getCode
    val rawBody = Option(e.getResponseBody).filter(_.nonEmpty)
    val message = s"${fallbackMessages(code)}"

    logger.warn(s"LakeFS error $code, ${e.getMessage}, body: ${rawBody.getOrElse("N/A")}")

    def errorResponse(status: Int): Response =
      Response
        .status(status)
        .entity(Map("message" -> message).asJava)
        .`type`(MediaType.APPLICATION_JSON)
        .build()

    throw (code match {
      case 400                      => new BadRequestException(errorResponse(400))
      case 401                      => new NotAuthorizedException(errorResponse(401))
      case 403                      => new ForbiddenException(errorResponse(403))
      case 404                      => new NotFoundException(errorResponse(404))
      case c if c >= 400 && c < 500 => new WebApplicationException(errorResponse(c))
      case _                        => new InternalServerErrorException(errorResponse(500))
    })
  }
}
