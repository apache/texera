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

import io.lakefs.clients.sdk.ApiException
import jakarta.ws.rs._
import org.apache.texera.service.util.LakeFSExceptionHandler.withLakeFSErrorHandling
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LakeFSExceptionHandlerSpec extends AnyFlatSpec with Matchers {

  private def lakeFSError(code: Int): ApiException =
    new ApiException(code, s"lakefs returned $code")

  // typed as Unit (not Nothing) so the no-context overload resolves unambiguously
  private def failingCall(code: Int): Unit = throw lakeFSError(code)

  private def entityMessage(e: WebApplicationException): String =
    e.getResponse.getEntity
      .asInstanceOf[java.util.Map[String, String]]
      .get("message")

  "withLakeFSErrorHandling" should "return the call's result when no exception is thrown" in {
    withLakeFSErrorHandling("reading a file")(42) shouldEqual 42
  }

  it should "map each LakeFS client error code to the matching JAX-RS exception" in {
    val expected = Map(
      400 -> classOf[BadRequestException],
      401 -> classOf[NotAuthorizedException],
      403 -> classOf[ForbiddenException],
      404 -> classOf[NotFoundException]
    )
    expected.foreach {
      case (code, exceptionClass) =>
        val thrown = intercept[WebApplicationException] {
          withLakeFSErrorHandling(failingCall(code))
        }
        thrown.getClass shouldEqual exceptionClass
        thrown.getResponse.getStatus shouldEqual code
    }
  }

  it should "keep the original status for other 4xx codes (e.g. 409 conflict)" in {
    val thrown = intercept[WebApplicationException] {
      withLakeFSErrorHandling(failingCall(409))
    }
    thrown.getResponse.getStatus shouldEqual 409
  }

  it should "map server-side and unknown codes to a 500 response" in {
    Seq(500, 502, 503, 0).foreach { code =>
      val thrown = intercept[InternalServerErrorException] {
        withLakeFSErrorHandling(failingCall(code))
      }
      thrown.getResponse.getStatus shouldEqual 500
    }
  }

  it should "include the operation context in the message visible to the frontend" in {
    val thrown = intercept[NotFoundException] {
      withLakeFSErrorHandling("deleting file 'a.csv' from dataset 'd1'") {
        throw lakeFSError(404)
      }
    }
    val message = entityMessage(thrown)
    message should include("deleting file 'a.csv' from dataset 'd1'")
    message should include("not found")
  }

  it should "produce a frontend-readable message without operation context too" in {
    val thrown = intercept[ForbiddenException] {
      withLakeFSErrorHandling(failingCall(403))
    }
    entityMessage(thrown) should include("Permission denied")
  }

  it should "let non-LakeFS exceptions propagate unchanged" in {
    val original = new IllegalStateException("not a lakefs error")
    val thrown = intercept[IllegalStateException] {
      withLakeFSErrorHandling("any operation")(throw original)
    }
    thrown should be theSameInstanceAs original
  }

  it should "still map the status code when LakeFS provides a response body" in {
    val withBody = new ApiException(
      404,
      java.util.Collections.emptyMap[String, java.util.List[String]](),
      """{"message":"object not found"}"""
    )
    val thrown = intercept[NotFoundException] {
      withLakeFSErrorHandling("reading file 'a.csv'")(throw withBody)
    }
    entityMessage(thrown) should include("LakeFS resource not found")
  }
}
