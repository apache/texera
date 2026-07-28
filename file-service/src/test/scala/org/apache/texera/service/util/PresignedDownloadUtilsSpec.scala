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

import jakarta.ws.rs.core.Response
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// The presign endpoints accept repositoryName + commitHash together (direct
// addressing) or neither (resolve from a logical path). Exactly one is a client
// error.
class PresignedDownloadUtilsSpec extends AnyFlatSpec with Matchers {

  "requireBothOrNeither" should "reject a repositoryName without a commitHash" in {
    val response = PresignedDownloadUtils.requireBothOrNeither("dataset-1", null)
    response.map(_.getStatus) shouldBe Some(Response.Status.BAD_REQUEST.getStatusCode)
  }

  it should "reject a commitHash without a repositoryName" in {
    val response = PresignedDownloadUtils.requireBothOrNeither(null, "abc123")
    response.map(_.getStatus) shouldBe Some(Response.Status.BAD_REQUEST.getStatusCode)
  }

  it should "explain what the caller got wrong" in {
    PresignedDownloadUtils
      .requireBothOrNeither("dataset-1", null)
      .map(_.getEntity.toString)
      .getOrElse("") should include("must be provided together")
  }

  it should "accept both being provided" in {
    PresignedDownloadUtils.requireBothOrNeither("dataset-1", "abc123") shouldBe None
  }

  it should "accept neither being provided" in {
    PresignedDownloadUtils.requireBothOrNeither(null, null) shouldBe None
  }
}
