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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Unit tests for the pure request-parsing helpers of [[S3ProxyServlet]] — the credential
  * token extraction (which must handle both the SigV4 and SigV2 `Authorization` formats
  * GeeseFS emits, carrying the user JWT in the access-key position) and the path-style
  * bucket/repository extraction that scopes each request.
  */
class S3ProxyServletSpec extends AnyFlatSpec with Matchers {

  // A representative JWT: base64url segments (A-Za-z0-9-_) joined by '.', so it contains
  // none of the '/', ':' or whitespace that the two Authorization formats delimit on.
  private val jwt =
    "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXhlcmEiLCJ1c2VySWQiOjF9.abc-_DEF123"

  "extractCredentialToken" should "read the access key from a SigV4 Authorization header" in {
    val header =
      s"AWS4-HMAC-SHA256 Credential=$jwt/20260721/us-east-1/s3/aws4_request, " +
        "SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=deadbeef"
    S3ProxyServlet.extractCredentialToken(header) shouldBe Some(jwt)
  }

  it should "read the access key from a SigV2 Authorization header" in {
    S3ProxyServlet.extractCredentialToken(s"AWS $jwt:c2lnbmF0dXJl") shouldBe Some(jwt)
  }

  it should "handle a short (non-JWT) access key in both formats" in {
    S3ProxyServlet.extractCredentialToken(
      "AWS4-HMAC-SHA256 Credential=AKIAEXAMPLE/20260721/us-east-1/s3/aws4_request, " +
        "SignedHeaders=host, Signature=abc"
    ) shouldBe Some("AKIAEXAMPLE")
    S3ProxyServlet.extractCredentialToken("AWS AKIAEXAMPLE:sig") shouldBe Some("AKIAEXAMPLE")
  }

  it should "return None for a null header" in {
    S3ProxyServlet.extractCredentialToken(null) shouldBe None
  }

  it should "return None for a header in neither AWS format" in {
    S3ProxyServlet.extractCredentialToken("Bearer some.jwt.token") shouldBe None
    S3ProxyServlet.extractCredentialToken("") shouldBe None
    S3ProxyServlet.extractCredentialToken("AWS4-HMAC-SHA256 SignedHeaders=host") shouldBe None
  }

  "bucketFromUri" should "return the first path segment of a path-style object request" in {
    S3ProxyServlet.bucketFromUri(
      "/dataset-1/097b4111e0ac9f46/model-00001-of-00003.pt"
    ) shouldBe "dataset-1"
  }

  it should "return the bucket for a bucket-only request (with or without trailing slash)" in {
    S3ProxyServlet.bucketFromUri("/dataset-1") shouldBe "dataset-1"
    S3ProxyServlet.bucketFromUri("/dataset-1/") shouldBe "dataset-1"
  }

  it should "URL-decode the bucket segment" in {
    S3ProxyServlet.bucketFromUri("/my%20dataset/commit/f.txt") shouldBe "my dataset"
  }

  it should "return empty for the root URI (no bucket to authorize)" in {
    S3ProxyServlet.bucketFromUri("/") shouldBe ""
    S3ProxyServlet.bucketFromUri("") shouldBe ""
  }
}
