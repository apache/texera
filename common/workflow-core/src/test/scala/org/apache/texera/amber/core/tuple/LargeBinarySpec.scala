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

package org.apache.texera.amber.core.tuple

import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.service.util.LargeBinaryManager
import org.scalatest.flatspec.AnyFlatSpec

/**
  * Unit tests for [[LargeBinary]], the S3-URI reference stored in tuples.
  *
  * No S3 endpoint is involved: the URI-string constructor is pure, and the no-arg
  * constructor only needs a base URI on the calling thread (see [[LargeBinaryManager]]),
  * which is set on a dedicated thread so the thread-local does not leak between tests.
  */
class LargeBinarySpec extends AnyFlatSpec {

  /** Runs `body` on a fresh thread, optionally seeding that thread's base URI. */
  private def onFreshThread[T](baseUri: Option[String])(body: => T): Either[Throwable, T] = {
    @volatile var result: Either[Throwable, T] = Left(new IllegalStateException("did not run"))
    val thread = new Thread(() => {
      result =
        try {
          baseUri.foreach(LargeBinaryManager.setCurrentBaseUri)
          Right(body)
        } catch { case e: Throwable => Left(e) }
    })
    thread.start()
    thread.join()
    result
  }

  "LargeBinary" should "accept an s3 URI and expose it verbatim" in {
    val binary = new LargeBinary("s3://my-bucket/objects/42/abc")
    assert(binary.getUri == "s3://my-bucket/objects/42/abc")
    assert(binary.toString == "s3://my-bucket/objects/42/abc")
  }

  it should "reject a null URI" in {
    val ex = intercept[IllegalArgumentException] {
      new LargeBinary(null)
    }
    assert(ex.getMessage == "LargeBinary URI cannot be null")
  }

  it should "reject any URI that does not start with the s3 scheme" in {
    // the prefix check is a literal, case-sensitive "s3://" match
    List("http://bucket/key", "s3:/bucket/key", "S3://bucket/key", "/local/path", "", "bucket/key")
      .foreach { bad =>
        val ex = intercept[IllegalArgumentException] {
          new LargeBinary(bad)
        }
        assert(
          ex.getMessage == s"LargeBinary URI must start with 's3://', got: $bad",
          s"unexpected message for '$bad'"
        )
      }
  }

  it should "split an s3 URI into bucket name and object key" in {
    val binary = new LargeBinary("s3://texera-large-binaries/objects/7/some-uuid.bin")
    assert(binary.getBucketName == "texera-large-binaries")
    // the leading slash of the URI path is stripped from the object key
    assert(binary.getObjectKey == "objects/7/some-uuid.bin")
  }

  it should "return an empty object key when the URI carries no path" in {
    val binary = new LargeBinary("s3://bucket-only")
    assert(binary.getBucketName == "bucket-only")
    assert(binary.getObjectKey == "")
  }

  it should "keep a single-segment object key" in {
    val binary = new LargeBinary("s3://bucket/key")
    assert(binary.getBucketName == "bucket")
    assert(binary.getObjectKey == "key")
  }

  it should "compare by URI value" in {
    val binary = new LargeBinary("s3://bucket/a")
    val same = new LargeBinary("s3://bucket/a")
    val other = new LargeBinary("s3://bucket/b")

    assert(binary == binary) // identity short-circuit
    assert(binary == same)
    assert(binary.hashCode() == same.hashCode())
    assert(binary != other)
    assert(!binary.equals("s3://bucket/a")) // a bare String is not a LargeBinary
    assert(!binary.equals(null))
  }

  "LargeBinary()" should "derive a unique URI from the current thread's base URI" in {
    val base = "s3://unit-test-bucket/objects/999/"
    val uris = onFreshThread(Some(base)) {
      val first = new LargeBinary()
      val second = new LargeBinary()
      (first, second)
    }.fold(e => throw e, identity)

    val (first, second) = uris
    assert(first.getUri.startsWith(base))
    assert(first.getUri.stripPrefix(base).nonEmpty)
    assert(first.getBucketName == "unit-test-bucket")
    assert(first.getObjectKey.startsWith("objects/999/"))
    // each instance gets its own generated suffix
    assert(first != second)
  }

  it should "propagate the failure when the thread has no base URI" in {
    // a fresh thread starts with no base URI, so the generating constructor fails fast
    val outcome = onFreshThread(None)(new LargeBinary())
    assert(outcome.isLeft)
    assert(outcome.swap.toOption.exists(_.isInstanceOf[IllegalStateException]))
  }

  "LargeBinary JSON" should "serialize to the bare URI string via @JsonValue" in {
    val binary = new LargeBinary("s3://bucket/objects/1/blob")
    assert(objectMapper.writeValueAsString(binary) == "\"s3://bucket/objects/1/blob\"")
  }

  it should "deserialize from the uri property declared by its @JsonCreator" in {
    val restored =
      objectMapper.readValue("{\"uri\":\"s3://bucket/objects/1/blob\"}", classOf[LargeBinary])
    assert(restored == new LargeBinary("s3://bucket/objects/1/blob"))
  }

  it should "reject a payload whose uri is not an s3 URI" in {
    // the constructor guard runs during deserialization, so bad data fails loudly
    intercept[Exception] {
      objectMapper.readValue("{\"uri\":\"http://bucket/key\"}", classOf[LargeBinary])
    }
  }
}
