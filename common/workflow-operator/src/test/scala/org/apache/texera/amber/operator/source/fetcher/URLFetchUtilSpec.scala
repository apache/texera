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

package org.apache.texera.amber.operator.source.fetcher

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException, InputStream}
import java.net.{URL, URLConnection, URLStreamHandler}
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

class URLFetchUtilSpec extends AnyFlatSpec with Matchers {

  private class StubHandler(behavior: Int => Either[IOException, Array[Byte]])
      extends URLStreamHandler {

    val attempts = new AtomicInteger(0)
    val requestProperties: mutable.Map[String, String] = mutable.Map.empty

    override protected def openConnection(url: URL): URLConnection = new URLConnection(url) {
      override def connect(): Unit = {}

      override def setRequestProperty(key: String, value: String): Unit = {
        requestProperties.update(key, value)
      }

      override def getRequestProperty(key: String): String = {
        requestProperties.getOrElse(key, null)
      }

      override def getInputStream: InputStream = {
        val attempt = attempts.incrementAndGet()
        behavior(attempt) match {
          case Right(bytes) => new ByteArrayInputStream(bytes)
          case Left(error)  => throw error
        }
      }
    }
  }

  private def stubUrl(handler: StubHandler): URL = {
    new URL(null, "stub://url-fetch-util-test", handler)
  }

  private def bytes(value: String): Array[Byte] = {
    value.getBytes(StandardCharsets.UTF_8)
  }

  private def readStream(inputStream: InputStream): String = {
    val output = new ByteArrayOutputStream()
    val buffer = new Array[Byte](1024)
    var length = inputStream.read(buffer)

    while (length != -1) {
      output.write(buffer, 0, length)
      length = inputStream.read(buffer)
    }

    new String(output.toByteArray, StandardCharsets.UTF_8)
  }

  "getInputStreamFromURL" should "return Some input stream on the first successful attempt" in {
    val handler = new StubHandler(_ => Right(bytes("success")))

    val result = URLFetchUtil.getInputStreamFromURL(stubUrl(handler), retries = 5)

    result.isDefined shouldBe true
    readStream(result.get) shouldBe "success"
    handler.attempts.get() shouldBe 1
  }

  it should "attempt five times by default when every attempt fails" in {
    val handler = new StubHandler(_ => Left(new IOException("boom")))

    val result = URLFetchUtil.getInputStreamFromURL(stubUrl(handler))

    result shouldBe None
    handler.attempts.get() shouldBe 5
  }

  it should "attempt exactly once when retries is one" in {
    val handler = new StubHandler(_ => Left(new IOException("boom")))

    val result = URLFetchUtil.getInputStreamFromURL(stubUrl(handler), retries = 1)

    result shouldBe None
    handler.attempts.get() shouldBe 1
  }

  it should "return None without opening a connection when retries is zero" in {
    val handler = new StubHandler(_ => Right(bytes("should not be read")))

    val result = URLFetchUtil.getInputStreamFromURL(stubUrl(handler), retries = 0)

    result shouldBe None
    handler.attempts.get() shouldBe 0
  }

  it should "swallow failures and return None after all attempts fail" in {
    val handler = new StubHandler(_ => Left(new IOException("always fails")))

    val result = URLFetchUtil.getInputStreamFromURL(stubUrl(handler), retries = 3)

    result shouldBe None
    handler.attempts.get() shouldBe 3
  }

  it should "stop retrying after a later attempt succeeds" in {
    val handler = new StubHandler(attempt =>
      if (attempt < 3) {
        Left(new IOException(s"failure $attempt"))
      } else {
        Right(bytes("eventual success"))
      }
    )

    val result = URLFetchUtil.getInputStreamFromURL(stubUrl(handler), retries = 5)

    result.isDefined shouldBe true
    readStream(result.get) shouldBe "eventual success"
    handler.attempts.get() shouldBe 3
  }

  it should "set the User-Agent request property" in {
    val handler = new StubHandler(_ => Right(bytes("success")))

    val result = URLFetchUtil.getInputStreamFromURL(stubUrl(handler), retries = 1)

    result.isDefined shouldBe true
    handler.requestProperties.contains("User-Agent") shouldBe true
    handler.requestProperties("User-Agent").nonEmpty shouldBe true
  }
}
