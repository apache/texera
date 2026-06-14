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

import java.io.{ByteArrayInputStream, IOException, InputStream}
import java.net.{URL, URLConnection, URLStreamHandler}

class URLFetchUtilSpec extends AnyFlatSpec {

  // Stub URLStreamHandler from the issue's hint, extended only to capture the
  // User-Agent header (contract row 7). It drives URLFetchUtil with no network:
  // getInputStream returns scripted bytes (Right) or throws (Left) per attempt.
  // Injected per-URL via stubUrl so each test is isolated.
  class StubHandler(behavior: (Int) => Either[IOException, Array[Byte]]) extends URLStreamHandler {
    val attempts = new java.util.concurrent.atomic.AtomicInteger(0)
    var userAgent: Option[String] = None
    protected override def openConnection(u: URL): URLConnection =
      new URLConnection(u) {
        override def connect(): Unit = ()
        override def setRequestProperty(key: String, value: String): Unit =
          if (key == "User-Agent") userAgent = Some(value)
        override def getInputStream: InputStream = {
          val i = attempts.incrementAndGet()
          behavior(i) match {
            case Right(bytes) => new ByteArrayInputStream(bytes)
            case Left(e)      => throw e
          }
        }
      }
  }

  private def stubUrl(handler: StubHandler): URL =
    new URL(null, "http://example.com/data", handler)

  // Successful single attempt —
  // returns Some(InputStream) on the first try and reads the body
  "URLFetchUtil.getInputStreamFromURL" should
    "return Some on the first try and read the body" in {
    val body = "hello".getBytes("UTF-8")
    val handler = new StubHandler(_ => Right(body))
    val result = URLFetchUtil.getInputStreamFromURL(stubUrl(handler))
    assert(result.isDefined)
    assert(result.get.readAllBytes().sameElements(body))
    assert(handler.attempts.get() == 1)
  }

  // Default retries = 5 —
  // up to 5 total attempts when every attempt fails
  it should "make up to 5 total attempts when every attempt fails" in {
    val handler = new StubHandler(_ => Left(new IOException("boom")))
    val result = URLFetchUtil.getInputStreamFromURL(stubUrl(handler))
    assert(result.isEmpty)
    assert(handler.attempts.get() == 5)
  }

  // Explicit retries = 1 —
  // exactly one attempt
  it should "make exactly one attempt when retries = 1" in {
    val handler = new StubHandler(_ => Left(new IOException("boom")))
    val result = URLFetchUtil.getInputStreamFromURL(stubUrl(handler), retries = 1)
    assert(result.isEmpty)
    assert(handler.attempts.get() == 1)
  }

  // Explicit retries = 0 —
  // returns None immediately (loop body never enters)
  it should "return None immediately, never entering the loop body, when retries = 0" in {
    val handler = new StubHandler(_ => Left(new IOException("boom")))
    val result = URLFetchUtil.getInputStreamFromURL(stubUrl(handler), retries = 0)
    assert(result.isEmpty)
    assert(handler.attempts.get() == 0)
  }

  // All attempts fail —
  // returns None (does NOT throw — the inner try/catch swallows exceptions)
  it should "return None (does NOT throw — the inner try/catch swallows exceptions)" in {
    val handler = new StubHandler(_ => Left(new IOException("boom")))
    val result = URLFetchUtil.getInputStreamFromURL(stubUrl(handler), retries = 3)
    assert(result.isEmpty)
  }

  // Stops at first success —
  // when a later attempt succeeds, no further attempts are made
  it should "make no further attempts when a later attempt succeeds" in {
    val body = "third-time".getBytes("UTF-8")
    val handler = new StubHandler(i => if (i < 3) Left(new IOException("fail")) else Right(body))
    val result = URLFetchUtil.getInputStreamFromURL(stubUrl(handler), retries = 5)
    assert(result.isDefined)
    assert(result.get.readAllBytes().sameElements(body))
    assert(handler.attempts.get() == 3)
  }

  // Sets User-Agent request property —
  // the value comes from RandomUserAgent.getRandomUserAgent (use a URLStreamHandler to
  // capture headers and pin that the header is set, without pinning its value)
  it should "set the User-Agent request property (without pinning its value)" in {
    val handler = new StubHandler(_ => Right("x".getBytes("UTF-8")))
    URLFetchUtil.getInputStreamFromURL(stubUrl(handler))
    assert(handler.userAgent.isDefined)
    assert(handler.userAgent.get.nonEmpty)
  }
}
