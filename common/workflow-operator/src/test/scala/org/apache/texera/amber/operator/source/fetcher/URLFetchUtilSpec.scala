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
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicInteger

class URLFetchUtilSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------

  private def freshTempFile(contents: String): Path = {
    val path = Files.createTempFile("url-fetch-util-spec-", ".bin")
    Files.write(path, contents.getBytes(StandardCharsets.UTF_8))
    path.toFile.deleteOnExit()
    path
  }

  private def fileUrl(path: Path): URL = path.toUri.toURL

  /**
    * A URLStreamHandler that counts how many times the URL is opened (one
    * `openConnection` per retry attempt in `getInputStreamFromURL`) and either
    * yields a fixed byte payload or always fails the stream fetch.
    *
    * Counting the `openConnection` calls lets the tests assert the EXACT number
    * of attempts the retry loop makes — a stronger contract than only checking
    * the final Option, and it does not depend on Scala's synthetic default-arg
    * accessor name (which is compiler/version-specific).
    */
  private class CountingStreamHandler(succeedWithBytes: Option[Array[Byte]])
      extends URLStreamHandler {
    val openConnectionCount = new AtomicInteger(0)
    // Captures the most recent User-Agent request property set on a connection.
    var userAgent: Option[String] = None

    override def openConnection(u: URL): URLConnection = {
      openConnectionCount.incrementAndGet()
      new URLConnection(u) {
        override def connect(): Unit = ()
        override def setRequestProperty(key: String, value: String): Unit =
          if (key == "User-Agent") userAgent = Some(value)
        override def getInputStream: InputStream =
          succeedWithBytes match {
            case Some(bytes) => new ByteArrayInputStream(bytes)
            case None        => throw new IOException("simulated fetch failure")
          }
      }
    }
  }

  private def countingUrl(handler: CountingStreamHandler): URL =
    new URL(null, "counting://retry-test", handler)

  // ---------------------------------------------------------------------------
  // Success path
  // ---------------------------------------------------------------------------

  "URLFetchUtil.getInputStreamFromURL" should
    "return Some(stream) carrying the URL's bytes on success" in {
    val path = freshTempFile("hello-url-fetch")
    val result = URLFetchUtil.getInputStreamFromURL(fileUrl(path))
    assert(result.isDefined)
    try {
      val bytes = result.get.readAllBytes()
      assert(new String(bytes, StandardCharsets.UTF_8) == "hello-url-fetch")
    } finally {
      result.foreach(_.close())
    }
  }

  it should "stop after the first successful attempt (no extra connections)" in {
    val handler =
      new CountingStreamHandler(Some("ok".getBytes(StandardCharsets.UTF_8)))
    val result = URLFetchUtil.getInputStreamFromURL(countingUrl(handler), retries = 5)
    assert(result.isDefined)
    try {
      assert(new String(result.get.readAllBytes(), StandardCharsets.UTF_8) == "ok")
    } finally {
      result.foreach(_.close())
    }
    // First attempt succeeds, so the loop returns immediately.
    assert(handler.openConnectionCount.get() == 1)
  }

  // ---------------------------------------------------------------------------
  // Failure path — exact attempt counts via the counting handler
  // ---------------------------------------------------------------------------

  it should "return None and attempt exactly 5 connections at the default retry count" in {
    val handler = new CountingStreamHandler(None)
    // No `retries` argument supplied → exercises the default value at runtime.
    val result = URLFetchUtil.getInputStreamFromURL(countingUrl(handler))
    assert(result.isEmpty)
    assert(handler.openConnectionCount.get() == 5)
  }

  it should "never open a connection when retries is 0 (loop body runs zero times)" in {
    val handler = new CountingStreamHandler(None)
    val result = URLFetchUtil.getInputStreamFromURL(countingUrl(handler), retries = 0)
    assert(result.isEmpty)
    assert(handler.openConnectionCount.get() == 0)
  }

  it should "attempt exactly 1 connection when retries is 1" in {
    val handler = new CountingStreamHandler(None)
    val result = URLFetchUtil.getInputStreamFromURL(countingUrl(handler), retries = 1)
    assert(result.isEmpty)
    assert(handler.openConnectionCount.get() == 1)
  }

  it should "attempt exactly N connections on persistent failure (N = retries)" in {
    val handler = new CountingStreamHandler(None)
    val result = URLFetchUtil.getInputStreamFromURL(countingUrl(handler), retries = 2)
    assert(result.isEmpty)
    assert(handler.openConnectionCount.get() == 2)
  }

  // ---------------------------------------------------------------------------
  // Request headers
  // ---------------------------------------------------------------------------

  it should "set a User-Agent request property before reading the stream" in {
    val handler =
      new CountingStreamHandler(Some("ok".getBytes(StandardCharsets.UTF_8)))
    val result = URLFetchUtil.getInputStreamFromURL(countingUrl(handler))
    result.foreach(_.close())
    // Pin only that the header is set, not its specific (randomized) value.
    assert(handler.userAgent.isDefined)
  }
}
