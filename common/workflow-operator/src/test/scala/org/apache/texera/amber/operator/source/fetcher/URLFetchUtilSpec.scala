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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

class URLFetchUtilSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Fixtures — temp files reachable via the JVM's built-in `file:` URL handler
  // ---------------------------------------------------------------------------

  private def freshTempFile(contents: String): Path = {
    val path = Files.createTempFile("url-fetch-util-spec-", ".bin")
    Files.write(path, contents.getBytes(StandardCharsets.UTF_8))
    path.toFile.deleteOnExit()
    path
  }

  private def fileUrl(path: Path): java.net.URL = path.toUri.toURL

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

  it should "return Some(stream) when explicit retries is supplied (>= 1)" in {
    val path = freshTempFile("with-retries")
    val result = URLFetchUtil.getInputStreamFromURL(fileUrl(path), retries = 3)
    assert(result.isDefined)
    try {
      val bytes = result.get.readAllBytes()
      assert(new String(bytes, StandardCharsets.UTF_8) == "with-retries")
    } finally {
      result.foreach(_.close())
    }
  }

  // ---------------------------------------------------------------------------
  // Failure path — non-existent file URL exhausts retries and returns None
  // ---------------------------------------------------------------------------

  it should "return None when the URL never produces an input stream (default retries)" in {
    val missing = new java.io.File(
      System.getProperty("java.io.tmpdir"),
      "this-file-must-not-exist-" + System.nanoTime()
    )
    val url = missing.toURI.toURL
    val result = URLFetchUtil.getInputStreamFromURL(url)
    assert(result.isEmpty)
  }

  it should "return None immediately when retries is 0 (loop iterates zero times)" in {
    val missing = new java.io.File(
      System.getProperty("java.io.tmpdir"),
      "still-must-not-exist-" + System.nanoTime()
    )
    val url = missing.toURI.toURL
    val result = URLFetchUtil.getInputStreamFromURL(url, retries = 0)
    assert(result.isEmpty)
  }

  it should "return None after the requested number of retries on persistent failure" in {
    val missing = new java.io.File(
      System.getProperty("java.io.tmpdir"),
      "absent-" + System.nanoTime()
    )
    val url = missing.toURI.toURL
    val result = URLFetchUtil.getInputStreamFromURL(url, retries = 2)
    assert(result.isEmpty)
  }

  // ---------------------------------------------------------------------------
  // Default-arg shape — exposed via Scala's synthetic accessor
  // ---------------------------------------------------------------------------

  "URLFetchUtil.getInputStreamFromURL$default$2" should "default retries to 5" in {
    val cls = URLFetchUtil.getClass
    val accessor = cls.getDeclaredMethod("getInputStreamFromURL$default$2")
    accessor.setAccessible(true)
    val default = accessor.invoke(URLFetchUtil)
    assert(default == Integer.valueOf(5))
  }
}
