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

package org.apache.texera.amber.core.storage.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.{URI, URLEncoder}
import java.nio.charset.StandardCharsets
import java.nio.file.Paths

class DatasetFileDocumentSpec extends AnyFlatSpec with Matchers {

  // Realistic 40-char git commit hash, mirroring the URIs produced by FileResolver
  // (format: dataset:///{repositoryName}/{versionHash}/{fileRelativePath}).
  private val versionHash = "97fd4c2a755b69b7c66d322eab40b7e5c2ad5d10"

  "DatasetFileDocument" should "parse a valid 3-segment dataset URI into its components" in {
    val uri = new URI(s"dataset:///test_dataset/$versionHash/1.txt")
    val doc = new DatasetFileDocument(uri)

    doc.getRepositoryName() shouldBe "test_dataset"
    doc.getVersionHash() shouldBe versionHash
    doc.getFileRelativePath() shouldBe "1.txt"
  }

  it should "join multi-segment relative paths correctly" in {
    val uri = new URI(s"dataset:///my_repo/$versionHash/some/nested/dir/data.csv")
    val doc = new DatasetFileDocument(uri)

    doc.getRepositoryName() shouldBe "my_repo"
    doc.getVersionHash() shouldBe versionHash
    doc.getFileRelativePath() shouldBe Paths.get("some", "nested", "dir", "data.csv").toString
  }

  it should "URL-decode the version hash" in {
    // FileResolver URL-encodes segments and then builds the URI with the multi-arg
    // constructor, so uri.getPath still contains URLEncoder-encoded segments.
    val uri = new URI("dataset", "", "/repo/hash%20with%2Bspecials/file.txt", null)
    val doc = new DatasetFileDocument(uri)

    doc.getVersionHash() shouldBe "hash with+specials"
    doc.getFileRelativePath() shouldBe "file.txt"
  }

  it should "URL-decode each relative path segment" in {
    val uri = new URI("dataset", "", "/repo/hash/dir+one/file%23two%20a.csv", null)
    val doc = new DatasetFileDocument(uri)

    doc.getRepositoryName() shouldBe "repo"
    doc.getVersionHash() shouldBe "hash"
    doc.getFileRelativePath() shouldBe Paths.get("dir one", "file#two a.csv").toString
  }

  it should "return the parsed components and the original URI through its getters" in {
    val uri = new URI("dataset", "", s"/repo/$versionHash/a%20b/c.csv", null)
    val doc = new DatasetFileDocument(uri)

    doc.getRepositoryName() shouldBe "repo"
    doc.getVersionHash() shouldBe versionHash
    doc.getFileRelativePath() shouldBe Paths.get("a b", "c.csv").toString
    // getURI must hand back the exact URI the document was constructed with.
    doc.getURI shouldBe uri
    doc.getURI.toString shouldBe uri.toString
  }

  it should "not URL-decode the repository name" in {
    // parseUri only URLDecoder-decodes the version hash and the relative-path
    // segments; the repository name is returned raw. This mirrors FileResolver,
    // which URLEncoder-encodes only the fileRelativePath segments. The multi-arg
    // URI constructor is required here: a single-arg URI already percent-decodes
    // getPath, so "%20" in a raw URI string would reach parseUri as a space.
    val uri = new URI("dataset", "", "/repo%20name/hash%20value/file.txt", null)
    val doc = new DatasetFileDocument(uri)

    doc.getRepositoryName() shouldBe "repo%20name"
    // Same encoded token in the version-hash position IS decoded (asymmetry pin).
    doc.getVersionHash() shouldBe "hash value"
    doc.getFileRelativePath() shouldBe "file.txt"
  }

  it should "round-trip non-ASCII UTF-8 relative path segments encoded FileResolver-style" in {
    val rawSegments = Seq("中文 目录", "中文 文件.csv")
    val encodedPath =
      rawSegments.map(URLEncoder.encode(_, StandardCharsets.UTF_8)).mkString("/")
    val uri = new URI("dataset", "", s"/repo/$versionHash/$encodedPath", null)
    val doc = new DatasetFileDocument(uri)

    doc.getFileRelativePath() shouldBe Paths.get(rawSegments.head, rawSegments.tail: _*).toString
  }

  it should "collapse redundant and trailing slashes in the URI path" in {
    // Paths.get collapses duplicate separators and ignores a trailing slash,
    // so this still yields exactly the three segments [repo, hash, file.txt].
    val doc = new DatasetFileDocument(new URI("dataset:///repo//hash///file.txt/"))

    doc.getRepositoryName() shouldBe "repo"
    doc.getVersionHash() shouldBe "hash"
    doc.getFileRelativePath() shouldBe "file.txt"
  }

  it should "preserve dot segments in the relative path without normalization (current behavior)" in {
    // "." and ".." segments are kept verbatim (current behavior): the relative
    // path is passed downstream un-normalized, with no sanitization applied.
    val parentDoc = new DatasetFileDocument(new URI("dataset:///repo/hash/../x.csv"))
    parentDoc.getFileRelativePath() shouldBe Paths.get("..", "x.csv").toString

    val dotDoc = new DatasetFileDocument(new URI("dataset:///repo/hash/./sub/../x.csv"))
    dotDoc.getFileRelativePath() shouldBe Paths.get(".", "sub", "..", "x.csv").toString
  }

  it should "reject URIs with fewer than three path segments" in {
    val invalidUris = Seq(
      new URI("dataset:///"), // 0 segments
      new URI("dataset:///repo"), // 1 segment
      new URI(s"dataset:///repo/$versionHash"), // 2 segments
      new URI("dataset:///repo/hash/") // trailing slash: still only 2 segments
    )
    invalidUris.foreach { uri =>
      val thrown = intercept[IllegalArgumentException] {
        new DatasetFileDocument(uri)
      }
      thrown.getMessage shouldBe "URI format is incorrect"
    }
  }
}
