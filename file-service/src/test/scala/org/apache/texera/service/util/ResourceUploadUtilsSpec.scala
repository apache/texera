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

import jakarta.ws.rs.BadRequestException
import org.apache.texera.service.`type`.ExistingUploadFile
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// Unit tests for the resource-agnostic upload matcher shared by the dataset and
// model "which of these files do I already have?" endpoints. The helper is pure,
// so these run without a DB or LakeFS.
class ResourceUploadUtilsSpec extends AnyFlatSpec with Matchers {

  private def committed = List("committed.csv" -> 10L, "shared.bin" -> 30L)
  private def staged = List("staged.csv" -> 20L)

  /** The two halves as the endpoints chain them: validate first, then match. */
  private def matchNormalized(
      requested: List[ExistingUploadFile],
      committed: List[(String, Long)],
      staged: List[(String, Long)]
  ): List[String] =
    ResourceUploadUtils.matchExistingUploads(
      ResourceUploadUtils.normalizeUploadRequest(requested),
      committed,
      staged
    )

  "matchExistingUploads" should "return files whose path and size both match" in {
    matchNormalized(
      List(ExistingUploadFile("committed.csv", 10L), ExistingUploadFile("staged.csv", 20L)),
      committed,
      staged
    ) shouldBe List("committed.csv", "staged.csv")
  }

  it should "drop a file whose path matches but whose size differs" in {
    matchNormalized(
      List(ExistingUploadFile("committed.csv", 11L)),
      committed,
      staged
    ) shouldBe empty
  }

  it should "drop a file that exists in neither the committed nor the staged set" in {
    matchNormalized(
      List(ExistingUploadFile("missing.csv", 10L)),
      committed,
      staged
    ) shouldBe empty
  }

  it should "let a staged entry shadow a committed entry of the same path" in {
    // (committed ++ staged).toMap — the staged size wins, because it is what a
    // re-upload would collide with.
    matchNormalized(
      List(ExistingUploadFile("shared.bin", 99L)),
      committed,
      List("shared.bin" -> 99L)
    ) shouldBe List("shared.bin")
  }

  it should "return the caller's original path, not the normalized one" in {
    matchNormalized(
      List(ExistingUploadFile("a/./b/../committed.csv", 10L)),
      List("a/committed.csv" -> 10L),
      Nil
    ) shouldBe List("a/./b/../committed.csv")
  }

  it should "return matches distinct and sorted" in {
    matchNormalized(
      List(
        ExistingUploadFile("staged.csv", 20L),
        ExistingUploadFile("committed.csv", 10L),
        ExistingUploadFile("staged.csv", 20L)
      ),
      committed,
      staged
    ) shouldBe List("committed.csv", "staged.csv")
  }

  it should "return nothing for an empty request" in {
    matchNormalized(Nil, committed, staged) shouldBe empty
  }

  it should "return nothing when there is nothing on the server yet" in {
    matchNormalized(
      List(ExistingUploadFile("committed.csv", 10L)),
      Nil,
      Nil
    ) shouldBe empty
  }

  it should "reject a negative size" in {
    val ex = intercept[BadRequestException] {
      matchNormalized(
        List(ExistingUploadFile("committed.csv", -1L)),
        committed,
        staged
      )
    }
    ex.getMessage shouldBe "sizeBytes must be >= 0"
  }

  it should "accept a zero size" in {
    matchNormalized(
      List(ExistingUploadFile("empty.txt", 0L)),
      List("empty.txt" -> 0L),
      Nil
    ) shouldBe List("empty.txt")
  }

  it should "reject a path that escapes above the root" in {
    val ex = intercept[BadRequestException] {
      matchNormalized(
        List(ExistingUploadFile("../secret.txt", 1L)),
        committed,
        staged
      )
    }
    ex.getMessage shouldBe "Invalid path"
  }

  it should "reject an absolute path" in {
    intercept[BadRequestException] {
      matchNormalized(
        List(ExistingUploadFile("/etc/passwd", 1L)),
        committed,
        staged
      )
    }.getMessage shouldBe "Absolute paths not allowed"
  }

  it should "treat a null request list as empty" in {
    matchNormalized(null, committed, staged) shouldBe empty
  }

  // The endpoints call normalizeUploadRequest before touching storage, so a bad
  // request costs no network round trips.
  "normalizeUploadRequest" should "reject a bad path without needing any repository state" in {
    intercept[BadRequestException] {
      ResourceUploadUtils.normalizeUploadRequest(List(ExistingUploadFile("../secret.txt", 1L)))
    }.getMessage shouldBe "Invalid path"
  }

  it should "reject a negative size without needing any repository state" in {
    intercept[BadRequestException] {
      ResourceUploadUtils.normalizeUploadRequest(List(ExistingUploadFile("a.csv", -1L)))
    }.getMessage shouldBe "sizeBytes must be >= 0"
  }

  it should "keep both the normalized and the original spelling of a path" in {
    val normalized =
      ResourceUploadUtils.normalizeUploadRequest(List(ExistingUploadFile("a/./b/../c.csv", 5L)))

    normalized.map(_.path) shouldBe List("a/c.csv")
    normalized.map(_.originalPath) shouldBe List("a/./b/../c.csv")
    normalized.map(_.sizeBytes) shouldBe List(5L)
  }

  it should "treat a null list as empty" in {
    ResourceUploadUtils.normalizeUploadRequest(null) shouldBe empty
  }
}
