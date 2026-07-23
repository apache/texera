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

package org.apache.texera.service.resource

import jakarta.ws.rs.BadRequestException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// Focused unit tests for the DatasetResource companion helper
// validateAndNormalizeFilePathOrThrow, which guards every upload/lookup path.
// These call the pure companion method directly and avoid the heavy
// DatasetResourceSpec integration harness (DB + LakeFS).
class DatasetResourcePathSpec extends AnyFlatSpec with Matchers {

  "validateAndNormalizeFilePathOrThrow" should "reject a null path" in {
    val ex = intercept[BadRequestException] {
      DatasetResource.validateAndNormalizeFilePathOrThrow(null)
    }
    ex.getMessage shouldBe "Path cannot be empty"
  }

  it should "reject an empty path" in {
    val ex = intercept[BadRequestException] {
      DatasetResource.validateAndNormalizeFilePathOrThrow("")
    }
    ex.getMessage shouldBe "Path cannot be empty"
  }

  it should "reject a whitespace-only path" in {
    val ex = intercept[BadRequestException] {
      DatasetResource.validateAndNormalizeFilePathOrThrow("   ")
    }
    ex.getMessage shouldBe "Path cannot be empty"
  }

  it should "reject a path that normalizes to null (escapes above root)" in {
    // FilenameUtils.normalize returns null when the path traverses above the
    // root, e.g. a leading "../".
    val ex = intercept[BadRequestException] {
      DatasetResource.validateAndNormalizeFilePathOrThrow("../secret.txt")
    }
    ex.getMessage shouldBe "Invalid path"
  }

  it should "reject an absolute path" in {
    val ex = intercept[BadRequestException] {
      DatasetResource.validateAndNormalizeFilePathOrThrow("/etc/passwd")
    }
    ex.getMessage shouldBe "Absolute paths not allowed"
  }

  it should "return a normalized relative path unchanged when already clean" in {
    DatasetResource.validateAndNormalizeFilePathOrThrow(
      "california/irvine/tw1.csv"
    ) shouldBe "california/irvine/tw1.csv"
  }

  it should "collapse interior '.' and '..' segments in a relative path" in {
    DatasetResource.validateAndNormalizeFilePathOrThrow(
      "a/./b/../c.csv"
    ) shouldBe "a/c.csv"
  }
}
