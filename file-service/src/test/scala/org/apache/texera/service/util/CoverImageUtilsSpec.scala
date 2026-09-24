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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * The extension allowlist is a security control: a cover path is served to the browser
  * as a presigned URL, so an active document (.svg, .html) is a stored-XSS vector.
  */
class CoverImageUtilsSpec extends AnyFlatSpec with Matchers {

  private val maxLen = CoverImageUtils.MAX_PATH_LENGTH

  // -- accepted paths ---------------------------------------------------------

  "validatePathOrThrow" should "accept each allowed image extension" in {
    Seq("jpg", "jpeg", "png", "gif", "webp").foreach { ext =>
      CoverImageUtils.validatePathOrThrow(s"v1/cover.$ext", maxLen) shouldBe s"v1/cover.$ext"
    }
  }

  it should "accept uppercase and mixed-case extensions" in {
    CoverImageUtils.validatePathOrThrow("v1/COVER.JPG", maxLen) shouldBe "v1/COVER.JPG"
    CoverImageUtils.validatePathOrThrow("v1/Cover.PnG", maxLen) shouldBe "v1/Cover.PnG"
  }

  it should "accept the generated version-name form, which contains spaces" in {
    CoverImageUtils.validatePathOrThrow("v1 - init/cover.jpg", maxLen) shouldBe
      "v1 - init/cover.jpg"
  }

  it should "accept unicode in the file name" in {
    CoverImageUtils.validatePathOrThrow("v1/カバー画像.jpg", maxLen) shouldBe "v1/カバー画像.jpg"
  }

  it should "accept a double extension whose last segment is allowed" in {
    CoverImageUtils.validatePathOrThrow("v1/cover.png.jpg", maxLen) shouldBe "v1/cover.png.jpg"
  }

  it should "normalize redundant path segments" in {
    CoverImageUtils.validatePathOrThrow("v1/./cover.jpg", maxLen) shouldBe "v1/cover.jpg"
    CoverImageUtils.validatePathOrThrow("v1/sub/../cover.jpg", maxLen) shouldBe "v1/cover.jpg"
  }

  // -- rejected paths ---------------------------------------------------------

  it should "reject path traversal above the root" in {
    Seq("../../../etc/passwd", "v1/../../secret.jpg", "../escape.jpg").foreach { p =>
      a[BadRequestException] should be thrownBy CoverImageUtils.validatePathOrThrow(p, maxLen)
    }
  }

  it should "reject absolute paths" in {
    Seq("/etc/passwd.jpg", "C:/x.jpg").foreach { p =>
      a[BadRequestException] should be thrownBy CoverImageUtils.validatePathOrThrow(p, maxLen)
    }
  }

  it should "reject null, empty and whitespace-only paths" in {
    Seq(null, "", "   ").foreach { p =>
      a[BadRequestException] should be thrownBy CoverImageUtils.validatePathOrThrow(p, maxLen)
    }
  }

  it should "reject a non-image extension" in {
    Seq("v1/cover.js", "v1/cover.pdf", "v1/cover.csv", "v1/weights.safetensors").foreach { p =>
      a[BadRequestException] should be thrownBy CoverImageUtils.validatePathOrThrow(p, maxLen)
    }
  }

  it should "reject .svg and .html, which the browser would execute" in {
    Seq("v1/cover.svg", "v1/cover.html", "v1/cover.htm").foreach { p =>
      a[BadRequestException] should be thrownBy CoverImageUtils.validatePathOrThrow(p, maxLen)
    }
  }

  it should "reject a path with no extension or a trailing dot" in {
    Seq("v1/cover", "v1/cover.").foreach { p =>
      a[BadRequestException] should be thrownBy CoverImageUtils.validatePathOrThrow(p, maxLen)
    }
  }

  it should "reject a double extension whose last segment is not allowed" in {
    a[BadRequestException] should be thrownBy
      CoverImageUtils.validatePathOrThrow("v1/cover.jpg.js", maxLen)
  }

  it should "reject a bare file name with no version segment" in {
    Seq("cover.jpg", "./cover.png", "sub/../cover.gif").foreach { p =>
      withClue(s"$p: ") {
        a[BadRequestException] should be thrownBy CoverImageUtils.validatePathOrThrow(p, maxLen)
      }
    }
  }

  it should "accept a path nested deeper than <version>/<file>" in {
    CoverImageUtils.validatePathOrThrow("v1/img/sub/cover.jpg", maxLen) shouldBe
      "v1/img/sub/cover.jpg"
  }

  // -- length boundary --------------------------------------------------------

  it should "accept a path exactly at the column length and reject one over it" in {
    val suffix = "/c.jpg"
    val atLimit = "v" + "a" * (255 - suffix.length - 1) + suffix
    atLimit.length shouldBe 255
    CoverImageUtils.validatePathOrThrow(atLimit, 255) shouldBe atLimit

    val overLimit = atLimit + "x"
    a[BadRequestException] should be thrownBy CoverImageUtils.validatePathOrThrow(overLimit, 255)
  }

  it should "honour the narrower limit datasets pass" in {
    // dataset.cover_image is varchar(246), so the caller's limit must win.
    val suffix = "/c.jpg"
    val at246 = "v" + "a" * (246 - suffix.length - 1) + suffix
    CoverImageUtils.validatePathOrThrow(at246, 246) shouldBe at246
    a[BadRequestException] should be thrownBy CoverImageUtils.validatePathOrThrow(at246, 245)
  }

  // -- size limit -------------------------------------------------------------

  "requireWithinSizeLimit" should "accept a file at exactly the limit" in {
    noException should be thrownBy
      CoverImageUtils.requireWithinSizeLimit(CoverImageUtils.SIZE_LIMIT_BYTES)
  }

  it should "reject a file one byte over the limit, naming the limit in MB" in {
    val ex = intercept[BadRequestException] {
      CoverImageUtils.requireWithinSizeLimit(CoverImageUtils.SIZE_LIMIT_BYTES + 1)
    }
    ex.getMessage should include("10 MB")
  }

  it should "accept a zero-byte file" in {
    noException should be thrownBy CoverImageUtils.requireWithinSizeLimit(0L)
  }
}
