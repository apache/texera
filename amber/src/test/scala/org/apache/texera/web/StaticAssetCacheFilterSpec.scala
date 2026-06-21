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

package org.apache.texera.web

import org.apache.texera.web.StaticAssetCacheFilter.{ImmutableCacheControl, RevalidateCacheControl}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StaticAssetCacheFilterSpec extends AnyFlatSpec with Matchers {

  private def cc(path: String) = StaticAssetCacheFilter.cacheControlFor(path)

  "cacheControlFor" should "mark content-hashed JS and CSS bundles immutable" in {
    cc("/main.138cf96bab6ef6d9.js") shouldBe Some(ImmutableCacheControl)
    cc("/styles.266ff0ada80cd80a.css") shouldBe Some(ImmutableCacheControl)
    cc("/polyfills.9d67f25b35182fa7.js") shouldBe Some(ImmutableCacheControl)
  }

  it should "mark content-hashed media assets immutable" in {
    cc("/assets/roboto.abcdef12.woff2") shouldBe Some(ImmutableCacheControl)
  }

  it should "force revalidation of the index document so a deploy is never served stale" in {
    cc("/") shouldBe Some(RevalidateCacheControl)
    cc("/index.html") shouldBe Some(RevalidateCacheControl)
  }

  it should "force revalidation of Angular route paths (served the index document via the 404 fallback)" in {
    cc("/dashboard") shouldBe Some(RevalidateCacheControl)
    cc("/dashboard/workflow/42") shouldBe Some(RevalidateCacheControl)
  }

  it should "force revalidation of non-fingerprinted static files" in {
    cc("/favicon.ico") shouldBe Some(RevalidateCacheControl)
    cc("/assets/logo.png") shouldBe Some(RevalidateCacheControl)
    cc("/3rdpartylicenses.txt") shouldBe Some(RevalidateCacheControl)
  }

  it should "leave backend /api/* responses untouched" in {
    cc("/api/workflow/123") shouldBe None
    cc("/api/auth/login") shouldBe None
  }

  it should "not mistake a short numeric segment for a content hash" in {
    // "v2" / "12345" are too short to be a fingerprint; only 8+ hex chars qualify.
    cc("/app.v2.js") shouldBe Some(RevalidateCacheControl)
    cc("/data.12345.json") shouldBe Some(RevalidateCacheControl)
  }

  it should "not freeze long purely-numeric segments (dates, version numbers)" in {
    // A real content hash contains hex letters; an all-digit segment is more likely a
    // date or version stamp and must not be cached immutably for a year.
    cc("/report.20240101.csv") shouldBe Some(RevalidateCacheControl)
    cc("/photo.20240101120000.jpg") shouldBe Some(RevalidateCacheControl)
  }
}
