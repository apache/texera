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

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.util.concurrent.atomic.AtomicBoolean
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import javax.servlet.{FilterChain, ServletRequest, ServletResponse}
import scala.collection.mutable

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

  it should "fingerprint assets in nested directories and multi-dot chunk names" in {
    cc("/assets/fonts/roboto.abcdef12.woff2") shouldBe Some(ImmutableCacheControl)
    cc("/vendor.es2015.8a9b0c1d2e3f4a5b.js") shouldBe Some(ImmutableCacheControl)
  }

  it should "only match lowercase hex hashes, as emitted by the Angular build" in {
    cc("/main.ABCDEF1234567890.js") shouldBe Some(RevalidateCacheControl)
  }

  it should "only exclude the /api/ prefix, not paths merely starting with 'api'" in {
    cc("/api") shouldBe Some(RevalidateCacheControl)
    cc("/api-docs.html") shouldBe Some(RevalidateCacheControl)
  }

  it should "require at least eight hex characters for a fingerprint" in {
    // Seven hex chars is one short of an Angular content hash and must not be frozen.
    cc("/main.abcdef1.js") shouldBe Some(RevalidateCacheControl)
    // Eight is the minimum that qualifies.
    cc("/main.abcdef12.js") shouldBe Some(ImmutableCacheControl)
  }

  it should "not freeze an all-digit segment even at fingerprint length" in {
    // Eight digits is long enough for the regex but contains no hex letter, so it is
    // treated as a version/date stamp rather than a content hash.
    cc("/main.12345678.js") shouldBe Some(RevalidateCacheControl)
  }

  it should "not treat non-hex letters as a content hash" in {
    cc("/main.ghijklmn.js") shouldBe Some(RevalidateCacheControl)
    cc("/main.zzzzzzzz.js") shouldBe Some(RevalidateCacheControl)
  }

  it should "fingerprint assets regardless of the file extension's case" in {
    cc("/main.138cf96bab6ef6d9.JS") shouldBe Some(ImmutableCacheControl)
    cc("/styles.266ff0ada80cd80a.CSS") shouldBe Some(ImmutableCacheControl)
  }

  it should "force revalidation of directory paths ending in a slash" in {
    cc("/assets/") shouldBe Some(RevalidateCacheControl)
    cc("/dashboard/") shouldBe Some(RevalidateCacheControl)
  }

  // --- doFilter wiring, exercised via dependency-free dynamic-proxy doubles ---

  // A proxy that answers the handled methods and returns nulls/zeros for everything else.
  private def proxy[T](
      cls: Class[T]
  )(handler: PartialFunction[(String, Seq[AnyRef]), AnyRef]): T = {
    val h = new InvocationHandler {
      override def invoke(p: Any, m: Method, args: Array[AnyRef]): AnyRef = {
        val a = if (args == null) Seq.empty[AnyRef] else args.toSeq
        handler.applyOrElse(
          (m.getName, a),
          (_: (String, Seq[AnyRef])) => defaultValue(m.getReturnType)
        )
      }
    }
    Proxy.newProxyInstance(cls.getClassLoader, Array[Class[_]](cls), h).asInstanceOf[T]
  }

  private def defaultValue(t: Class[_]): AnyRef =
    if (t == java.lang.Boolean.TYPE) java.lang.Boolean.FALSE
    else if (t == java.lang.Integer.TYPE) java.lang.Integer.valueOf(0)
    else if (t == java.lang.Long.TYPE) java.lang.Long.valueOf(0L)
    else null

  private def httpRequest(uri: String): HttpServletRequest =
    proxy(classOf[HttpServletRequest]) { case ("getRequestURI", _) => uri }

  private def httpResponse(into: mutable.Map[String, String]): HttpServletResponse =
    proxy(classOf[HttpServletResponse]) {
      case ("setHeader", Seq(name, value)) => into.update(name.toString, value.toString); null
    }

  private def recordingChain(invoked: AtomicBoolean): FilterChain =
    (_: ServletRequest, _: ServletResponse) => invoked.set(true)

  "doFilter" should "set immutable Cache-Control on a fingerprinted asset and continue the chain" in {
    val headers = mutable.Map.empty[String, String]
    val chained = new AtomicBoolean(false)
    new StaticAssetCacheFilter()
      .doFilter(
        httpRequest("/main.138cf96bab6ef6d9.js"),
        httpResponse(headers),
        recordingChain(chained)
      )
    headers.get("Cache-Control") shouldBe Some(ImmutableCacheControl)
    chained.get() shouldBe true
  }

  it should "set revalidate Cache-Control on a non-fingerprinted asset" in {
    val headers = mutable.Map.empty[String, String]
    val chained = new AtomicBoolean(false)
    new StaticAssetCacheFilter()
      .doFilter(httpRequest("/index.html"), httpResponse(headers), recordingChain(chained))
    headers.get("Cache-Control") shouldBe Some(RevalidateCacheControl)
    chained.get() shouldBe true
  }

  it should "leave /api/* responses untouched but still continue the chain" in {
    val headers = mutable.Map.empty[String, String]
    val chained = new AtomicBoolean(false)
    new StaticAssetCacheFilter()
      .doFilter(httpRequest("/api/workflow/1"), httpResponse(headers), recordingChain(chained))
    headers shouldBe empty
    chained.get() shouldBe true
  }

  it should "ignore non-HTTP request/response pairs but still continue the chain" in {
    val chained = new AtomicBoolean(false)
    new StaticAssetCacheFilter().doFilter(
      proxy(classOf[ServletRequest])(PartialFunction.empty),
      proxy(classOf[ServletResponse])(PartialFunction.empty),
      recordingChain(chained)
    )
    chained.get() shouldBe true
  }

  it should "ignore an HTTP request paired with a non-HTTP response but still continue the chain" in {
    val chained = new AtomicBoolean(false)
    new StaticAssetCacheFilter().doFilter(
      httpRequest("/main.138cf96bab6ef6d9.js"),
      proxy(classOf[ServletResponse])(PartialFunction.empty),
      recordingChain(chained)
    )
    chained.get() shouldBe true
  }

  it should "ignore a non-HTTP request paired with an HTTP response but still continue the chain" in {
    val headers = mutable.Map.empty[String, String]
    val chained = new AtomicBoolean(false)
    new StaticAssetCacheFilter().doFilter(
      proxy(classOf[ServletRequest])(PartialFunction.empty),
      httpResponse(headers),
      recordingChain(chained)
    )
    headers shouldBe empty
    chained.get() shouldBe true
  }

  "init and destroy" should "be no-ops that do not throw" in {
    val filter = new StaticAssetCacheFilter()
    noException should be thrownBy {
      filter.init(null)
      filter.destroy()
    }
  }
}
