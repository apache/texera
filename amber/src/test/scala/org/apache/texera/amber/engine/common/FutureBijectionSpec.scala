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

package org.apache.texera.amber.engine.common

import com.twitter.util.{Await => TwitterAwait, Future => TwitterFuture}
import org.apache.texera.amber.engine.common.FutureBijection._
import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.duration._
import scala.concurrent.{Await => ScalaAwait, Future => ScalaFuture}

class FutureBijectionSpec extends AnyFlatSpec {

  // Short timeout — the futures complete synchronously (or near-so) for
  // the values used here; we keep the bound tight so a regression that
  // *fails to resolve* surfaces as a timeout rather than hanging CI.
  private val timeout: FiniteDuration = 5.seconds
  private val twitterTimeout: com.twitter.util.Duration = com.twitter.util.Duration.fromSeconds(5)

  // ---------------------------------------------------------------------------
  // Twitter Future → Scala Future
  // ---------------------------------------------------------------------------

  "RichTwitterFuture.asScala" should "resolve to the same value as the wrapped TwitterFuture" in {
    val tf = TwitterFuture.value(42)
    val result = ScalaAwait.result(tf.asScala, timeout)
    assert(result == 42)
  }

  it should "preserve the exception type and message on the failure path" in {
    val cause = new IllegalStateException("boom")
    val tf = TwitterFuture.exception[Int](cause)
    val ex = intercept[IllegalStateException] {
      ScalaAwait.result(tf.asScala, timeout)
    }
    assert(ex.getMessage == "boom")
    assert(ex eq cause, "the same Throwable instance should propagate through")
  }

  it should "preserve a null value on the value path (Twitter Return wraps any AnyRef)" in {
    // The conversion calls `promise.success(value)` directly; for a `null`
    // value, the Scala promise resolves to `null`. Pin that the wire does
    // not coerce null into an exception.
    val tf = TwitterFuture.value[String](null)
    val result = ScalaAwait.result(tf.asScala, timeout)
    assert(result == null)
  }

  it should "preserve the value type (compile-time enforced)" in {
    val tf: TwitterFuture[String] = TwitterFuture.value("hello")
    val sf: ScalaFuture[String] = tf.asScala
    assert(ScalaAwait.result(sf, timeout) == "hello")
  }

  it should "produce a future that has already completed when the source TwitterFuture is already resolved" in {
    val tf = TwitterFuture.value(7)
    val sf = tf.asScala
    // The conversion uses `respond` which fires synchronously for already-
    // resolved futures, so the scala future is complete by the time the
    // implicit returns.
    assert(sf.isCompleted, "the converted future should be completed immediately")
  }

  // ---------------------------------------------------------------------------
  // Scala Future → Twitter Future
  // ---------------------------------------------------------------------------

  "RichScalaFuture.asTwitter" should "resolve to the same value as the wrapped ScalaFuture" in {
    val sf = ScalaFuture.successful(42)
    val tf = sf.asTwitter()
    assert(TwitterAwait.result(tf, twitterTimeout) == 42)
  }

  it should "preserve the exception type and message on the failure path" in {
    val cause = new IllegalArgumentException("nope")
    val sf = ScalaFuture.failed[Int](cause)
    val ex = intercept[IllegalArgumentException] {
      TwitterAwait.result(sf.asTwitter(), twitterTimeout)
    }
    assert(ex.getMessage == "nope")
    assert(ex eq cause, "the same Throwable instance should propagate through")
  }

  it should "preserve a null value on the value path" in {
    val sf = ScalaFuture.successful[String](null)
    val tf = sf.asTwitter()
    assert(TwitterAwait.result(tf, twitterTimeout) == null)
  }

  it should "preserve the value type (compile-time enforced)" in {
    val sf: ScalaFuture[String] = ScalaFuture.successful("hello")
    val tf: TwitterFuture[String] = sf.asTwitter()
    assert(TwitterAwait.result(tf, twitterTimeout) == "hello")
  }

  // ---------------------------------------------------------------------------
  // Round-trip — Twitter → Scala → Twitter and vice versa
  // ---------------------------------------------------------------------------

  "FutureBijection" should "round-trip a value through Twitter → Scala → Twitter" in {
    val tf1 = TwitterFuture.value("payload")
    val tf2: TwitterFuture[String] = tf1.asScala.asTwitter()
    assert(TwitterAwait.result(tf2, twitterTimeout) == "payload")
  }

  it should "round-trip a value through Scala → Twitter → Scala" in {
    val sf1 = ScalaFuture.successful("payload")
    val sf2: ScalaFuture[String] = sf1.asTwitter().asScala
    assert(ScalaAwait.result(sf2, timeout) == "payload")
  }

  it should "round-trip an exception through Twitter → Scala → Twitter (type + message preserved)" in {
    val cause = new RuntimeException("round-trip-err")
    val tf1 = TwitterFuture.exception[Int](cause)
    val tf2: TwitterFuture[Int] = tf1.asScala.asTwitter()
    val ex = intercept[RuntimeException] {
      TwitterAwait.result(tf2, twitterTimeout)
    }
    assert(ex.getMessage == "round-trip-err")
    assert(ex eq cause)
  }
}
