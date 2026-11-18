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

import scala.concurrent.{Future => ScalaFuture}

class FutureBijectionSpec extends AnyFlatSpec {

  private val twitterTimeout: com.twitter.util.Duration = com.twitter.util.Duration.fromSeconds(5)

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
}
