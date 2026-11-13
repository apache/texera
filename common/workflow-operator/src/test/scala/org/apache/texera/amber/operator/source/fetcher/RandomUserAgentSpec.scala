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
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

/**
  * `RandomUserAgent` picks a browser by weighted draw over a private static frequency
  * table and then picks one of that browser's user-agent strings at random.
  *
  * The weights are hard-coded and sum to 98.6, not 100, while the draw is over
  * `[0, 100)`. Roughly 1.4% of calls therefore exhaust the loop without any bucket
  * claiming them, leaving `browser` null and taking the `"Chrome"` fallback. Waiting
  * for that tail to show up on its own would make the suite flaky and slow, so the
  * tests below swap the private static `freqMap` for a table they control and restore
  * the original in a `finally`. That is a process-wide mutation, which is safe here
  * only because this module runs its suites strictly serially
  * (`Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)` in build.sbt) and
  * because the swap never outlives a single test.
  *
  * Deliberately NOT covered: the class's implicit default constructor. `RandomUserAgent`
  * is a static-only utility that nothing instantiates, so `new RandomUserAgent()` would
  * assert nothing about its behaviour.
  */
class RandomUserAgentSpec extends AnyFlatSpec with Matchers {

  private def declaredField(name: String): java.lang.reflect.Field = {
    val field = classOf[RandomUserAgent].getDeclaredField(name)
    field.setAccessible(true)
    field
  }

  private def uaMap: java.util.Map[String, Array[String]] =
    declaredField("uaMap").get(null).asInstanceOf[java.util.Map[String, Array[String]]]

  private def freqMap: java.util.Map[String, java.lang.Double] =
    declaredField("freqMap").get(null).asInstanceOf[java.util.Map[String, java.lang.Double]]

  /**
    * Runs `body` with the private static frequency table replaced by `weights`, then
    * restores the original table. Restoring matters: `URLFetchUtil` draws from the same
    * static table on every fetch, so a leaked replacement would starve every later suite
    * in this JVM.
    */
  private def withFreqTable[T](weights: (String, Double)*)(body: => T): T = {
    val field = declaredField("freqMap")
    val original = field.get(null)
    val replacement = new java.util.HashMap[String, java.lang.Double]()
    weights.foreach {
      case (browser, weight) =>
        replacement.put(browser, java.lang.Double.valueOf(weight))
    }
    field.set(null, replacement)
    try body
    finally field.set(null, original)
  }

  "RandomUserAgent.getRandomUserAgent" should
    "fall back to a Chrome agent when no frequency bucket claims the draw" in {
    // `Math.random() * 100` lands in [0, 100) and the only bucket contributes -1.0, so
    // `rand <= count` is false for every draw: the loop runs to exhaustion, `browser`
    // stays null, and the fallback has to supply a browser that actually has agents.
    // This forces the fallback code path deterministically (the production fallback occurs ~1.4% of the time because weights sum to 98.6 < 100).
    val chromeAgents = uaMap.get("Chrome").toSet
    chromeAgents should not be empty

    withFreqTable("Firefox" -> -1.0) {
      (1 to 50).foreach { _ =>
        chromeAgents should contain(RandomUserAgent.getRandomUserAgent)
      }
    }
  }

  it should "draw from the bucket the frequency table selects" in {
    // A single bucket weighted above the draw range claims every draw, so each browser's
    // routing through `uaMap` is pinned deterministically rather than sampled.
    uaMap.keySet.asScala.toSeq.foreach { browser =>
      val agents = uaMap.get(browser).toSet
      withFreqTable(browser -> 200.0) {
        val drawn = (1 to 20).map(_ => RandomUserAgent.getRandomUserAgent).toSet
        withClue(s"draw for $browser strayed outside its bucket: ") {
          drawn.subsetOf(agents) shouldBe true
        }
        // Membership alone would also hold for a class that always returned
        // `userAgents[0]` -- which is the one thing a *random* user agent must not do,
        // since the point of the class is that a scraped host does not see the same
        // header on every request. The smallest bucket holds 173 strings, so 20 draws
        // collapsing onto one value has probability (1/173)^19: this is sampled rather
        // than deterministic, but the flake window is not physically reachable.
        withClue(s"$browser always returned the same agent: ") {
          drawn.size should be > 1
        }
      }
    }
  }

  it should "return agents from more than one bucket across a weighted table" in {
    // Two buckets that split the whole draw range: neither the fallback nor a single
    // bucket can satisfy this, so it pins that the weighted walk really advances past
    // the first entry instead of always claiming the draw with it.
    val firefox = uaMap.get("Firefox").toSet
    val opera = uaMap.get("Opera").toSet
    firefox.intersect(opera) shouldBe empty

    withFreqTable("Firefox" -> 50.0, "Opera" -> 50.0) {
      val drawn = (1 to 400).map(_ => RandomUserAgent.getRandomUserAgent).toSet
      drawn.foreach(agent => (firefox ++ opera) should contain(agent))
      drawn.exists(firefox.contains) shouldBe true
      drawn.exists(opera.contains) shouldBe true
    }
  }

  "RandomUserAgent" should "declare a non-empty user-agent bucket for every browser it can draw" in {
    // `getRandomUserAgent` dereferences `uaMap.get(browser)` without a null check, so a
    // browser carrying a frequency weight but no agent bucket is an NPE waiting for the
    // right draw. Same for the "Chrome" fallback, which no frequency weight guards.
    (freqMap.keySet.asScala.toSeq :+ "Chrome").foreach { browser =>
      withClue(s"$browser can be drawn but has no user-agent bucket: ") {
        uaMap.get(browser) should not be null
      }
      withClue(s"$browser has an empty user-agent bucket: ") {
        uaMap.get(browser).length should be > 0
      }
    }
  }

  it should "stock each browser's bucket with that browser's own agents" in {
    // The routing tests above read their expected set out of `uaMap` itself, so they
    // only ever prove routing is self-consistent: swap two buckets' keys and they all
    // still pass, while every request the frequency table meant to look like Firefox
    // goes out advertising Opera. This one compares against a token that is NOT derived
    // from the map. A strict majority rather than "all", because a handful of entries in
    // each bucket are historical strings that omit the product token (e.g. 1 of the 424
    // Firefox agents, 13 of the 213 Safari ones).
    Map(
      "Internet Explorer" -> "MSIE",
      "Firefox" -> "Firefox",
      "Chrome" -> "Chrome",
      "Safari" -> "Safari",
      "Opera" -> "Opera"
    ).foreach {
      case (browser, token) =>
        val bucket = uaMap.get(browser)
        withClue(s"$browser's bucket does not look like $browser agents: ") {
          bucket.count(_.contains(token)) * 2 should be > bucket.length
        }
    }
  }
}
