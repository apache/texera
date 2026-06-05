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

package org.apache.texera.web.observability.gateway

import org.apache.texera.web.observability.gateway.dtos._
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ParcaClientSpec extends AnyFlatSpec with Matchers with OptionValues {

  // ----- ParcaSummary.toProfilesResponse ------------------------------

  "ParcaSummary.toProfilesResponse" should "return empty for a zero-series summary" in {
    val s = ParcaQueryRangeSummary(seriesCount = 0, totalSampleCount = 0L, series = Seq.empty)
    val r = ParcaSummary.toProfilesResponse(s)
    r.root shouldBe None
    r.totalSamples shouldBe 0L
  }

  it should "return empty when series exist but no samples were observed" in {
    val s = ParcaQueryRangeSummary(
      seriesCount = 2,
      totalSampleCount = 0L,
      series = Seq(ParcaSeriesSummary("a=1", 0L, 0L), ParcaSeriesSummary("a=2", 0L, 0L))
    )
    ParcaSummary.toProfilesResponse(s).root shouldBe None
  }

  it should "render one child per series under a labelled root" in {
    val s = ParcaQueryRangeSummary(
      seriesCount = 2,
      totalSampleCount = 42L,
      series = Seq(
        ParcaSeriesSummary("node=texera-dev", 30L, 120L),
        ParcaSeriesSummary("node=ci", 12L, 50L)
      )
    )
    val r = ParcaSummary.toProfilesResponse(s)
    r.totalSamples shouldBe 42L
    val root = r.root.value
    root.value shouldBe 42L
    root.name should include("parca_agent")
    root.name should include("2 series")
    root.children should have size 2
    root.children.head.name shouldBe "node=texera-dev"
    root.children.head.value shouldBe 30L
    root.children(1).name shouldBe "node=ci"
  }

  // ----- Protobuf round-trip (encode then decode our own output) -----

  "ProtoEncode + Proto" should "round-trip a QueryRangeRequest through the wire format" in {
    // The encoder is private[gateway], but reachable from the same
    // package. The reader walks top-level fields.
    val payload = TestAccess.encodeQueryRange("parca_agent:samples:count:cpu:nanoseconds:delta",
                                              startMs = 1_700_000_000_000L,
                                              endMs = 1_700_000_600_000L,
                                              limit = 100)

    val collected = scala.collection.mutable.Map.empty[Int, Any]
    TestAccess.walk(payload) {
      case (1, TestAccess.LD(b)) => collected += 1 -> new String(b, "UTF-8")
      case (2, TestAccess.LD(b)) =>
        // Timestamp{seconds=1, nanos=2}
        var seconds = -1L
        TestAccess.walk(b) {
          case (1, TestAccess.V(v)) => seconds = v
          case _                    => ()
        }
        collected += 2 -> seconds
      case (3, TestAccess.LD(b)) =>
        var seconds = -1L
        TestAccess.walk(b) {
          case (1, TestAccess.V(v)) => seconds = v
          case _                    => ()
        }
        collected += 3 -> seconds
      case (4, TestAccess.V(v)) => collected += 4 -> v
      case _                    => ()
    }

    collected(1) shouldBe "parca_agent:samples:count:cpu:nanoseconds:delta"
    collected(2) shouldBe 1_700_000_000L // seconds
    collected(3) shouldBe 1_700_000_600L
    collected(4) shouldBe 100L
  }

  it should "encode multi-byte varints correctly (>= 128)" in {
    // Field 4 (limit) = 300 should encode as: tag=0x20, then varint 300 = 0xac 0x02
    val payload = TestAccess.encodeQueryRange("x", 0L, 0L, 300)
    payload should not be empty
    // Walk to confirm we get 300 back.
    var got = -1L
    TestAccess.walk(payload) {
      case (4, TestAccess.V(v)) => got = v
      case _                    => ()
    }
    got shouldBe 300L
  }

  it should "split a sub-second epoch into seconds + nanos in the Timestamp message" in {
    // 1_700_000_000_456L ms → seconds=1_700_000_000, nanos=456_000_000.
    val payload = TestAccess.encodeQueryRange("q", 1_700_000_000_456L, 1_700_000_001_789L, 1)
    var startSec = -1L; var startNanos = -1L
    var endSec = -1L; var endNanos = -1L
    TestAccess.walk(payload) {
      case (2, TestAccess.LD(b)) =>
        TestAccess.walk(b) {
          case (1, TestAccess.V(v)) => startSec = v
          case (2, TestAccess.V(v)) => startNanos = v
          case _                    => ()
        }
      case (3, TestAccess.LD(b)) =>
        TestAccess.walk(b) {
          case (1, TestAccess.V(v)) => endSec = v
          case (2, TestAccess.V(v)) => endNanos = v
          case _                    => ()
        }
      case _ => ()
    }
    startSec shouldBe 1_700_000_000L
    startNanos shouldBe 456_000_000L
    endSec shouldBe 1_700_000_001L
    endNanos shouldBe 789_000_000L
  }

  it should "omit the nanos sub-field entirely when timestamp is an exact second" in {
    // 1_700_000_000_000L → seconds = 1_700_000_000, nanos = 0 → skipped.
    val payload = TestAccess.encodeQueryRange("q", 1_700_000_000_000L, 1_700_000_000_000L, 1)
    var sawNanos = false
    TestAccess.walk(payload) {
      case (2, TestAccess.LD(b)) =>
        TestAccess.walk(b) {
          case (2, _) => sawNanos = true
          case _      => ()
        }
      case _ => ()
    }
    sawNanos shouldBe false
  }
}

/** Package-private hooks so the spec can poke at the private encoder/decoder
 *  without exposing them to the rest of the codebase. */
private[gateway] object TestAccess {
  type V = Proto.VarInt
  val V = Proto.VarInt
  type LD = Proto.LengthDelimited
  val LD = Proto.LengthDelimited

  def encodeQueryRange(query: String, startMs: Long, endMs: Long, limit: Int): Array[Byte] =
    ProtoEncode.queryRangeRequest(query, startMs, endMs, limit)

  def walk(bytes: Array[Byte])(f: PartialFunction[(Int, Proto.Value), Unit]): Unit =
    Proto.foreachField(bytes)(f)
}
