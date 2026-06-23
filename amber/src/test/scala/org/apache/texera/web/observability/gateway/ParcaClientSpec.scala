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

import java.io.ByteArrayOutputStream
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ParcaClientSpec extends AnyFlatSpec with Matchers {

  // ----- ProtoEncode round-trips --------------------------------------

  "ProtoEncode.queryMergeRequest" should "encode mode, merge{query,start,end}, report_type" in {
    val payload = TestAccess.encodeQueryMerge(
      "parca_agent:samples:count:cpu:nanoseconds:delta",
      startMs = 1_700_000_000_000L,
      endMs = 1_700_000_600_000L,
      mode = 2L,
      reportType = 2L
    )
    var mode = -1L
    var reportType = -1L
    var query = ""
    var startSec = -1L
    var endSec = -1L
    TestAccess.walk(payload) {
      case (1, TestAccess.V(v)) => mode = v
      case (5, TestAccess.V(v)) => reportType = v
      case (3, TestAccess.LD(b)) =>
        TestAccess.walk(b) {
          case (1, TestAccess.LD(qb)) => query = new String(qb, "UTF-8")
          case (2, TestAccess.LD(ts)) =>
            TestAccess.walk(ts) { case (1, TestAccess.V(v)) => startSec = v }
          case (3, TestAccess.LD(ts)) =>
            TestAccess.walk(ts) { case (1, TestAccess.V(v)) => endSec = v }
        }
    }
    mode shouldBe 2L // MODE_MERGE
    reportType shouldBe 2L // REPORT_TYPE_TOP
    query shouldBe "parca_agent:samples:count:cpu:nanoseconds:delta"
    startSec shouldBe 1_700_000_000L
    endSec shouldBe 1_700_000_600L
  }

  "ProtoEncode.queryRangeRequest" should "encode query, start, end, limit, step" in {
    val payload = TestAccess.encodeQueryRange("q", 1_700_000_000_000L, 1_700_000_600_000L, 500, 30L)
    var query = ""
    var startSec = -1L
    var limit = -1L
    var stepSec = -1L
    TestAccess.walk(payload) {
      case (1, TestAccess.LD(b))  => query = new String(b, "UTF-8")
      case (2, TestAccess.LD(ts)) => TestAccess.walk(ts) { case (1, TestAccess.V(v)) => startSec = v }
      case (4, TestAccess.V(v))   => limit = v
      case (5, TestAccess.LD(d))  => TestAccess.walk(d) { case (1, TestAccess.V(v)) => stepSec = v }
    }
    query shouldBe "q"
    startSec shouldBe 1_700_000_000L
    limit shouldBe 500L
    stepSec shouldBe 30L
  }

  // ----- parseTop: Parca TOP report -> ranked self-CPU table ----------

  "ParcaClient.parseTop" should "rank by flat, bucket unsymbolized, sum total" in {
    val body = TopFixture.framed(
      Seq(
        Some("foo") -> 100L,
        None -> 50L, // unsymbolized
        None -> 30L, // unsymbolized
        Some("bar") -> 20L
      )
    )
    val Right((total, entries)) = ParcaClient.parseTop(body, 25)
    total shouldBe 200L
    entries.map(_.name) shouldBe Seq("foo", "(unsymbolized)", "bar")
    entries.map(_.flat) shouldBe Seq(100L, 80L, 20L) // two unsymbolized merge to 80
  }

  it should "cap the number of returned entries" in {
    val nodes = (1 to 30).map(i => Some(s"fn$i") -> i.toLong)
    val Right((_, entries)) = ParcaClient.parseTop(TopFixture.framed(nodes), 5)
    entries should have size 5
    entries.head.name shouldBe "fn30" // highest flat first
  }

  it should "return empty for a response with no top nodes" in {
    val Right((total, entries)) = ParcaClient.parseTop(TopFixture.framed(Seq.empty), 25)
    total shouldBe 0L
    entries shouldBe empty
  }
}

/** Builds a gRPC-Web-framed QueryResponse carrying a TOP report, for parseTop.
  *  QueryResponse{ top=7: Top{ list=1: repeated TopNode{ meta=1:
  *  TopNodeMeta{ function=3: Function{ name=3 } }, flat=3 } } }.
  */
private[gateway] object TopFixture {
  private def varint(n: Long): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    var v = n
    while ((v & ~0x7fL) != 0) { out.write(((v & 0x7f) | 0x80).toInt); v >>>= 7 }
    out.write((v & 0x7f).toInt)
    out.toByteArray
  }
  private def tag(field: Int, wire: Int): Array[Byte] = varint((field.toLong << 3) | wire.toLong)
  private def ld(field: Int, payload: Array[Byte]): Array[Byte] =
    tag(field, 2) ++ varint(payload.length.toLong) ++ payload
  private def vf(field: Int, v: Long): Array[Byte] = tag(field, 0) ++ varint(v)
  private def str(field: Int, s: String): Array[Byte] = ld(field, s.getBytes("UTF-8"))

  /** Build the framed response. Each node is (optional function name, flat). */
  def framed(nodes: Seq[(Option[String], Long)]): Array[Byte] = {
    val topBody = nodes.foldLeft(Array.emptyByteArray) { case (acc, (name, flat)) =>
      val meta = name.map(n => ld(3 /*function*/, str(3 /*name*/, n))).getOrElse(Array.emptyByteArray)
      val node = ld(1 /*meta*/, meta) ++ vf(3 /*flat*/, flat)
      acc ++ ld(1 /*list*/, node)
    }
    val queryResponse = ld(7 /*top*/, topBody)
    // gRPC-Web frame: flag byte + 4-byte big-endian length + payload.
    val len = queryResponse.length
    Array[Byte](0) ++ Array[Byte](
      ((len >> 24) & 0xff).toByte,
      ((len >> 16) & 0xff).toByte,
      ((len >> 8) & 0xff).toByte,
      (len & 0xff).toByte
    ) ++ queryResponse
  }
}

/** Package-private hooks so the spec can poke at the private encoders without
  *  exposing them to the rest of the codebase.
  */
private[gateway] object TestAccess {
  type V = Proto.VarInt
  val V = Proto.VarInt
  type LD = Proto.LengthDelimited
  val LD = Proto.LengthDelimited

  def encodeQueryMerge(
      query: String,
      startMs: Long,
      endMs: Long,
      mode: Long,
      reportType: Long
  ): Array[Byte] =
    ProtoEncode.queryMergeRequest(query, startMs, endMs, mode, reportType)

  def encodeQueryRange(
      query: String,
      startMs: Long,
      endMs: Long,
      limit: Int,
      stepSeconds: Long
  ): Array[Byte] =
    ProtoEncode.queryRangeRequest(query, startMs, endMs, limit, stepSeconds)

  def walk(bytes: Array[Byte])(f: PartialFunction[(Int, Proto.Value), Unit]): Unit =
    Proto.foreachField(bytes)(f)
}
