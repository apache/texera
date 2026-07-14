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

import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.web.observability.gateway.dtos._

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.http.HttpResponse.BodyHandlers
import java.nio.{ByteBuffer, ByteOrder}
import java.time.Duration
import scala.util.{Failure, Success, Try}

/**
  * Minimal gRPC-Web client for Parca's `QueryService`.
  *
  * Parca v0.28 ships only a gRPC API on :7070 — no JSON/REST gateway.
  * Reaching it from a HTTP/1.1 Java client (no HTTP/2) means using the
  * gRPC-Web framing over `application/grpc-web+proto`. The outbound
  * QueryRequest is a small hand-coded protobuf payload; responses are walked
  * at the wire level with [[Proto]]. `queryTop` asks for the TOP report
  * (server-side aggregation, a few KB) and `queryTimeline` for a QueryRange
  * (CPU over time). Both are far lighter than fetching the full pprof, which
  * for unsymbolized JVMs runs to tens of MB.
  *
  * Wire constants (field numbers, enum ordinals) are taken from
  * parca-dev/parca query.proto and metastore.proto at v0.28.0. Java/JIT
  * frames are often unsymbolized and roll up under "(unsymbolized)" until
  * JVM symbolization is added to the agent.
  */
object ParcaClient extends LazyLogging {

  /** Verified wire constants for `query.v1alpha1` (parca-dev/parca @ v0.28.0).
    *  QueryRequest: mode=1, options.merge=3, report_type=5.
    *  Mode.MODE_MERGE=2; ReportType.REPORT_TYPE_TOP=2.
    *  QueryResponse: top = field 7.
    */
  private val ModeMerge: Long = 2L
  private val ReportTypeTop: Long = 2L
  private val TopResponseField: Int = 7
  private val Unsymbolized: String = "(unsymbolized)"
  private val QueryPath = "/parca.query.v1alpha1.QueryService/Query"
  private val QueryRangePath = "/parca.query.v1alpha1.QueryService/QueryRange"

  private lazy val http: HttpClient = HttpClient
    .newBuilder()
    .connectTimeout(Duration.ofSeconds(10))
    .followRedirects(HttpClient.Redirect.NEVER)
    .build()

  /** Top functions over [startMs, endMs] for the selector, plus total CPU.
    *  Asks Parca for its server-side TOP report (already aggregated by
    *  function) rather than the full pprof: the response is a few KB instead of
    *  tens of MB, so it never hits the response-size cap. The merge is the heavy
    *  step; a wide/unfiltered selector can take several seconds, hence the
    *  generous timeout. Ranked by `flat` (self CPU), which sums to the total;
    *  cumulative is intentionally not summed across the unsymbolized bucket
    *  (that would double-count).
    */
  def queryTop(
      baseUrl: String,
      profileQuery: String,
      startMs: Long,
      endMs: Long,
      maxEntries: Int = 25,
      timeoutMs: Long = 30000L
  ): Either[GatewayError, (Long, Seq[ProfileTopEntry])] = {
    val payload =
      ProtoEncode.queryMergeRequest(profileQuery, startMs, endMs, ModeMerge, ReportTypeTop)
    postGrpcWeb(baseUrl, QueryPath, payload, timeoutMs)
      .flatMap(b => parseTop(b, maxEntries))
  }

  /** CPU-over-time points for the selector. Fast (a metrics range query, no
    *  profile merge); values are summed across series into one line.
    */
  def queryTimeline(
      baseUrl: String,
      profileQuery: String,
      startMs: Long,
      endMs: Long,
      limit: Int = 1000,
      timeoutMs: Long = 8000L
  ): Either[GatewayError, Seq[ProfileTimelinePoint]] = {
    // `step` is the bucket width: without it Parca returns a single aggregate
    // point. Aim for ~120 buckets across the window (min 1s) for a smooth line.
    val windowSec = math.max((endMs - startMs) / 1000L, 1L)
    val stepSec = math.max(windowSec / 120L, 1L)
    val payload = ProtoEncode.queryRangeRequest(profileQuery, startMs, endMs, limit, stepSec)
    postGrpcWeb(baseUrl, QueryRangePath, payload, timeoutMs)
      .flatMap(parseTimeline)
  }

  // ---- HTTP plumbing -----------------------------------------------------

  private def postGrpcWeb(
      baseUrl: String,
      path: String,
      payload: Array[Byte],
      timeoutMs: Long
  ): Either[GatewayError, Array[Byte]] = {
    val framed = grpcWebFrame(payload)
    val req = HttpRequest
      .newBuilder(URI.create(baseUrl + path))
      .timeout(Duration.ofMillis(timeoutMs))
      .header("Content-Type", "application/grpc-web+proto")
      .header("Accept", "application/grpc-web+proto")
      .POST(HttpRequest.BodyPublishers.ofByteArray(framed))
      .build()
    logger.debug(
      s"[profiles] sending gRPC-Web POST $baseUrl$path (${framed.length} bytes, timeout ${timeoutMs}ms)"
    )
    Try(http.send(req, BodyHandlers.ofByteArray())) match {
      case Failure(e) =>
        logger.warn(
          s"[profiles] Parca backend unreachable at $baseUrl: " +
            s"${e.getClass.getSimpleName}: ${e.getMessage}"
        )
        Left(GatewayError.BackendUnreachable("profiles"))
      case Success(resp: HttpResponse[Array[Byte]]) =>
        // gRPC over HTTP always returns 200; the real status lives in
        // the Grpc-Status header (or the trailer block in the body).
        val grpcStatus = headerInt(resp, "grpc-status").orElse(headerInt(resp, "Grpc-Status"))
        grpcStatus match {
          case Some(0) | None =>
            val body = Option(resp.body()).getOrElse(Array.emptyByteArray)
            logger.debug(s"[profiles] Parca QueryRange ok (${body.length} bytes)")
            Right(body)
          case Some(other) =>
            val msg = headerStr(resp, "grpc-message")
              .orElse(headerStr(resp, "Grpc-Message"))
              .getOrElse("")
            logger.warn(s"[profiles] Parca returned gRPC status=$other ${msg.take(120)}")
            Left(
              GatewayError(
                "backend_error",
                s"profiles backend gRPC status=$other ${msg.take(120)}",
                502
              )
            )
        }
    }
  }

  private def grpcWebFrame(payload: Array[Byte]): Array[Byte] = {
    // 5-byte prefix: 1 byte flags (0=uncompressed) + 4-byte length big-endian.
    val out = new Array[Byte](5 + payload.length)
    out(0) = 0
    val lenBuf = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(payload.length).array()
    System.arraycopy(lenBuf, 0, out, 1, 4)
    System.arraycopy(payload, 0, out, 5, payload.length)
    out
  }

  private def headerInt(resp: HttpResponse[_], key: String): Option[Int] =
    headerStr(resp, key).flatMap(s => Try(s.trim.toInt).toOption)

  private def headerStr(resp: HttpResponse[_], key: String): Option[String] = {
    val list = resp.headers().allValues(key)
    if (list.isEmpty) None else Option(list.get(0))
  }

  // ---- Response parsing --------------------------------------------------

  /** QueryResponse with report=PPROF carries a gzipped pprof profile in field
    *  6 (bytes). Extract it, decompress with a hard cap, and parse with ScalaPB.
    */
  /** QueryResponse.top (field 7) is a `Top{ list = field 1: repeated TopNode }`.
    *  Each `TopNode{ meta = 1, cumulative = 2, flat = 3 }`; `meta.function`
    *  (field 3) is a metastore `Function{ name = field 3, string }`. We rank by
    *  `flat` (self CPU, which sums to the total) and bucket frames with no
    *  resolved function name under a single "(unsymbolized)" entry.
    */
  private[gateway] def parseTop(
      body: Array[Byte],
      maxEntries: Int
  ): Either[GatewayError, (Long, Seq[ProfileTopEntry])] = {
    stripGrpcWebFrame(body).map { unframed =>
      val flat = scala.collection.mutable.LinkedHashMap.empty[String, Long]
      var total = 0L
      Proto.foreachField(unframed) {
        case (TopResponseField, Proto.LengthDelimited(top)) =>
          Proto.foreachField(top) {
            case (1, Proto.LengthDelimited(node)) =>
              var meta: Option[Array[Byte]] = None
              var fl = 0L
              Proto.foreachField(node) {
                case (1, Proto.LengthDelimited(m)) => meta = Some(m)
                case (3, Proto.VarInt(v))          => fl = v
                case _                             => ()
              }
              val name = meta.flatMap(functionName).getOrElse(Unsymbolized)
              flat.update(name, flat.getOrElse(name, 0L) + fl)
              total += fl
            case _ => ()
          }
        case _ => ()
      }
      val entries = flat.iterator
        .map { case (n, f) => ProfileTopEntry(n, f) }
        .toSeq
        .sortBy(e => -e.flat)
        .take(maxEntries)
      (total, entries)
    }
  }

  /** TopNodeMeta{ function = field 3 } -> Function{ name = field 3, string }. */
  private def functionName(meta: Array[Byte]): Option[String] = {
    var name: Option[String] = None
    Proto.foreachField(meta) {
      case (3, Proto.LengthDelimited(fn)) =>
        Proto.foreachField(fn) {
          case (3, Proto.LengthDelimited(s)) =>
            val str = new String(s, "UTF-8")
            if (str.nonEmpty) name = Some(str)
          case _ => ()
        }
      case _ => ()
    }
    name
  }

  /** QueryRangeResponse: field 1 repeated MetricsSeries{ samples = field 2 }.
    *  Each MetricsSample has timestamp (field 1, Timestamp) and value (field 2,
    *  varint). We sum value per timestamp across all series into one CPU line.
    */
  private def parseTimeline(body: Array[Byte]): Either[GatewayError, Seq[ProfileTimelinePoint]] = {
    stripGrpcWebFrame(body).map { unframed =>
      val byTs = scala.collection.mutable.LinkedHashMap.empty[Long, Long]
      Proto.foreachField(unframed) {
        case (1, Proto.LengthDelimited(series)) =>
          Proto.foreachField(series) {
            case (2, Proto.LengthDelimited(sample)) =>
              var tsMs = 0L
              var value = 0L
              Proto.foreachField(sample) {
                case (1, Proto.LengthDelimited(ts)) => tsMs = timestampMs(ts)
                case (2, Proto.VarInt(v))           => value = v
                case _                              => ()
              }
              byTs.update(tsMs, byTs.getOrElse(tsMs, 0L) + value)
            case _ => ()
          }
        case _ => ()
      }
      byTs.toSeq.sortBy(_._1).map { case (t, v) => ProfileTimelinePoint(t, v) }
    }
  }

  /** google.protobuf.Timestamp{ seconds (1), nanos (2) } -> epoch millis. */
  private def timestampMs(ts: Array[Byte]): Long = {
    var seconds = 0L
    var nanos = 0L
    Proto.foreachField(ts) {
      case (1, Proto.VarInt(v)) => seconds = v
      case (2, Proto.VarInt(v)) => nanos = v
      case _                    => ()
    }
    seconds * 1000L + nanos / 1_000_000L
  }

  private def stripGrpcWebFrame(body: Array[Byte]): Either[GatewayError, Array[Byte]] = {
    if (body.length < 5) Right(Array.emptyByteArray)
    else {
      val len = ByteBuffer.wrap(body, 1, 4).order(ByteOrder.BIG_ENDIAN).getInt
      if (len < 0 || 5 + len > body.length)
        Left(
          GatewayError(
            "bad_backend_response",
            "malformed gRPC-Web frame from profiles backend",
            502
          )
        )
      else Right(java.util.Arrays.copyOfRange(body, 5, 5 + len))
    }
  }
}

/** Tiny protobuf-wire-format encoder used for outbound requests. */
private[gateway] object ProtoEncode {

  /** Parca QueryRequest for a merged report (report type chosen by caller):
    *   1 (varint)  mode        = MODE_MERGE
    *   3 (message) merge       = MergeProfile{ query(1), start(2), end(3) }
    *   5 (varint)  report_type = caller's value (e.g. REPORT_TYPE_TOP)
    */
  def queryMergeRequest(
      query: String,
      startMs: Long,
      endMs: Long,
      mode: Long,
      reportType: Long
  ): Array[Byte] = {
    val merge = new java.io.ByteArrayOutputStream()
    writeString(merge, fieldNumber = 1, query)
    writeMessage(merge, fieldNumber = 2, timestamp(startMs))
    writeMessage(merge, fieldNumber = 3, timestamp(endMs))
    val out = new java.io.ByteArrayOutputStream()
    writeVarintField(out, fieldNumber = 1, mode)
    writeMessage(out, fieldNumber = 3, merge.toByteArray)
    writeVarintField(out, fieldNumber = 5, reportType)
    out.toByteArray
  }

  /** Parca QueryRangeRequest{ query(1), start(2), end(3), limit(4), step(5) },
    *  used for the CPU-over-time timeline. `step` (a Duration) is the bucket
    *  width; omitting it makes Parca return a single aggregate point.
    */
  def queryRangeRequest(
      query: String,
      startMs: Long,
      endMs: Long,
      limit: Int,
      stepSeconds: Long
  ): Array[Byte] = {
    val out = new java.io.ByteArrayOutputStream()
    writeString(out, fieldNumber = 1, query)
    writeMessage(out, fieldNumber = 2, timestamp(startMs))
    writeMessage(out, fieldNumber = 3, timestamp(endMs))
    writeVarintField(out, fieldNumber = 4, limit.toLong)
    if (stepSeconds > 0L) writeMessage(out, fieldNumber = 5, durationSecs(stepSeconds))
    out.toByteArray
  }

  /** google.protobuf.Duration{ seconds (1), nanos (2) }; whole seconds only. */
  private def durationSecs(seconds: Long): Array[Byte] = {
    val out = new java.io.ByteArrayOutputStream()
    writeVarintField(out, fieldNumber = 1, seconds)
    out.toByteArray
  }

  /** google.protobuf.Timestamp{ seconds (1), nanos (2) }. */
  private def timestamp(epochMs: Long): Array[Byte] = {
    val seconds = epochMs / 1000L
    val nanos = ((epochMs % 1000L) * 1_000_000L).toInt
    val out = new java.io.ByteArrayOutputStream()
    writeVarintField(out, fieldNumber = 1, seconds)
    if (nanos != 0) writeVarintField(out, fieldNumber = 2, nanos.toLong)
    out.toByteArray
  }

  private def writeVarintField(
      out: java.io.ByteArrayOutputStream,
      fieldNumber: Int,
      value: Long
  ): Unit = {
    writeVarint(out, ((fieldNumber.toLong) << 3) | 0L)
    writeVarint(out, value)
  }

  private def writeString(
      out: java.io.ByteArrayOutputStream,
      fieldNumber: Int,
      value: String
  ): Unit = {
    val bytes = value.getBytes("UTF-8")
    writeVarint(out, ((fieldNumber.toLong) << 3) | 2L)
    writeVarint(out, bytes.length.toLong)
    out.write(bytes)
  }

  private def writeMessage(
      out: java.io.ByteArrayOutputStream,
      fieldNumber: Int,
      value: Array[Byte]
  ): Unit = {
    writeVarint(out, ((fieldNumber.toLong) << 3) | 2L)
    writeVarint(out, value.length.toLong)
    out.write(value)
  }

  private def writeVarint(out: java.io.ByteArrayOutputStream, value: Long): Unit = {
    var v = value
    while ((v & ~0x7fL) != 0) {
      out.write(((v & 0x7f) | 0x80).toInt)
      v >>>= 7
    }
    out.write((v & 0x7f).toInt)
  }
}

/** Tiny protobuf-wire-format reader. Just walks top-level fields and
  *  yields (field_number, value) pairs. Does not skip group fields
  *  (deprecated, not used by Parca).
  */
private[gateway] object Proto {

  sealed trait Value
  case class VarInt(value: Long) extends Value
  case class LengthDelimited(bytes: Array[Byte]) extends Value
  case class Fixed64(bytes: Array[Byte]) extends Value
  case class Fixed32(bytes: Array[Byte]) extends Value

  def foreachField(bytes: Array[Byte])(f: PartialFunction[(Int, Value), Unit]): Unit = {
    var i = 0
    while (i < bytes.length) {
      val (tag, next) = readVarint(bytes, i)
      i = next
      val field = (tag >>> 3).toInt
      val wireType = (tag & 7).toInt
      val value: Value = wireType match {
        case 0 =>
          val (v, ni) = readVarint(bytes, i); i = ni; VarInt(v)
        case 2 =>
          val (len, ni) = readVarint(bytes, i); i = ni
          if (len < 0 || i + len > bytes.length) return
          val payload = java.util.Arrays.copyOfRange(bytes, i, i + len.toInt)
          i += len.toInt
          LengthDelimited(payload)
        case 1 =>
          if (i + 8 > bytes.length) return
          val payload = java.util.Arrays.copyOfRange(bytes, i, i + 8); i += 8; Fixed64(payload)
        case 5 =>
          if (i + 4 > bytes.length) return
          val payload = java.util.Arrays.copyOfRange(bytes, i, i + 4); i += 4; Fixed32(payload)
        case _ =>
          // Unknown / deprecated group wire types — stop walking rather
          // than risk misalignment. Partial parse is acceptable here
          // since we only need top-level field hits.
          return
      }
      if (f.isDefinedAt((field, value))) f((field, value))
    }
  }

  private def readVarint(bytes: Array[Byte], start: Int): (Long, Int) = {
    var result = 0L
    var shift = 0
    var i = start
    while (i < bytes.length) {
      val b = bytes(i) & 0xff
      result |= ((b & 0x7fL) << shift)
      i += 1
      if ((b & 0x80) == 0) return (result, i)
      shift += 7
      if (shift > 64) return (result, i) // overflow guard
    }
    (result, i)
  }
}
