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
 * gRPC-Web framing over `application/grpc-web+proto`. We do that here
 * without pulling in a scalapb-generated client: the request body is
 * a hand-coded protobuf payload for one RPC (QueryRange), and the
 * response is walked at the wire-format level just deep enough to
 * surface a meaningful summary frame.
 *
 * Field numbers below were discovered empirically against a live
 * Parca 0.28 server; they match the proto schema published in the
 * parca-dev/parca repo at that tag. The single-RPC scope is the
 * point: a full flamegraph reader would need the nested Function /
 * Location / Mapping schema and is a much bigger PR.
 */
object ParcaClient extends LazyLogging {

  /** Reverse-engineered field layout for `query.v1alpha1.QueryRangeRequest`:
   *   1 (string)  query
   *   2 (message) start  google.protobuf.Timestamp
   *   3 (message) end    google.protobuf.Timestamp
   *   4 (varint)  limit
   */
  def queryRange(
      baseUrl: String,
      profileQuery: String,
      startMs: Long,
      endMs: Long,
      limit: Int = 1000,
      timeoutMs: Long = 5000L
  ): Either[GatewayError, ParcaQueryRangeSummary] = {
    val payload = ProtoEncode.queryRangeRequest(profileQuery, startMs, endMs, limit)
    postGrpcWeb(baseUrl, "/parca.query.v1alpha1.QueryService/QueryRange", payload, timeoutMs)
      .flatMap(parseQueryRangeResponse)
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
    val client = HttpClient
      .newBuilder()
      .connectTimeout(Duration.ofMillis(timeoutMs))
      .followRedirects(HttpClient.Redirect.NEVER)
      .build()
    logger.debug(s"[profiles] sending gRPC-Web POST $baseUrl$path (${framed.length} bytes, timeout ${timeoutMs}ms)")
    Try(client.send(req, BodyHandlers.ofByteArray())) match {
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
            Left(GatewayError("backend_error", s"profiles backend gRPC status=$other ${msg.take(120)}", 502))
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

  // ---- Response walking --------------------------------------------------

  /** QueryRangeResponse top-level layout:
   *    1 (message, repeated) MetricsSeries  series
   *  Each Series:
   *    1 (message) LabelSet  labelset
   *    2 (message, repeated) MetricsSample  samples
   *  Each MetricsSample:
   *    1 (message) google.protobuf.Timestamp timestamp
   *    2 (varint)  value          // sample count or value
   *    5 (varint)  duration_ns    // observed at runtime
   */
  private def parseQueryRangeResponse(body: Array[Byte]): Either[GatewayError, ParcaQueryRangeSummary] = {
    val unframed = stripGrpcWebFrame(body) match {
      case Right(b) => b
      case Left(e) => return Left(e)
    }
    var seriesCount = 0
    var sampleCount = 0L
    val series = scala.collection.mutable.ArrayBuffer.empty[ParcaSeriesSummary]
    Proto.foreachField(unframed) {
      case (1, Proto.LengthDelimited(bytes)) =>
        seriesCount += 1
        val (label, samples, totalValue) = parseSeries(bytes)
        sampleCount += samples
        series += ParcaSeriesSummary(label.getOrElse("(unlabeled)"), samples, totalValue)
      case _ => ()
    }
    Right(ParcaQueryRangeSummary(seriesCount, sampleCount, series.toSeq))
  }

  private def parseSeries(bytes: Array[Byte]): (Option[String], Long, Long) = {
    var label: Option[String] = None
    var sampleCount = 0L
    var totalValue = 0L
    Proto.foreachField(bytes) {
      case (1, Proto.LengthDelimited(b)) =>
        label = label.orElse(extractFirstLabelString(b))
      case (2, Proto.LengthDelimited(b)) =>
        // MetricsSample. Field 2 (varint) is the sample value.
        sampleCount += 1
        Proto.foreachField(b) {
          case (2, Proto.VarInt(v)) => totalValue += v
          case _                    => ()
        }
      case _ => ()
    }
    (label, sampleCount, totalValue)
  }

  /** LabelSet contains repeated Label{name (1), value (2)}. We render
   *  the first key=value as a compact human-readable identifier. */
  private def extractFirstLabelString(bytes: Array[Byte]): Option[String] = {
    var name: Option[String] = None
    var value: Option[String] = None
    Proto.foreachField(bytes) {
      case (1, Proto.LengthDelimited(b)) =>
        // first sub-label is itself a Label{name=1,value=2}
        Proto.foreachField(b) {
          case (1, Proto.LengthDelimited(nb)) => if (name.isEmpty) name = Some(new String(nb, "UTF-8"))
          case (2, Proto.LengthDelimited(vb)) => if (value.isEmpty) value = Some(new String(vb, "UTF-8"))
          case _ => ()
        }
      case _ => ()
    }
    (name, value) match {
      case (Some(n), Some(v)) => Some(s"$n=$v")
      case (Some(n), None)    => Some(n)
      case _                  => None
    }
  }

  private def stripGrpcWebFrame(body: Array[Byte]): Either[GatewayError, Array[Byte]] = {
    if (body.length < 5) Right(Array.emptyByteArray)
    else {
      val len = ByteBuffer.wrap(body, 1, 4).order(ByteOrder.BIG_ENDIAN).getInt
      if (len < 0 || 5 + len > body.length)
        Left(GatewayError("bad_backend_response", "malformed gRPC-Web frame from profiles backend", 502))
      else Right(java.util.Arrays.copyOfRange(body, 5, 5 + len))
    }
  }
}

case class ParcaSeriesSummary(label: String, sampleCount: Long, totalValue: Long)

case class ParcaQueryRangeSummary(
    seriesCount: Int,
    totalSampleCount: Long,
    series: Seq[ParcaSeriesSummary]
)

/** Maps the lightweight Parca summary into the dashboard's
 *  [[FlameFrame]] DTO. The shape is a one-deep tree:
 *
 *    root (totalSampleCount)
 *      ├── series-label-1 (count)
 *      ├── series-label-2 (count)
 *      └── ...
 *
 *  Real call-stack flame nodes need the nested Function / Location /
 *  Mapping schema in Parca's profile.proto; pulling those in is the
 *  next PR. Until then this gives the dashboard real numbers from a
 *  real backend instead of an empty panel.
 */
object ParcaSummary {
  import org.apache.texera.web.observability.gateway.dtos._

  /** Cap on children rendered. The dashboard's flame view degrades on
   *  thousands of siblings; we already cap at 1000 in upstream parsing
   *  but trim again here to be defensive. */
  private val MaxRenderedSeries: Int = 256

  def toProfilesResponse(summary: ParcaQueryRangeSummary): ProfilesQueryResponse = {
    if (summary.seriesCount == 0 || summary.totalSampleCount == 0L) {
      ProfilesQueryResponse(root = None, totalSamples = 0L)
    } else {
      val children = summary.series
        .take(MaxRenderedSeries)
        .map(s => FlameFrame(name = s.label, value = s.sampleCount, children = Seq.empty))
      val rootName = s"parca_agent cpu samples (${summary.seriesCount} series)"
      ProfilesQueryResponse(
        root = Some(FlameFrame(name = rootName, value = summary.totalSampleCount, children = children)),
        totalSamples = summary.totalSampleCount
      )
    }
  }
}

/** Tiny protobuf-wire-format encoder used for outbound requests. */
private[gateway] object ProtoEncode {

  def queryRangeRequest(query: String, startMs: Long, endMs: Long, limit: Int): Array[Byte] = {
    // Field 1: query (string)
    // Field 2: start Timestamp
    // Field 3: end Timestamp
    // Field 4: limit (varint)
    val parts = new java.io.ByteArrayOutputStream()
    writeString(parts, fieldNumber = 1, query)
    writeMessage(parts, fieldNumber = 2, timestamp(startMs))
    writeMessage(parts, fieldNumber = 3, timestamp(endMs))
    writeVarintField(parts, fieldNumber = 4, limit.toLong)
    parts.toByteArray
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

  private def writeVarintField(out: java.io.ByteArrayOutputStream, fieldNumber: Int, value: Long): Unit = {
    writeVarint(out, ((fieldNumber.toLong) << 3) | 0L)
    writeVarint(out, value)
  }

  private def writeString(out: java.io.ByteArrayOutputStream, fieldNumber: Int, value: String): Unit = {
    val bytes = value.getBytes("UTF-8")
    writeVarint(out, ((fieldNumber.toLong) << 3) | 2L)
    writeVarint(out, bytes.length.toLong)
    out.write(bytes)
  }

  private def writeMessage(out: java.io.ByteArrayOutputStream, fieldNumber: Int, value: Array[Byte]): Unit = {
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
 *  (deprecated, not used by Parca). */
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
          val payload = java.util.Arrays.copyOfRange(bytes, i, i + len.toInt)
          i += len.toInt
          LengthDelimited(payload)
        case 1 =>
          val payload = java.util.Arrays.copyOfRange(bytes, i, i + 8); i += 8; Fixed64(payload)
        case 5 =>
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
