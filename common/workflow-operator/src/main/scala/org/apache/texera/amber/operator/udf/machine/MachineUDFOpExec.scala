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

package org.apache.texera.amber.operator.udf.machine

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import scala.collection.mutable

/**
  * Runs a user Python script on a *registered machine* via its machine-manager service.
  *
  * Two modes (selected by `desc.batchMode`):
  *   - per-tuple (default): one HTTP /python call per input row. The output of each call's
  *     last JSON stdout line becomes the output tuple. Input row is exposed to the script
  *     as `tuple_in` (dict).
  *   - batch: buffer every input row, and on `onFinish` issue ONE HTTP call with the full
  *     buffer. `tuple_in` is then a list of dicts. The script can print multiple JSON lines
  *     and each becomes an output tuple — useful for e.g. training N models and emitting
  *     one metric row per model.
  *
  * In both cases we pin HTTP/1.1 to work around uvicorn's h2c upgrade dropping the request
  * body (we hit that bug with HTTP/2 in earlier testing).
  */
class MachineUDFOpExec(descString: String) extends OperatorExecutor {
  private val desc: MachineUDFOpDesc =
    objectMapper.readValue(descString, classOf[MachineUDFOpDesc])

  private val httpClient: HttpClient =
    HttpClient
      .newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(10))
      .build()

  // Per-tuple mode: nothing to buffer. Batch mode: accumulate input rows here.
  private val batchBuffer: mutable.ArrayBuffer[ObjectNode] =
    mutable.ArrayBuffer.empty[ObjectNode]

  private def tupleToObjectNode(tuple: Tuple): ObjectNode = {
    val node = objectMapper.createObjectNode()
    tuple.schema.getAttributes.foreach { attr =>
      val v = tuple.getField[AnyRef](attr.getName)
      node.putPOJO(attr.getName, v)
    }
    node
  }

  /**
    * POST to `<machineUrl>/python` with a JSON body and return the parsed response body.
    * `tupleInPayload` is either an ObjectNode (per-tuple) or an ArrayNode (batch).
    */
  private def callMachinePython(tupleInPayload: JsonNode): JsonNode = {
    val payload = objectMapper.createObjectNode()
    payload.put("code", Option(desc.code).getOrElse(""))
    payload.set[ObjectNode]("tuple_in", tupleInPayload)
    payload.put("timeout_seconds", desc.timeoutSeconds.toDouble)

    val payloadJson = objectMapper.writeValueAsString(payload)
    val targetUrl = desc.machineUrl.stripSuffix("/") + "/python"
    System.err.println(
      s"[MachineUDFOpExec] -> POST $targetUrl bodyLen=${payloadJson.length} bodyPreview=${payloadJson.take(180)}"
    )

    val requestBuilder = HttpRequest
      .newBuilder(URI.create(targetUrl))
      .timeout(Duration.ofSeconds(desc.timeoutSeconds.toLong + 15L))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(payloadJson))

    if (desc.machineToken != null && desc.machineToken.trim.nonEmpty) {
      requestBuilder.header("Authorization", s"Bearer ${desc.machineToken.trim}")
    }

    val response =
      httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString())

    if (response.statusCode() / 100 != 2) {
      throw new RuntimeException(
        s"machine-manager ${desc.machineUrl} returned HTTP ${response.statusCode()}: ${response.body()}"
      )
    }

    val body = objectMapper.readTree(response.body())
    val exitCode = body.path("exit_code").asInt(-1)
    val stderr = body.path("stderr").asText("")
    if (exitCode != 0) {
      throw new RuntimeException(
        s"machine-manager script failed (exit=$exitCode): $stderr"
      )
    }
    body
  }

  /**
    * For per-tuple mode, build a single output tuple from the script's parsed `result`
    * (the last JSON line). If `retainInputColumns`, original input values are re-used
    * for any column the script did not override (preserves type identity with the input
    * schema).
    */
  private def buildPerTupleOutput(inputTuple: Tuple, scriptResult: JsonNode): TupleLike = {
    val builder = mutable.LinkedHashMap[String, Any]()
    val inputAttrNames = mutable.Set.empty[String]
    if (desc.retainInputColumns) {
      inputTuple.schema.getAttributes.foreach { attr =>
        builder(attr.getName) = inputTuple.getField[Any](attr.getName)
        inputAttrNames += attr.getName
      }
    }
    if (scriptResult.isObject) {
      val it = scriptResult.fields()
      while (it.hasNext) {
        val entry = it.next()
        val key = entry.getKey
        if (!inputAttrNames.contains(key)) {
          builder(key) = jsonToScala(entry.getValue)
        }
      }
    }
    TupleLike(builder.toSeq: _*)
  }

  /**
    * For batch mode, the script can emit one or more rows. We accept either:
    *   - `result` is an ObjectNode → emit a single row from the declared output columns.
    *   - `result` is an ArrayNode of ObjectNode → emit one row per element.
    *   - script's stdout has multiple JSON object lines → each becomes a row.
    * We accept the last form via the response's `stdout` field, splitting on lines.
    */
  private def buildBatchOutputs(responseBody: JsonNode): Iterator[TupleLike] = {
    val outputColumns = Option(desc.outputColumns).getOrElse(List()).map(_.getName)
    val result = responseBody.path("result")

    val rows: Seq[JsonNode] =
      if (result.isArray) {
        val arr = result.asInstanceOf[ArrayNode]
        (0 until arr.size()).map(arr.get)
      } else if (result.isObject) {
        Seq(result)
      } else {
        // No structured `result`; fall back to scanning stdout for JSON object lines.
        val stdout = responseBody.path("stdout").asText("")
        stdout
          .split("\n")
          .toSeq
          .map(_.trim)
          .filter(_.nonEmpty)
          .flatMap { line =>
            scala.util.Try(objectMapper.readTree(line)).toOption.filter(_.isObject)
          }
      }

    rows.iterator.map(row => batchRowFromJson(row, outputColumns))
  }

  private def batchRowFromJson(row: JsonNode, outputColumnNames: List[String]): TupleLike = {
    val builder = mutable.LinkedHashMap[String, Any]()
    // Project only the declared output columns, in declared order. Missing columns become null.
    for (colName <- outputColumnNames) {
      val v = row.path(colName)
      builder(colName) = if (v.isMissingNode) null else jsonToScala(v)
    }
    TupleLike(builder.toSeq: _*)
  }

  private def jsonToScala(node: JsonNode): Any = {
    if (node == null || node.isNull) null
    else if (node.isInt) node.asInt()
    else if (node.isLong) node.asLong()
    else if (node.isDouble || node.isFloat) node.asDouble()
    else if (node.isBoolean) node.asBoolean()
    else node.asText()
  }

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    if (desc.batchMode) {
      // Buffer; emit nothing until input is finished.
      batchBuffer += tupleToObjectNode(tuple)
      Iterator.empty
    } else {
      val body = callMachinePython(tupleToObjectNode(tuple))
      Iterator.single(buildPerTupleOutput(tuple, body.path("result")))
    }
  }

  override def onFinish(port: Int): Iterator[TupleLike] = {
    if (!desc.batchMode) return Iterator.empty
    val arr = objectMapper.createArrayNode()
    batchBuffer.foreach(arr.add)
    System.err.println(
      s"[MachineUDFOpExec] batch finished; sending ${batchBuffer.size} rows to ${desc.machineUrl}"
    )
    val body = callMachinePython(arr)
    buildBatchOutputs(body)
  }
}
