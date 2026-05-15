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

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}
import org.apache.texera.amber.operator.map.MapOpExec
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration

class MachineUDFOpExec(descString: String) extends MapOpExec {
  private val desc: MachineUDFOpDesc =
    objectMapper.readValue(descString, classOf[MachineUDFOpDesc])

  private val httpClient: HttpClient =
    HttpClient
      .newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(10))
      .build()

  setMapFunc(runOnMachine)

  private def runOnMachine(tuple: Tuple): TupleLike = {
    val tupleJson = objectMapper.createObjectNode()
    tuple.schema.getAttributes.foreach { attr =>
      val v = tuple.getField[AnyRef](attr.getName)
      tupleJson.putPOJO(attr.getName, v)
    }

    val payload = objectMapper.createObjectNode()
    payload.put("code", Option(desc.code).getOrElse(""))
    payload.set[ObjectNode]("tuple_in", tupleJson)
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

    // Build the output tuple as a sequence of (name, value). For columns the operator
    // declared as retained input columns, always re-use the original input tuple's value
    // (preserves type identity with the input schema). Only use the script's result for
    // columns not already present in the input schema (i.e. the declared outputColumns).
    val builder = scala.collection.mutable.LinkedHashMap[String, Any]()
    val inputAttrNames = scala.collection.mutable.Set.empty[String]
    if (desc.retainInputColumns) {
      tuple.schema.getAttributes.foreach { attr =>
        builder(attr.getName) = tuple.getField[Any](attr.getName)
        inputAttrNames += attr.getName
      }
    }
    val result = body.path("result")
    if (result.isObject) {
      val it = result.fields()
      while (it.hasNext) {
        val entry = it.next()
        val key = entry.getKey
        if (!inputAttrNames.contains(key)) {
          val node = entry.getValue
          val value: Any =
            if (node.isNull) null
            else if (node.isInt) node.asInt()
            else if (node.isLong) node.asLong()
            else if (node.isDouble || node.isFloat) node.asDouble()
            else if (node.isBoolean) node.asBoolean()
            else node.asText()
          builder(key) = value
        }
      }
    }

    TupleLike(builder.toSeq: _*)
  }
}
