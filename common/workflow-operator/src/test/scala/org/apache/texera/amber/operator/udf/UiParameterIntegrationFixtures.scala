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

package org.apache.texera.amber.operator.udf

import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import org.apache.texera.amber.core.tuple.Attribute
import org.apache.texera.amber.operator.udf.python.PythonUdfUiParameterInjector
import org.apache.texera.amber.operator.udf.r.RUdfUiParameterInjector
import org.apache.texera.amber.util.JSONUtils.objectMapper

import _root_.java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/** Produces real compiler output for the Python/R runtime and Arrow integration suite. */
object UiParameterIntegrationFixtures {
  private def parameter(
      name: String,
      token: String,
      value: String,
      resource: String = ""
  ): UiUDFParameter = {
    val result = new UiUDFParameter
    result.attribute = new Attribute(name, UiUdfParameterContract.typesByToken(token))
    result.value = value
    result.inputType = resource
    result
  }

  def main(args: Array[String]): Unit = {
    require(args.length == 1, "Supply the generated JSON fixture path.")
    val output = objectMapper.createObjectNode()
    val contract =
      objectMapper.readTree(getClass.getResourceAsStream("/udf/ui-parameter-contract.json"))
    val values = output.putArray("values")
    contract.path("values").elements().asScala.foreach { example =>
      val item = example.deepCopy[ObjectNode]()
      val token = example.path("type").asText()
      val parameters = List(parameter("x", token, example.path("raw").asText()))
      val pythonCode = s"""from pytexera import *
        |class ProcessTupleOperator(UDFOperatorV2):
        |    def open(self):
        |        self.result = self.UiParameter("x", AttributeType.$token).value
        |    def process_tuple(self, tuple_, port):
        |        yield tuple_
        |""".stripMargin
      val rCode = s"""function() UiParameter("x", AttributeType$$$token)$$value"""
      Try {
        item.put("pythonCode", PythonUdfUiParameterInjector.inject(pythonCode, parameters))
        item.put("rCode", RUdfUiParameterInjector.inject(rCode, parameters))
      } match {
        case Success(_)     => ()
        case Failure(error) => item.put("compileError", error.getMessage)
      }
      values.add(item)
    }
    val parameters = List(
      parameter("count", "INT", "7"),
      parameter("label", "STRING", "café\n\"; stop('injected') #"),
      parameter("enabled", "BOOL", "2"),
      parameter("score", "DOUBLE", "2.5"),
      parameter("long_value", "LONG", "+0009223372036854775807"),
      parameter("created", "TIMESTAMP", "2026-11-03T14:30+02:00"),
      parameter("data", "STRING", "/tmp/texera-r-ui-contract-data", "dataset"),
      parameter("model", "STRING", "/tmp/texera-r-ui-contract-model", "model")
    )
    val declarations = parameters
      .map { p =>
        val token = UiUdfParameterContract.types(p.attribute.getType).head
        val resource = Option
          .when(p.inputType.nonEmpty)(s", value=Resource$$${p.inputType.toUpperCase}")
          .getOrElse("")
        s"""${p.attribute.getName} <- UiParameter("${p.attribute.getName}", AttributeType$$$token$resource)$$value"""
      }
      .mkString("\n")
    val fields = Seq(
      "count" -> "count",
      "label" -> "label",
      "enabled" -> "enabled",
      "score" -> "score",
      "long_value" -> "long_value",
      "created" -> "created",
      "resource_rows" -> "nrow(read.csv(file.path(data, 'fixture.csv')))",
      "model_text" -> "readLines(file.path(model, 'model.txt'), warn=FALSE)"
    )
    val operators: ArrayNode = output.putArray("operators")
    for (source <- Seq(false, true); tuple <- Seq(false, true)) {
      val output = s"${if (tuple) "list" else "data.frame"}(${fields
        .map { case (name, value) => s"$name=$value" }
        .mkString(", ")})"
      val body =
        if (source) output
        else
          s"output <- $output\nfor (name in names(output)) input[[name]] <- output[[name]]\ninput"
      val args = if (source) "" else "input, port"
      val expression =
        if (tuple)
          s"library(coro)\n$declarations\ncoro::generator(function($args) { yield({ $body }) })"
        else s"$declarations\nfunction($args) { $body }"
      val item = objectMapper.createObjectNode()
      item.put("source", source)
      item.put("tuple", tuple)
      val sourceCode = if (source && !tuple) expression.replace("\n", "\r\n") else expression
      item.put("code", RUdfUiParameterInjector.inject(sourceCode, parameters))
      operators.add(item)
    }
    val path = Paths.get(args(0))
    Files.createDirectories(path.toAbsolutePath.getParent)
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(path.toFile, output)
    println(s"Generated $path")
  }
}
