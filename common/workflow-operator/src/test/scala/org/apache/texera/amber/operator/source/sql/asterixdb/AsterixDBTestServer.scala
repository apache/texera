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
package org.apache.texera.amber.operator.source.sql.asterixdb

import com.sun.net.httpserver.{HttpExchange, HttpServer}

import java.net.{InetSocketAddress, URLDecoder}
import java.nio.charset.StandardCharsets
import scala.collection.mutable

private[asterixdb] object AsterixDBTestServer {
  def versionJson(version: String): String =
    s"""{"git.build.version":"$version"}"""

  def datatypeResult(fields: Seq[(String, String)]): String = {
    val entries = fields
      .map { case (name, tpe) => s"""{"FieldName":"$name","FieldType":"$tpe"}""" }
      .mkString(",")
    s"""{"results":[{"Fields":[$entries]}]}"""
  }

  def forStatements(recorded: mutable.Buffer[String], port: Int = 0)(
      response: String => String
  ): HttpServer =
    bind(port) { form =>
      val statement = form.getOrElse("statement", "")
      recorded.synchronized { recorded += statement }
      (200, response(statement))
    }

  def bind(port: Int = 0, version: => (Int, String) = (200, versionJson("0.9.9")))(
      query: Map[String, String] => (Int, String)
  ): HttpServer = {
    val server = HttpServer.create(new InetSocketAddress(port), 0)
    server.createContext(
      "/admin/version",
      (exchange: HttpExchange) => {
        val (status, body) = version
        respond(exchange, status, body)
      }
    )
    server.createContext(
      "/query/service",
      (exchange: HttpExchange) => {
        val is = exchange.getRequestBody
        val body =
          try new String(is.readAllBytes(), StandardCharsets.UTF_8)
          finally is.close()
        val (status, responseBody) = query(parseForm(body))
        respond(exchange, status, responseBody)
      }
    )
    server
  }

  private def respond(exchange: HttpExchange, status: Int, body: String): Unit = {
    if (status == 204) {
      // A 204 response is bodyless.
      exchange.sendResponseHeaders(204, -1)
      exchange.close()
    } else {
      val bytes = body.getBytes(StandardCharsets.UTF_8)
      exchange.getResponseHeaders.add("Content-Type", "application/json")
      exchange.sendResponseHeaders(status, bytes.length.toLong)
      val os = exchange.getResponseBody
      try os.write(bytes)
      finally os.close()
    }
  }

  private def parseForm(body: String): Map[String, String] =
    body
      .split("&")
      .filter(_.contains("="))
      .map { pair =>
        val idx = pair.indexOf('=')
        URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8) ->
          URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8)
      }
      .toMap
}
