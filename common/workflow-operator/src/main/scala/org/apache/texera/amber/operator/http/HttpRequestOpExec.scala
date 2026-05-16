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

package org.apache.texera.amber.operator.http

import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}
import org.apache.texera.amber.operator.http.util.{HttpClientFactory, HttpMethod, TemplateInterpolator}
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.net.URI
import java.net.http.{HttpRequest, HttpResponse}
import java.time.Duration
import scala.collection.mutable

class HttpRequestOpExec(descString: String) extends OperatorExecutor {
  private val desc: HttpRequestOpDesc =
    objectMapper.readValue(descString, classOf[HttpRequestOpDesc])

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    val resolvedUrl = TemplateInterpolator.interpolate(desc.url, tuple)
    val resolvedBody = TemplateInterpolator.interpolate(desc.bodyTemplate, tuple)

    val (status, body, error) = try {
      val builder = HttpRequest
        .newBuilder()
        .uri(URI.create(resolvedUrl))
        .timeout(Duration.ofSeconds(math.max(1, desc.timeoutSeconds).toLong))

      Option(desc.headers).foreach { hs =>
        hs.forEach { kv =>
          if (kv != null && kv.key != null && kv.value != null) {
            builder.header(kv.key, kv.value)
          }
        }
      }

      val bodyPublisher =
        if (resolvedBody != null && resolvedBody.nonEmpty)
          HttpRequest.BodyPublishers.ofString(resolvedBody)
        else HttpRequest.BodyPublishers.noBody()

      val method = if (desc.method == null) HttpMethod.POST else desc.method
      method match {
        case HttpMethod.GET    => builder.GET()
        case HttpMethod.POST   => builder.POST(bodyPublisher)
        case HttpMethod.PUT    => builder.PUT(bodyPublisher)
        case HttpMethod.PATCH  => builder.method("PATCH", bodyPublisher)
        case HttpMethod.DELETE => builder.DELETE()
      }

      val response = HttpClientFactory.sharedClient
        .send(builder.build(), HttpResponse.BodyHandlers.ofString())
      val code = response.statusCode()
      val errOpt =
        if (code >= 200 && code < 300) null
        else s"HTTP $code"
      (Integer.valueOf(code), response.body(), errOpt)
    } catch {
      case t: Throwable =>
        if (desc.failOnError) throw t
        (Integer.valueOf(-1), "", s"${t.getClass.getSimpleName}: ${t.getMessage}")
    }

    if (desc.failOnError && error != null) {
      throw new RuntimeException(s"HTTP request failed: $error (body=$body)")
    }

    val fields = mutable.LinkedHashMap[String, Any]()
    tuple.schema.getAttributeNames.foreach { name =>
      fields(name) = tuple.getField[Any](name)
    }
    fields("http_request_status") = status
    fields("http_request_body") = body
    fields("http_request_error") = error
    Iterator(TupleLike(fields.toSeq: _*))
  }
}
