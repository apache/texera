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

package org.apache.texera.amber.operator.source.http

import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.amber.core.executor.SourceOperatorExecutor
import org.apache.texera.amber.core.tuple.TupleLike
import org.apache.texera.amber.operator.http.util.{HttpClientFactory, HttpMethod}
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.net.URI
import java.net.http.{HttpRequest, HttpResponse}
import java.sql.Timestamp
import java.time.Duration

class PollingHttpSourceOpExec(descString: String) extends SourceOperatorExecutor with LazyLogging {
  private val desc: PollingHttpSourceOpDesc =
    objectMapper.readValue(descString, classOf[PollingHttpSourceOpDesc])

  logger.info(
    s"[PollingHttpSource] url=${desc.url} interval=${desc.intervalSeconds}s " +
      s"maxIterations=${desc.maxIterations} method=${desc.method} descRaw=$descString"
  )

  override def produceTuple(): Iterator[TupleLike] = new Iterator[TupleLike] {
    private var iteration: Long = 0L
    private var firstCall: Boolean = true

    override def hasNext: Boolean =
      desc.maxIterations <= 0 || iteration < desc.maxIterations

    override def next(): TupleLike = {
      // Sleep between polls (skip the wait on the very first iteration so the
      // workflow emits its first tuple promptly).
      if (firstCall) firstCall = false
      else Thread.sleep(math.max(0, desc.intervalSeconds).toLong * 1000L)
      iteration += 1
      poll()
    }
  }

  private def poll(): TupleLike = {
    val requestBuilder = HttpRequest
      .newBuilder()
      .uri(URI.create(desc.url))
      .timeout(Duration.ofSeconds(30))

    Option(desc.headers).foreach { hs =>
      hs.forEach { kv =>
        if (kv != null && kv.key != null && kv.value != null) {
          requestBuilder.header(kv.key, kv.value)
        }
      }
    }

    val bodyPublisher =
      if (desc.requestBody != null && desc.requestBody.nonEmpty)
        HttpRequest.BodyPublishers.ofString(desc.requestBody)
      else HttpRequest.BodyPublishers.noBody()

    val method = if (desc.method == null) HttpMethod.GET else desc.method
    method match {
      case HttpMethod.GET    => requestBuilder.GET()
      case HttpMethod.POST   => requestBuilder.POST(bodyPublisher)
      case HttpMethod.PUT    => requestBuilder.PUT(bodyPublisher)
      case HttpMethod.PATCH  => requestBuilder.method("PATCH", bodyPublisher)
      case HttpMethod.DELETE => requestBuilder.DELETE()
    }

    val (body, status) =
      try {
        val response = HttpClientFactory.sharedClient
          .send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString())
        (response.body(), response.statusCode())
      } catch {
        case t: Throwable => (s"ERROR: ${t.getMessage}", -1)
      }

    TupleLike(body, Integer.valueOf(status), new Timestamp(System.currentTimeMillis()))
  }
}
