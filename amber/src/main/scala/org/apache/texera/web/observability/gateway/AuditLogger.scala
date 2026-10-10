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

import org.apache.texera.observability.LogSanitizer
import org.slf4j.{Logger, LoggerFactory}

/**
  * Per-query audit log for the gateway.
  *
  * Lives on its own SLF4J logger name so operators can route audit
  * events to a dedicated appender (file/syslog/SIEM) separate from
  * application logs. The logback.xml in each service can selectively
  * disable propagation to the root appender if audit events should
  * NOT flow into the OTel collector.
  *
  * The audit line is one JSON-shaped string per query. Free-text
  * fields go through [[LogSanitizer.sanitize]] first so secret
  * patterns and CRLF injections cannot land in audit storage.
  */
object AuditLogger {

  private val log: Logger = LoggerFactory.getLogger("texera.audit.observability")

  case class Entry(
      userId: Long,
      remoteIp: String,
      endpoint: String,
      signal: String,
      scope: GatewayScope,
      query: String,
      fromMs: Long,
      toMs: Long,
      hits: Long
  )

  def record(entry: Entry): Unit = {
    if (!log.isInfoEnabled) return
    val safeQuery = LogSanitizer.sanitize(entry.query)
    val msg = new StringBuilder
    msg.append("{")
    msg.append(s""""ts":${System.currentTimeMillis()}""")
    msg.append(s""","user":${entry.userId}""")
    msg.append(s""","ip":"${escapeJson(entry.remoteIp)}"""")
    msg.append(s""","endpoint":"${escapeJson(entry.endpoint)}"""")
    msg.append(s""","signal":"${escapeJson(entry.signal)}"""")
    msg.append(s""","allowed_workflows":${entry.scope.allowedWorkflowIds.size}""")
    msg.append(s""","allowed_projects":${entry.scope.allowedProjectIds.size}""")
    msg.append(s""","query":"${escapeJson(safeQuery)}"""")
    msg.append(s""","from":${entry.fromMs}""")
    msg.append(s""","to":${entry.toMs}""")
    msg.append(s""","hits":${entry.hits}""")
    msg.append("}")
    log.info(msg.toString)
  }

  private def escapeJson(s: String): String = {
    if (s == null) return ""
    val out = new StringBuilder(s.length + 8)
    var i = 0
    while (i < s.length) {
      val c = s.charAt(i)
      c match {
        case '"'                        => out.append("\\\"")
        case '\\'                       => out.append("\\\\")
        case _ if c < 0x20 || c == 0x7f => // control chars stripped (defense in depth)
        case _                          => out.append(c)
      }
      i += 1
    }
    out.toString
  }
}
