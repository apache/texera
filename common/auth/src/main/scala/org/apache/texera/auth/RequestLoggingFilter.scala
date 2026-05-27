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

package org.apache.texera.auth

import jakarta.servlet._
import jakarta.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.slf4j.LoggerFactory

/**
  * Servlet filter that logs HTTP requests through SLF4J at INFO level.
  * This replaces Dropwizard's built-in request log (which uses a separate
  * access log pipeline not controllable by log level) so that request logs
  * are fully controlled by the TEXERA_SERVICE_LOG_LEVEL environment variable.
  */
class RequestLoggingFilter extends Filter {
  private val logger = LoggerFactory.getLogger("org.eclipse.jetty.server.RequestLog")

  override def doFilter(
      request: ServletRequest,
      response: ServletResponse,
      chain: FilterChain
  ): Unit = {
    chain.doFilter(request, response)
    if (logger.isInfoEnabled) {
      val req = request.asInstanceOf[HttpServletRequest]
      val resp = response.asInstanceOf[HttpServletResponse]
      logger.info(
        s"""${req.getRemoteAddr} - "${req.getMethod} ${req.getRequestURI} ${req.getProtocol}" ${resp.getStatus}"""
      )
    }
  }
}

object RequestLoggingFilter {

  /**
    * Registers the request logging filter on the given servlet context.
    * Usage: RequestLoggingFilter.register(environment.getApplicationContext)
    */
  def register(context: org.eclipse.jetty.servlet.ServletContextHandler): Unit = {
    context.addFilter(
      new org.eclipse.jetty.servlet.FilterHolder(new RequestLoggingFilter),
      "/*",
      java.util.EnumSet.allOf(classOf[DispatcherType])
    )
  }
}
