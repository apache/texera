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

import jakarta.servlet.{DispatcherType, FilterChain}
import jakarta.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.eclipse.jetty.servlet.{FilterHolder, ServletContextHandler}
import org.mockito.ArgumentMatchers.{any, eq => eqTo}
import org.mockito.Mockito.{mock, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RequestLoggingFilterSpec extends AnyFlatSpec with Matchers {

  "RequestLoggingFilter.doFilter" should "delegate to the chain and log the request" in {
    val filter = new RequestLoggingFilter
    val request = mock(classOf[HttpServletRequest])
    val response = mock(classOf[HttpServletResponse])
    val chain = mock(classOf[FilterChain])
    when(request.getRemoteAddr).thenReturn("1.2.3.4")
    when(request.getMethod).thenReturn("GET")
    when(request.getRequestURI).thenReturn("/api/x")
    when(request.getProtocol).thenReturn("HTTP/1.1")
    when(response.getStatus).thenReturn(200)

    filter.doFilter(request, response, chain)

    // the chain is always invoked, before any logging
    verify(chain).doFilter(request, response)
    // the request/response fields are read to build the INFO log line
    verify(request).getRemoteAddr
    verify(request).getMethod
    verify(request).getRequestURI
    verify(request).getProtocol
    verify(response).getStatus
  }

  "RequestLoggingFilter.register" should "add the filter to the servlet context for all dispatch types" in {
    val context = mock(classOf[ServletContextHandler])
    RequestLoggingFilter.register(context)
    verify(context).addFilter(
      any(classOf[FilterHolder]),
      eqTo("/*"),
      eqTo(java.util.EnumSet.allOf(classOf[DispatcherType]))
    )
  }
}
