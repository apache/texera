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

package org.apache.texera.web.observability

import org.apache.texera.observability.LogSanitizer
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.MDC

import javax.servlet.{FilterChain, ServletRequest, ServletResponse}
import javax.servlet.http.HttpServletRequest

class RequestContextMdcFilterSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  override def beforeEach(): Unit = MDC.clear()
  override def afterEach(): Unit = MDC.clear()

  // Minimal HttpServletRequest stub — only the methods the filter
  // actually reads. Hand-rolled instead of pulling Mockito to keep
  // the dependency surface narrow.
  private class StubReq(
      path: String,
      headers: Map[String, String] = Map.empty,
      params: Map[String, String] = Map.empty
  ) extends HttpServletRequestStub {
    override def getRequestURI: String = path
    override def getHeader(name: String): String = headers.getOrElse(name, null)
    override def getParameter(name: String): String = params.getOrElse(name, null)
  }

  /** Captures the MDC state from inside the chain so we can assert on
    *  the per-request context, not the post-finally cleared state.
    */
  private class CapturingChain extends FilterChain {
    var captured: Map[String, String] = Map.empty
    override def doFilter(request: ServletRequest, response: ServletResponse): Unit = {
      val ctx = MDC.getCopyOfContextMap
      captured =
        if (ctx == null) Map.empty
        else {
          val sb = scala.collection.mutable.Map.empty[String, String]
          ctx.forEach((k, v) => sb.put(k, v))
          sb.toMap
        }
    }
  }

  private val filter = new RequestContextMdcFilter()

  "RequestContextMdcFilter" should "extract workflow id from a /workflow/<id> URL" in {
    val chain = new CapturingChain
    filter.doFilter(new StubReq("/api/workflow/441/runs"), null, chain)
    chain.captured("texera.workflow.id") shouldBe "441"
  }

  it should "extract execution id from a /execution/<id> URL" in {
    val chain = new CapturingChain
    filter.doFilter(new StubReq("/api/execution/77/status"), null, chain)
    chain.captured("texera.execution.id") shouldBe "77"
  }

  it should "extract computing-unit id from a /computing-unit/<id> URL" in {
    val chain = new CapturingChain
    filter.doFilter(new StubReq("/api/computing-unit/8/health"), null, chain)
    chain.captured("texera.computing_unit.id") shouldBe "8"
  }

  it should "pick up multiple ids in a single URL" in {
    val chain = new CapturingChain
    filter.doFilter(new StubReq("/api/workflow/441/execution/77/computing-unit/8"), null, chain)
    chain.captured should contain allOf (
      "texera.workflow.id" -> "441",
      "texera.execution.id" -> "77",
      "texera.computing_unit.id" -> "8"
    )
  }

  it should "prefer explicit headers over URL inference" in {
    val chain = new CapturingChain
    filter.doFilter(
      new StubReq(
        "/api/workflow/999",
        headers = Map("X-Texera-Workflow-Id" -> "123")
      ),
      null,
      chain
    )
    // Header wins.
    chain.captured("texera.workflow.id") shouldBe "123"
  }

  it should "reject non-digit headers (log-forging guard)" in {
    val chain = new CapturingChain
    filter.doFilter(
      new StubReq("/", headers = Map("X-Texera-Workflow-Id" -> "evil\r\nINJECT")),
      null,
      chain
    )
    chain.captured.get("texera.workflow.id") shouldBe None
  }

  it should "reject impossibly-long ids" in {
    val chain = new CapturingChain
    val twentyDigits = "1" * 20
    filter.doFilter(
      new StubReq("/", headers = Map("X-Texera-Computing-Unit-Id" -> twentyDigits)),
      null,
      chain
    )
    chain.captured.get("texera.computing_unit.id") shouldBe None
  }

  it should "clear MDC entries after the request finishes (no thread leakage)" in {
    val chain = new CapturingChain
    filter.doFilter(new StubReq("/api/workflow/441"), null, chain)
    // Inside the chain it was set:
    chain.captured("texera.workflow.id") shouldBe "441"
    // After the filter completes, the MDC is back to clean.
    MDC.get("texera.workflow.id") shouldBe null
  }

  it should "clear MDC entries even when the chain throws" in {
    val throwingChain = new FilterChain {
      override def doFilter(request: ServletRequest, response: ServletResponse): Unit =
        throw new RuntimeException("downstream failure")
    }
    a[RuntimeException] should be thrownBy {
      filter.doFilter(new StubReq("/api/workflow/441"), null, throwingChain)
    }
    MDC.get("texera.workflow.id") shouldBe null
  }

  it should "not push anything to MDC for URLs that don't match any pattern" in {
    val chain = new CapturingChain
    filter.doFilter(new StubReq("/api/healthcheck"), null, chain)
    chain.captured shouldBe empty
  }

  // ---- query-string extraction (covers the WS upgrade URL) ---------

  it should "extract ids from query parameters (cuid / wid / eid)" in {
    val chain = new CapturingChain
    filter.doFilter(
      new StubReq(
        "/wsapi/workflow-websocket",
        params = Map("cuid" -> "8", "wid" -> "441", "eid" -> "9001")
      ),
      null,
      chain
    )
    chain.captured should contain allOf (
      "texera.computing_unit.id" -> "8",
      "texera.workflow.id" -> "441",
      "texera.execution.id" -> "9001"
    )
  }

  it should "reject non-numeric query-param values (log-forging guard)" in {
    val chain = new CapturingChain
    filter.doFilter(
      new StubReq("/x", params = Map("cuid" -> "abc")),
      null,
      chain
    )
    chain.captured.get("texera.computing_unit.id") shouldBe None
  }

  it should "prefer URL-path match over query-param when both are present" in {
    val chain = new CapturingChain
    filter.doFilter(
      new StubReq("/api/workflow/441", params = Map("wid" -> "999")),
      null,
      chain
    )
    // Path is checked first; param check skips when MDC already set.
    chain.captured("texera.workflow.id") shouldBe "441"
  }

  it should "stay aligned with LogSanitizer's MDC allowlist (otherwise the OTel appender drops them)" in {
    // Every key this filter may push must be allowlisted in
    // LogSanitizer.AllowedMdcKeys or the appender silently strips it
    // and the dashboard sees nothing.
    val filterKeys = Set("texera.workflow.id", "texera.execution.id", "texera.computing_unit.id")
    filterKeys.subsetOf(LogSanitizer.AllowedMdcKeys) shouldBe true
  }
}

/** Local trait that gives the unimplemented HttpServletRequest a
  *  no-op default for every other method — keeps test classes small
  *  without pulling in a mocking library.
  */
private abstract class HttpServletRequestStub extends HttpServletRequest {
  // Unused methods throw — surfaces an unexpected dependency before it
  // silently returns null and produces a flaky test.
  private def stub(): Nothing =
    throw new UnsupportedOperationException(
      "method not stubbed; add to StubReq if a test requires it"
    )
  override def getAuthType: String = stub()
  override def getCookies: Array[javax.servlet.http.Cookie] = stub()
  override def getDateHeader(name: String): Long = stub()
  override def getHeaders(name: String): java.util.Enumeration[String] = stub()
  override def getHeaderNames: java.util.Enumeration[String] = stub()
  override def getIntHeader(name: String): Int = stub()
  override def getMethod: String = "GET"
  override def getPathInfo: String = null
  override def getPathTranslated: String = null
  override def getContextPath: String = ""
  override def getQueryString: String = null
  override def getRemoteUser: String = null
  override def isUserInRole(role: String): Boolean = false
  override def getUserPrincipal: java.security.Principal = null
  override def getRequestedSessionId: String = null
  override def getServletPath: String = ""
  override def getSession(create: Boolean): javax.servlet.http.HttpSession = null
  override def getSession: javax.servlet.http.HttpSession = null
  override def changeSessionId(): String = stub()
  override def isRequestedSessionIdValid: Boolean = false
  override def isRequestedSessionIdFromCookie: Boolean = false
  override def isRequestedSessionIdFromURL: Boolean = false
  override def isRequestedSessionIdFromUrl: Boolean = false
  override def authenticate(response: javax.servlet.http.HttpServletResponse): Boolean = stub()
  override def login(username: String, password: String): Unit = stub()
  override def logout(): Unit = stub()
  override def getParts: java.util.Collection[javax.servlet.http.Part] = stub()
  override def getPart(name: String): javax.servlet.http.Part = stub()
  override def upgrade[T <: javax.servlet.http.HttpUpgradeHandler](handlerClass: Class[T]): T =
    stub()
  override def getRequestURL: StringBuffer = new StringBuffer(getRequestURI)
  override def getAttribute(name: String): AnyRef = null
  override def getAttributeNames: java.util.Enumeration[String] = stub()
  override def getCharacterEncoding: String = null
  override def setCharacterEncoding(env: String): Unit = ()
  override def getContentLength: Int = -1
  override def getContentLengthLong: Long = -1L
  override def getContentType: String = null
  override def getInputStream: javax.servlet.ServletInputStream = stub()
  override def getParameter(name: String): String = null
  override def getParameterNames: java.util.Enumeration[String] = stub()
  override def getParameterValues(name: String): Array[String] = null
  override def getParameterMap: java.util.Map[String, Array[String]] = stub()
  override def getProtocol: String = "HTTP/1.1"
  override def getScheme: String = "http"
  override def getServerName: String = "localhost"
  override def getServerPort: Int = 8080
  override def getReader: java.io.BufferedReader = stub()
  override def getRemoteAddr: String = "127.0.0.1"
  override def getRemoteHost: String = "localhost"
  override def setAttribute(name: String, o: Any): Unit = ()
  override def removeAttribute(name: String): Unit = ()
  override def getLocale: java.util.Locale = java.util.Locale.US
  override def getLocales: java.util.Enumeration[java.util.Locale] = stub()
  override def isSecure: Boolean = false
  override def getRequestDispatcher(path: String): javax.servlet.RequestDispatcher = stub()
  override def getRealPath(path: String): String = path
  override def getRemotePort: Int = 0
  override def getLocalName: String = "localhost"
  override def getLocalAddr: String = "127.0.0.1"
  override def getLocalPort: Int = 8080
  override def getServletContext: javax.servlet.ServletContext = null
  override def startAsync(): javax.servlet.AsyncContext = stub()
  override def startAsync(req: ServletRequest, resp: ServletResponse): javax.servlet.AsyncContext =
    stub()
  override def isAsyncStarted: Boolean = false
  override def isAsyncSupported: Boolean = false
  override def getAsyncContext: javax.servlet.AsyncContext = stub()
  override def getDispatcherType: javax.servlet.DispatcherType =
    javax.servlet.DispatcherType.REQUEST
}
