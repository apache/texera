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

package org.apache.texera.amber.operator.source.sql.postgresql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfterAll

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.sql.{Connection, Driver, DriverManager, DriverPropertyInfo, SQLException}
import java.util.Properties
import java.util.logging.Logger
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

class PostgreSQLConnUtilSpec extends AnyFlatSpec with BeforeAndAfterAll {

  // ---------------------------------------------------------------------------
  // Strategy — pin the JDBC URL composition (the only application-logic in
  // this util) without a real DB.
  //
  // The workflow-operator test classpath DOES include the real PostgreSQL
  // driver (transitively), and that driver eats `jdbc:postgresql:` URLs
  // before returning a generic "The connection attempt failed." exception.
  // So we can't rely on `DriverManager.getConnection`'s default
  // "No suitable driver" message.
  //
  // Instead, we deregister every driver claiming `jdbc:postgresql:`,
  // register a capturing driver that records each URL it is asked to open
  // (and returns a Proxy-backed Connection so the production code can call
  // `setReadOnly`), run the assertions, then restore the real drivers
  // in afterAll.
  // ---------------------------------------------------------------------------

  private object CapturingPGDriver extends Driver {
    val seenUrls: ArrayBuffer[String] = ArrayBuffer.empty
    val seenProps: ArrayBuffer[Properties] = ArrayBuffer.empty
    val readOnlyCalls: ArrayBuffer[Boolean] = ArrayBuffer.empty

    override def connect(url: String, info: Properties): Connection = {
      if (!acceptsURL(url)) return null
      seenUrls += url
      seenProps += info
      Proxy
        .newProxyInstance(
          getClass.getClassLoader,
          Array(classOf[Connection]),
          new InvocationHandler {
            override def invoke(p: Any, m: Method, args: Array[AnyRef]): AnyRef =
              m.getName match {
                case "setReadOnly" =>
                  readOnlyCalls += args(0).asInstanceOf[java.lang.Boolean].booleanValue()
                  null
                // Object methods — required so `conn != null`, `conn.toString`,
                // and identity HashMap-keying work without NPE on auto-unboxing.
                case "equals"       => java.lang.Boolean.valueOf(p eq args(0))
                case "hashCode"     => java.lang.Integer.valueOf(System.identityHashCode(p))
                case "toString"     => "CapturingPGDriver.StubConnection@" + System.identityHashCode(p)
                case "isWrapperFor" => java.lang.Boolean.FALSE
                case "close"        => null
                case _              => null
              }
          }
        )
        .asInstanceOf[Connection]
    }
    override def acceptsURL(url: String): Boolean =
      url != null && url.startsWith("jdbc:postgresql:")
    override def getPropertyInfo(url: String, info: Properties): Array[DriverPropertyInfo] =
      Array.empty
    override def getMajorVersion: Int = 1
    override def getMinorVersion: Int = 0
    override def jdbcCompliant(): Boolean = false
    override def getParentLogger: Logger = Logger.getLogger("test-pg-capturing")
  }

  // Snapshot the real PG drivers so afterAll can restore them.
  private val savedRealDrivers: List[Driver] = ArrayBuffer.empty[Driver].toList

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // Remove every other driver that accepts jdbc:postgresql: so our
    // capturing driver is the only one DriverManager.getConnection sees.
    val others = DriverManager.getDrivers.asScala.toList.filter { d =>
      d != CapturingPGDriver && d.acceptsURL("jdbc:postgresql://h/d")
    }
    others.foreach { d =>
      savedRealDriversBuffer += d
      DriverManager.deregisterDriver(d)
    }
    DriverManager.registerDriver(CapturingPGDriver)
  }

  override protected def afterAll(): Unit = {
    try {
      DriverManager.deregisterDriver(CapturingPGDriver)
      savedRealDriversBuffer.foreach(DriverManager.registerDriver)
    } finally {
      super.afterAll()
    }
  }

  private val savedRealDriversBuffer: ArrayBuffer[Driver] = ArrayBuffer.empty

  private def clearCapture(): Unit = {
    CapturingPGDriver.seenUrls.clear()
    CapturingPGDriver.seenProps.clear()
    CapturingPGDriver.readOnlyCalls.clear()
  }

  // ---------------------------------------------------------------------------
  // URL composition — pin the exact JDBC URL the driver receives
  // ---------------------------------------------------------------------------

  "PostgreSQLConnUtil.connect" should
    "build a JDBC URL of the form jdbc:postgresql://{host}:{port}/{database}" in {
    clearCapture()
    val conn = PostgreSQLConnUtil.connect("host-a", "5432", "db-a", "u", "p")
    assert(conn != null)
    assert(CapturingPGDriver.seenUrls.size == 1)
    assert(CapturingPGDriver.seenUrls.head == "jdbc:postgresql://host-a:5432/db-a")
  }

  it should "interpolate distinct host/port/database values into the URL" in {
    clearCapture()
    PostgreSQLConnUtil.connect("h-1", "1234", "d-1", "u", "p")
    assert(CapturingPGDriver.seenUrls.head == "jdbc:postgresql://h-1:1234/d-1")
    clearCapture()
    PostgreSQLConnUtil.connect("h-2", "9999", "d-2", "u", "p")
    assert(CapturingPGDriver.seenUrls.head == "jdbc:postgresql://h-2:9999/d-2")
  }

  it should "place host BEFORE port (host-then-port, not port-then-host)" in {
    clearCapture()
    PostgreSQLConnUtil.connect("a", "1", "x", "u", "p")
    val url = CapturingPGDriver.seenUrls.head
    assert(url.contains("//a:1/"), s"expected //a:1/ ordering, got: $url")
    assert(!url.contains("//1:a/"), s"port-then-host ordering must NOT appear, got: $url")
  }

  it should "use the `postgresql` JDBC subprotocol (not e.g. `mysql`)" in {
    clearCapture()
    PostgreSQLConnUtil.connect("h", "5432", "db", "u", "p")
    val url = CapturingPGDriver.seenUrls.head
    assert(url.startsWith("jdbc:postgresql://"))
    assert(!url.contains("jdbc:mysql:"))
  }

  it should "accept an empty database name and still produce a well-formed URL" in {
    clearCapture()
    PostgreSQLConnUtil.connect("h", "5432", "", "u", "p")
    // The resulting `jdbc:postgresql://h:5432/` is well-formed even if a
    // real driver would reject it.
    assert(CapturingPGDriver.seenUrls.head == "jdbc:postgresql://h:5432/")
  }

  // ---------------------------------------------------------------------------
  // Credentials propagation
  // ---------------------------------------------------------------------------

  it should "pass username and password through DriverManager properties" in {
    clearCapture()
    PostgreSQLConnUtil.connect("h", "5432", "db", "the-user", "the-pass")
    val props = CapturingPGDriver.seenProps.head
    assert(props.getProperty("user") == "the-user")
    assert(props.getProperty("password") == "the-pass")
  }

  // ---------------------------------------------------------------------------
  // setReadOnly(true) — pinned via the captured proxy
  // ---------------------------------------------------------------------------

  it should "flip the returned Connection to read-only (query-efficiency contract)" in {
    clearCapture()
    PostgreSQLConnUtil.connect("h", "5432", "db", "u", "p")
    assert(CapturingPGDriver.readOnlyCalls == ArrayBuffer(true))
  }

  // ---------------------------------------------------------------------------
  // SQLException propagation when the driver throws — pin the @throws contract
  // ---------------------------------------------------------------------------

  it should "propagate SQLException when the driver throws" in {
    // Swap in a one-shot throwing override of `connect`. We can't mutate
    // CapturingPGDriver in-place, so register a higher-priority throwing
    // driver and remove it after.
    val throwingDriver = new Driver {
      override def connect(url: String, info: Properties): Connection =
        throw new SQLException(s"forced-fail-for-test")
      override def acceptsURL(url: String): Boolean =
        url != null && url.startsWith("jdbc:postgresql:")
      override def getPropertyInfo(url: String, info: Properties) = Array.empty[DriverPropertyInfo]
      override def getMajorVersion: Int = 99
      override def getMinorVersion: Int = 0
      override def jdbcCompliant(): Boolean = false
      override def getParentLogger: Logger = Logger.getLogger("test-pg-throwing")
    }
    DriverManager.deregisterDriver(CapturingPGDriver)
    DriverManager.registerDriver(throwingDriver)
    try {
      val ex = intercept[SQLException] {
        PostgreSQLConnUtil.connect("h", "5432", "db", "u", "p")
      }
      assert(ex.getMessage.contains("forced-fail-for-test"))
    } finally {
      DriverManager.deregisterDriver(throwingDriver)
      DriverManager.registerDriver(CapturingPGDriver)
    }
  }
}
