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

  // Snapshot of real PG drivers temporarily deregistered in beforeAll.
  // Restored in afterAll so other suites are not left with a broken
  // JDBC driver registry.
  private val savedRealDrivers: ArrayBuffer[Driver] = ArrayBuffer.empty

  /** `acceptsURL` is declared `throws SQLException`; treat any throw as
    * "this driver doesn't claim our scheme" so a flaky third-party driver
    * cannot abort the whole suite.
    */
  private def safeAcceptsURL(d: Driver, url: String): Boolean =
    try d.acceptsURL(url)
    catch { case _: Throwable => false }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // Remove every other driver that claims jdbc:postgresql: so our
    // capturing driver is the only one DriverManager.getConnection sees.
    // The probe URL mirrors the exact shape `PostgreSQLConnUtil.connect`
    // constructs (`jdbc:postgresql://{host}:{port}/{database}`) so a
    // permissive third-party driver that returns `false` on a stripped-
    // down probe but `true` on the real URL can't slip past us.
    //
    // Wrapped in try/catch so that if any deregistration / registration
    // step throws, we restore whatever we already deregistered before
    // failing the suite — the alternative leaves the JVM's JDBC registry
    // in an inconsistent state for the rest of the test run.
    try {
      val others = DriverManager.getDrivers.asScala.toList.filter { d =>
        d != CapturingPGDriver && safeAcceptsURL(d, "jdbc:postgresql://probe-host:5432/probe-db")
      }
      others.foreach { d =>
        savedRealDrivers += d
        DriverManager.deregisterDriver(d)
      }
      DriverManager.registerDriver(CapturingPGDriver)
    } catch {
      case t: Throwable =>
        // Best-effort restore before re-throwing.
        savedRealDrivers.foreach { d =>
          try DriverManager.registerDriver(d)
          catch { case _: Throwable => () }
        }
        throw t
    }
  }

  override protected def afterAll(): Unit = {
    try {
      try DriverManager.deregisterDriver(CapturingPGDriver)
      catch { case _: Throwable => () }
      savedRealDrivers.foreach { d =>
        try DriverManager.registerDriver(d)
        catch { case _: Throwable => () }
      }
    } finally {
      super.afterAll()
    }
  }

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
      override def acceptsURL(url: String): Boolean =
        url != null && url.startsWith("jdbc:postgresql:")
      // Follow the JDBC contract: return `null` if the URL is not ours,
      // then throw only on a matching URL. A future refactor that calls
      // `DriverManager.getConnection` with a different scheme while
      // this driver is registered would otherwise see a spurious throw.
      override def connect(url: String, info: Properties): Connection = {
        if (!acceptsURL(url)) return null
        throw new SQLException("forced-fail-for-test")
      }
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
