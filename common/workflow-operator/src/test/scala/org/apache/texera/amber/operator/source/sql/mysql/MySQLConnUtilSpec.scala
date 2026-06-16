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

package org.apache.texera.amber.operator.source.sql.mysql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfterAll

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.sql.{Connection, Driver, DriverManager, DriverPropertyInfo, SQLException}
import java.util.Properties
import java.util.logging.Logger
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

class MySQLConnUtilSpec extends AnyFlatSpec with BeforeAndAfterAll {

  // ---------------------------------------------------------------------------
  // Strategy — same capturing-driver pattern as PostgreSQLConnUtilSpec.
  // The MySQL driver may or may not be present transitively, so we
  // proactively deregister anything that claims jdbc:mysql: and swap in a
  // capturing driver that records each URL and returns a Proxy-backed
  // Connection so the production code can call `setReadOnly(true)`.
  // ---------------------------------------------------------------------------

  private object CapturingMySQLDriver extends Driver {
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
                case "equals"   => java.lang.Boolean.valueOf(p eq args(0))
                case "hashCode" => java.lang.Integer.valueOf(System.identityHashCode(p))
                case "toString" =>
                  "CapturingMySQLDriver.StubConnection@" + System.identityHashCode(p)
                case "isWrapperFor" => java.lang.Boolean.FALSE
                case "close"        => null
                case _              => null
              }
          }
        )
        .asInstanceOf[Connection]
    }
    override def acceptsURL(url: String): Boolean =
      url != null && url.startsWith("jdbc:mysql:")
    override def getPropertyInfo(url: String, info: Properties): Array[DriverPropertyInfo] =
      Array.empty
    override def getMajorVersion: Int = 1
    override def getMinorVersion: Int = 0
    override def jdbcCompliant(): Boolean = false
    override def getParentLogger: Logger = Logger.getLogger("test-mysql-capturing")
  }

  private val savedRealDrivers: ArrayBuffer[Driver] = ArrayBuffer.empty

  private def safeAcceptsURL(d: Driver, url: String): Boolean =
    try d.acceptsURL(url)
    catch { case _: Throwable => false }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // The probe URL mirrors the exact shape `MySQLConnUtil.connect`
    // constructs (`jdbc:mysql://{host}:{port}/{database}?…`), including
    // the canonical query parameters. A permissive third-party driver
    // that returns `false` on a stripped-down probe but `true` on the
    // real URL would otherwise slip past us.
    try {
      val others = DriverManager.getDrivers.asScala.toList.filter { d =>
        d != CapturingMySQLDriver && safeAcceptsURL(
          d,
          "jdbc:mysql://probe-host:3306/probe-db?autoReconnect=true&useSSL=true"
        )
      }
      others.foreach { d =>
        savedRealDrivers += d
        DriverManager.deregisterDriver(d)
      }
      DriverManager.registerDriver(CapturingMySQLDriver)
    } catch {
      case t: Throwable =>
        savedRealDrivers.foreach { d =>
          try DriverManager.registerDriver(d)
          catch { case _: Throwable => () }
        }
        throw t
    }
  }

  override protected def afterAll(): Unit = {
    try {
      try DriverManager.deregisterDriver(CapturingMySQLDriver)
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
    CapturingMySQLDriver.seenUrls.clear()
    CapturingMySQLDriver.seenProps.clear()
    CapturingMySQLDriver.readOnlyCalls.clear()
  }

  // ---------------------------------------------------------------------------
  // URL composition — host/port/database
  // ---------------------------------------------------------------------------

  "MySQLConnUtil.connect" should
    "build a JDBC URL of the form jdbc:mysql://{host}:{port}/{database}?…" in {
    clearCapture()
    val conn = MySQLConnUtil.connect("host-m", "3306", "db-m", "u", "p")
    assert(conn != null)
    assert(CapturingMySQLDriver.seenUrls.size == 1)
    assert(CapturingMySQLDriver.seenUrls.head.startsWith("jdbc:mysql://host-m:3306/db-m"))
  }

  it should "interpolate distinct host/port/database values into the URL" in {
    clearCapture()
    MySQLConnUtil.connect("host-1", "3306", "db-1", "u", "p")
    assert(CapturingMySQLDriver.seenUrls.head.startsWith("jdbc:mysql://host-1:3306/db-1"))
    clearCapture()
    MySQLConnUtil.connect("host-2", "33060", "db-2", "u", "p")
    assert(CapturingMySQLDriver.seenUrls.head.startsWith("jdbc:mysql://host-2:33060/db-2"))
  }

  it should "place host BEFORE port" in {
    clearCapture()
    MySQLConnUtil.connect("a", "1", "x", "u", "p")
    val url = CapturingMySQLDriver.seenUrls.head
    assert(url.contains("//a:1/"))
    assert(!url.contains("//1:a/"))
  }

  // ---------------------------------------------------------------------------
  // Query parameters — autoReconnect=true and useSSL=true must be present
  // ---------------------------------------------------------------------------

  it should "include the `autoReconnect=true` query parameter" in {
    clearCapture()
    MySQLConnUtil.connect("h", "3306", "db", "u", "p")
    val url = CapturingMySQLDriver.seenUrls.head
    assert(url.contains("autoReconnect=true"), s"URL must include autoReconnect=true, got: $url")
  }

  it should "include the `useSSL=true` query parameter (TLS contract)" in {
    clearCapture()
    MySQLConnUtil.connect("h", "3306", "db", "u", "p")
    val url = CapturingMySQLDriver.seenUrls.head
    assert(url.contains("useSSL=true"), s"URL must include useSSL=true (TLS), got: $url")
  }

  it should "use the canonical `?…&…` separator pattern" in {
    clearCapture()
    MySQLConnUtil.connect("h", "3306", "db", "u", "p")
    val url = CapturingMySQLDriver.seenUrls.head
    assert(
      url == "jdbc:mysql://h:3306/db?autoReconnect=true&useSSL=true",
      s"URL must match canonical pattern, got: $url"
    )
  }

  it should "use the `mysql` JDBC subprotocol (not e.g. `postgresql`)" in {
    clearCapture()
    MySQLConnUtil.connect("h", "3306", "db", "u", "p")
    val url = CapturingMySQLDriver.seenUrls.head
    assert(url.startsWith("jdbc:mysql://"))
    assert(!url.contains("jdbc:postgresql:"))
  }

  // ---------------------------------------------------------------------------
  // Credentials propagation
  // ---------------------------------------------------------------------------

  it should "pass username and password through DriverManager properties" in {
    clearCapture()
    MySQLConnUtil.connect("h", "3306", "db", "the-user", "the-pass")
    val props = CapturingMySQLDriver.seenProps.head
    assert(props.getProperty("user") == "the-user")
    assert(props.getProperty("password") == "the-pass")
  }

  // ---------------------------------------------------------------------------
  // setReadOnly(true) — pinned via the captured proxy (parity with PG spec)
  // ---------------------------------------------------------------------------

  it should "flip the returned Connection to read-only (query-efficiency contract)" in {
    clearCapture()
    MySQLConnUtil.connect("h", "3306", "db", "u", "p")
    assert(CapturingMySQLDriver.readOnlyCalls == ArrayBuffer(true))
  }

  // ---------------------------------------------------------------------------
  // SQLException propagation when the driver throws
  // ---------------------------------------------------------------------------

  it should "propagate SQLException when the driver throws" in {
    val throwingDriver = new Driver {
      override def acceptsURL(url: String): Boolean =
        url != null && url.startsWith("jdbc:mysql:")
      // Follow the JDBC contract: return `null` if the URL isn't ours
      // and throw only on a matching URL — keeps the helper from
      // interfering with `DriverManager.getConnection` calls for any
      // other scheme that might happen during the suite.
      override def connect(url: String, info: Properties): Connection = {
        if (!acceptsURL(url)) return null
        throw new SQLException("forced-fail-for-test")
      }
      override def getPropertyInfo(url: String, info: Properties) =
        Array.empty[DriverPropertyInfo]
      override def getMajorVersion: Int = 99
      override def getMinorVersion: Int = 0
      override def jdbcCompliant(): Boolean = false
      override def getParentLogger: Logger = Logger.getLogger("test-mysql-throwing")
    }
    DriverManager.deregisterDriver(CapturingMySQLDriver)
    DriverManager.registerDriver(throwingDriver)
    try {
      val ex = intercept[SQLException] {
        MySQLConnUtil.connect("h", "3306", "db", "u", "p")
      }
      assert(ex.getMessage.contains("forced-fail-for-test"))
    } finally {
      DriverManager.deregisterDriver(throwingDriver)
      DriverManager.registerDriver(CapturingMySQLDriver)
    }
  }
}
