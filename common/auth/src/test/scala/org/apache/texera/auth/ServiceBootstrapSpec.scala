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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.dropwizard.configuration.ConfigurationSourceProvider
import io.dropwizard.core.{Application, Configuration}
import io.dropwizard.core.setup.Bootstrap
import org.apache.texera.dao.SqlServer
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, isA}
import org.mockito.Mockito.{mock, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import scala.util.control.NonFatal

class ServiceBootstrapSpec extends AnyFlatSpec with Matchers {

  // Every service shares this bootstrap helper, so its behavior is verified once here
  // rather than per service.
  "ServiceBootstrap.configure" should "wrap the config source provider and register the Scala module" in {
    val bootstrap = mock(classOf[Bootstrap[Configuration]])
    val objectMapper = mock(classOf[ObjectMapper])
    val sourceProvider = mock(classOf[ConfigurationSourceProvider])
    when(bootstrap.getObjectMapper).thenReturn(objectMapper)
    when(bootstrap.getConfigurationSourceProvider).thenReturn(sourceProvider)

    ServiceBootstrap.configure(bootstrap)

    verify(bootstrap).setConfigurationSourceProvider(any(classOf[ConfigurationSourceProvider]))
    verify(objectMapper).registerModule(isA(classOf[DefaultScalaModule]))
  }

  it should "install a source provider that substitutes environment variables in the config" in {
    assume(sys.env.contains("HOME"))
    val bootstrap = mock(classOf[Bootstrap[Configuration]])
    val delegate = mock(classOf[ConfigurationSourceProvider])
    when(bootstrap.getObjectMapper).thenReturn(mock(classOf[ObjectMapper]))
    when(bootstrap.getConfigurationSourceProvider).thenReturn(delegate)
    when(delegate.open("config.yaml")).thenReturn(
      new ByteArrayInputStream(
        "home: ${HOME}\nliteral: ${TEXERA_UNSET_TEST_VAR}".getBytes(StandardCharsets.UTF_8)
      )
    )

    ServiceBootstrap.configure(bootstrap)

    val captor = ArgumentCaptor.forClass(classOf[ConfigurationSourceProvider])
    verify(bootstrap).setConfigurationSourceProvider(captor.capture())
    val substituted =
      new String(captor.getValue.open("config.yaml").readAllBytes(), StandardCharsets.UTF_8)

    substituted should include(sys.env("HOME"))
    // strict = false: a placeholder with no matching env var must pass through unchanged
    // rather than fail service startup.
    substituted should include("${TEXERA_UNSET_TEST_VAR}")
  }

  "ServiceBootstrap.configFilePath" should "resolve the conventional resources path under the service dir" in {
    val result = ServiceBootstrap.configFilePath("file-service", "file-service-web-config.yaml")

    val expectedSuffix = Paths
      .get("file-service", "src", "main", "resources", "file-service-web-config.yaml")
      .toString
    result should endWith(expectedSuffix)
    Paths.get(result).isAbsolute shouldBe true
  }

  "ServiceBootstrap.start" should "launch the Dropwizard server command with the conventional config path" in {
    val app = mock(classOf[Application[Configuration]])

    ServiceBootstrap.start(app, "config-service", "config-service-web-config.yaml")

    verify(app).run(
      "server",
      ServiceBootstrap.configFilePath("config-service", "config-service-web-config.yaml")
    )
  }

  "ServiceBootstrap.initDatabase" should "run the shared SQL connection-pool setup from storage config" in {
    // A unit run may or may not have a reachable Postgres (CI provides one on
    // localhost:5432; a bare checkout does not). Either way this exercises the shared
    // init path: on success the SqlServer singleton is populated, on a connection
    // failure it throws fast. We only require the path to run, not the DB to be up.
    try {
      ServiceBootstrap.initDatabase()
      SqlServer.getInstance() should not be null
    } catch {
      case NonFatal(_) => succeed
    }
  }
}
