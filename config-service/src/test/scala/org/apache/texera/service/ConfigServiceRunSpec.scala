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

package org.apache.texera.service

import com.fasterxml.jackson.databind.ObjectMapper
import io.dropwizard.configuration.ConfigurationSourceProvider
import io.dropwizard.core.setup.Bootstrap
import org.apache.texera.auth.RoleAnnotationEnforcer
import org.apache.texera.service.resource.{ConfigResource, HealthCheckResource}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.control.NonFatal

class ConfigServiceRunSpec extends AnyFlatSpec with Matchers {

  // Every endpoint this service registers declares @RolesAllowed/@PermitAll/@DenyAll.
  "ConfigService's registered resources" should "all declare access control" in {
    RoleAnnotationEnforcer.findUnannotatedEndpoints(
      Seq(classOf[ConfigResource], classOf[HealthCheckResource])
    ) shouldBe empty
  }

  "ConfigService.initialize" should "run the shared bootstrap configuration and database setup" in {
    val bootstrap = mock(classOf[Bootstrap[ConfigServiceConfiguration]])
    when(bootstrap.getObjectMapper).thenReturn(mock(classOf[ObjectMapper]))
    when(bootstrap.getConfigurationSourceProvider)
      .thenReturn(mock(classOf[ConfigurationSourceProvider]))

    // initialize() also opens the SQL pool, which needs a live database that a bare unit
    // run lacks, so tolerate a failure after the config wiring asserted below.
    try new ConfigService().initialize(bootstrap)
    catch { case NonFatal(_) => }

    verify(bootstrap).setConfigurationSourceProvider(any(classOf[ConfigurationSourceProvider]))
  }
}
