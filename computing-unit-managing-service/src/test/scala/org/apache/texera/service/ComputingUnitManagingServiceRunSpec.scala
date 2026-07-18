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
import io.dropwizard.core.setup.{Bootstrap, Environment}
import io.dropwizard.jersey.DropwizardResourceConfig
import io.dropwizard.jersey.setup.JerseyEnvironment
import io.dropwizard.jetty.MutableServletContextHandler
import org.apache.texera.auth.RoleAnnotationEnforcer
import org.apache.texera.service.resource.{
  ComputingUnitAccessResource,
  ComputingUnitManagingResource,
  HealthCheckResource
}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.control.NonFatal

class ComputingUnitManagingServiceRunSpec extends AnyFlatSpec with Matchers {

  // Every endpoint this service registers declares @RolesAllowed/@PermitAll/@DenyAll.
  "ComputingUnitManagingService's registered resources" should "all declare access control" in {
    RoleAnnotationEnforcer.findUnannotatedEndpoints(
      Seq(
        classOf[ComputingUnitManagingResource],
        classOf[ComputingUnitAccessResource],
        classOf[HealthCheckResource]
      )
    ) shouldBe empty
  }

  "ComputingUnitManagingService.initialize" should "run the shared bootstrap configuration" in {
    val bootstrap = mock(classOf[Bootstrap[ComputingUnitManagingServiceConfiguration]])
    when(bootstrap.getObjectMapper).thenReturn(mock(classOf[ObjectMapper]))
    when(bootstrap.getConfigurationSourceProvider)
      .thenReturn(mock(classOf[ConfigurationSourceProvider]))

    new ComputingUnitManagingService().initialize(bootstrap)

    verify(bootstrap).setConfigurationSourceProvider(any(classOf[ConfigurationSourceProvider]))
  }

  "ComputingUnitManagingService.run" should "serve the API and register the health check" in {
    val jersey = mock(classOf[JerseyEnvironment])
    val context = mock(classOf[MutableServletContextHandler])
    val env = mock(classOf[Environment])
    when(env.jersey).thenReturn(jersey)
    when(env.getApplicationContext).thenReturn(context)
    when(jersey.getResourceConfig).thenReturn(DropwizardResourceConfig.forTesting())

    // run() opens the SQL pool and constructs a Kubernetes-backed resource partway
    // through; those steps need a live database / cluster that a bare unit run lacks,
    // so tolerate a failure after the HTTP wiring asserted below.
    try new ComputingUnitManagingService()
      .run(mock(classOf[ComputingUnitManagingServiceConfiguration]), env)
    catch { case NonFatal(_) => }

    verify(jersey).setUrlPattern("/api/*")
    verify(jersey).register(classOf[HealthCheckResource])
  }
}
