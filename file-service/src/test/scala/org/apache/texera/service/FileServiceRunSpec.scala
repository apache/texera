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
import io.dropwizard.jetty.setup.ServletEnvironment
import io.dropwizard.lifecycle.setup.LifecycleEnvironment
import org.apache.texera.auth.RoleAnnotationEnforcer
import org.apache.texera.service.resource.{
  DatasetAccessResource,
  DatasetResource,
  HealthCheckResource
}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.control.NonFatal

class FileServiceRunSpec extends AnyFlatSpec with Matchers {

  // Every endpoint this service registers declares @RolesAllowed/@PermitAll/@DenyAll.
  "FileService's registered resources" should "all declare access control" in {
    RoleAnnotationEnforcer.findUnannotatedEndpoints(
      Seq(
        classOf[DatasetResource],
        classOf[DatasetAccessResource],
        classOf[HealthCheckResource]
      )
    ) shouldBe empty
  }

  "FileService.initialize" should "run the shared bootstrap and register the dataset serializer module" in {
    val bootstrap = mock(classOf[Bootstrap[FileServiceConfiguration]])
    val objectMapper = mock(classOf[ObjectMapper])
    when(bootstrap.getObjectMapper).thenReturn(objectMapper)
    when(bootstrap.getConfigurationSourceProvider)
      .thenReturn(mock(classOf[ConfigurationSourceProvider]))

    new FileService().initialize(bootstrap)

    verify(bootstrap).setConfigurationSourceProvider(any(classOf[ConfigurationSourceProvider]))
    // Scala module (via ServiceBootstrap.configure) + the DatasetFileNode serializer module.
    verify(objectMapper, org.mockito.Mockito.atLeastOnce()).registerModule(any())
  }

  "FileService.run" should "serve the API before opening the storage dependencies" in {
    val jersey = mock(classOf[JerseyEnvironment])
    val servlets = mock(classOf[ServletEnvironment])
    val lifecycle = mock(classOf[LifecycleEnvironment])
    val context = mock(classOf[MutableServletContextHandler])
    val env = mock(classOf[Environment])
    when(env.jersey).thenReturn(jersey)
    when(env.servlets).thenReturn(servlets)
    when(env.lifecycle).thenReturn(lifecycle)
    when(env.getApplicationContext).thenReturn(context)
    when(jersey.getResourceConfig).thenReturn(DropwizardResourceConfig.forTesting())

    // run() opens the SQL pool and waits on the S3/LakeFS object stores, which need
    // live backends that a bare unit run lacks, so tolerate a failure after the
    // HTTP wiring asserted below.
    try new FileService().run(mock(classOf[FileServiceConfiguration]), env)
    catch { case NonFatal(_) => }

    verify(jersey).setUrlPattern("/api/*")
  }
}
