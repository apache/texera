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

import io.dropwizard.core.setup.Environment
import io.dropwizard.jersey.DropwizardResourceConfig
import io.dropwizard.jersey.setup.JerseyEnvironment
import org.apache.texera.auth.RoleAnnotationEnforcer
import org.apache.texera.service.resource.{
  DatasetAccessResource,
  DatasetResource,
  HealthCheckResource
}
import org.mockito.Mockito.{mock, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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

  // Runs the enforcer over the Jersey resource config; a default config has no holes.
  "FileService.enforceRoleAnnotations" should "pass for a resource config with no unannotated endpoints" in {
    val jersey = mock(classOf[JerseyEnvironment])
    val env = mock(classOf[Environment])
    when(env.jersey).thenReturn(jersey)
    when(jersey.getResourceConfig).thenReturn(DropwizardResourceConfig.forTesting())

    noException should be thrownBy FileService.enforceRoleAnnotations(env)
  }
}
