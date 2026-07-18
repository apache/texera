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
import io.dropwizard.jetty.MutableServletContextHandler
import io.dropwizard.jetty.setup.ServletEnvironment
import org.apache.texera.auth.RoleAnnotationEnforcer
import org.apache.texera.common.config.StorageConfig
import org.apache.texera.service.resource.{
  AdminComputingUnitResource,
  ComputingUnitAccessResource,
  ComputingUnitManagingResource,
  HealthCheckResource
}
import org.mockito.ArgumentMatchers.isA
import org.mockito.Mockito.{mock, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.DriverManager

class ComputingUnitManagingServiceRunSpec extends AnyFlatSpec with Matchers {

  // run() opens the real HikariCP pool via SqlServer.initConnection, so it is only
  // exercisable where Postgres is provisioned at the configured JDBC URL (as in CI).
  private def databaseReachable: Boolean =
    try {
      DriverManager
        .getConnection(
          StorageConfig.jdbcUrl,
          StorageConfig.jdbcUsername,
          StorageConfig.jdbcPassword
        )
        .close()
      true
    } catch {
      case _: Throwable => false
    }

  // Every endpoint this service registers declares @RolesAllowed/@PermitAll/@DenyAll.
  "ComputingUnitManagingService's registered resources" should "all declare access control" in {
    RoleAnnotationEnforcer.findUnannotatedEndpoints(
      Seq(
        classOf[ComputingUnitManagingResource],
        classOf[ComputingUnitAccessResource],
        classOf[AdminComputingUnitResource],
        classOf[HealthCheckResource]
      )
    ) shouldBe empty
  }

  "ComputingUnitManagingService.run" should "register the admin resource on the Jersey environment" in {
    val jersey = mock(classOf[JerseyEnvironment])
    val servlets = mock(classOf[ServletEnvironment])
    val context = mock(classOf[MutableServletContextHandler])
    val env = mock(classOf[Environment])
    when(env.jersey).thenReturn(jersey)
    when(env.servlets).thenReturn(servlets)
    when(env.getApplicationContext).thenReturn(context)
    when(jersey.getResourceConfig).thenReturn(DropwizardResourceConfig.forTesting())

    assume(
      databaseReachable,
      "run() requires a reachable Postgres at the configured JDBC URL (provided in CI)"
    )
    new ComputingUnitManagingService()
      .run(mock(classOf[ComputingUnitManagingServiceConfiguration]), env)

    verify(jersey).register(isA(classOf[ComputingUnitManagingResource]))
    verify(jersey).register(isA(classOf[ComputingUnitAccessResource]))
    verify(jersey).register(isA(classOf[AdminComputingUnitResource]))
    verify(jersey).setUrlPattern("/api/*")
  }
}
