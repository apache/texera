/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.texera.service.resource

import jakarta.annotation.security.PermitAll
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.{GET, Path, Produces}
import org.scalatest.flatspec.AnyFlatSpec

class HealthCheckResourceSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Response payload
  // ---------------------------------------------------------------------------

  "HealthCheckResource.healthCheck" should "report status ok" in {
    assert(new HealthCheckResource().healthCheck == Map("status" -> "ok"))
  }

  // ---------------------------------------------------------------------------
  // JAX-RS contract: this resource is shared across every service, so the
  // annotations are part of its public contract and are worth pinning down.
  // ---------------------------------------------------------------------------

  it should "be mounted at /healthcheck" in {
    assert(classOf[HealthCheckResource].getAnnotation(classOf[Path]).value() == "/healthcheck")
  }

  it should "be accessible without authentication (@PermitAll)" in {
    assert(classOf[HealthCheckResource].getAnnotation(classOf[PermitAll]) != null)
  }

  it should "produce application/json" in {
    val produces = classOf[HealthCheckResource].getAnnotation(classOf[Produces])
    assert(produces != null)
    assert(produces.value().contains(MediaType.APPLICATION_JSON))
  }

  it should "expose healthCheck as an HTTP GET" in {
    assert(
      classOf[HealthCheckResource].getMethod("healthCheck").getAnnotation(classOf[GET]) != null
    )
  }
}
