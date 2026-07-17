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

package org.apache.texera.service.resource

import jakarta.annotation.security.{PermitAll, RolesAllowed}
import jakarta.ws.rs.HttpMethod
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.lang.reflect.Method

// FileService registers RolesAllowedDynamicFeature, so every DatasetResource
// HTTP endpoint is now enforced by its own method-level annotation. A method
// with neither @PermitAll nor @RolesAllowed defaults to OPEN, so the contract
// these tests pin is: every endpoint carries exactly one of the two, the
// non-public ones require REGULAR/ADMIN, and the public hub reads stay
// anonymous-accessible.
class DatasetResourcePermissionsSpec extends AnyFlatSpec with Matchers {

  // A JAX-RS endpoint is any public method carrying an HTTP-method annotation
  // (@GET/@POST/@PUT/@DELETE/...), each of which is meta-annotated @HttpMethod.
  // Scanning by the meta-annotation avoids hard-coding the verb list.
  private val endpointMethods: Seq[Method] =
    classOf[DatasetResource].getMethods.toSeq
      .filter(_.getAnnotations.exists(_.annotationType.isAnnotationPresent(classOf[HttpMethod])))

  private def isPermitAll(m: Method): Boolean = m.getAnnotation(classOf[PermitAll]) != null
  private def rolesOf(m: Method): Option[RolesAllowed] =
    Option(m.getAnnotation(classOf[RolesAllowed]))

  // Public read endpoints for anonymous hub visitors browsing public datasets.
  // These MUST stay @PermitAll or the public hub breaks for logged-out users.
  private val publicEndpointMethods: Set[String] = Set(
    "getPublicPresignedUrl",
    "getPublicPresignedUrlWithS3",
    "getPublicDatasetVersionList",
    "retrievePublicDatasetVersionRootFileNodes",
    "getPublicDataset",
    "getDatasetCover",
    "getDatasetCoverUrl"
  )

  "DatasetResource" should "expose HTTP endpoints (sanity check for the reflection scan)" in {
    endpointMethods should not be empty
  }

  it should "annotate every HTTP endpoint with exactly one of @PermitAll or @RolesAllowed (none defaults to open)" in {
    endpointMethods.foreach { m =>
      withClue(s"${m.getName}: ") {
        (isPermitAll(m), rolesOf(m).isDefined) match {
          case (true, true) => fail("carries both @PermitAll and @RolesAllowed")
          case (false, false) =>
            fail("carries neither @PermitAll nor @RolesAllowed (would default to open)")
          case _ => succeed
        }
      }
    }
  }

  it should "guard every non-public endpoint with @RolesAllowed(REGULAR, ADMIN)" in {
    val nonPublic = endpointMethods.filterNot(m => publicEndpointMethods.contains(m.getName))
    nonPublic should not be empty
    nonPublic.foreach { m =>
      withClue(s"${m.getName}: ") {
        isPermitAll(m) shouldBe false
        val roles = rolesOf(m)
        roles should not be empty
        roles.get.value() should contain theSameElementsAs Array("REGULAR", "ADMIN")
      }
    }
  }

  it should "keep the public dataset endpoints @PermitAll for unauthenticated visitors" in {
    // Every documented public endpoint must still exist (guards against renames
    // that would silently drop a name from the non-public check above).
    val present = endpointMethods.map(_.getName).toSet.intersect(publicEndpointMethods)
    withClue("public endpoints missing from DatasetResource (renamed?): ") {
      present shouldBe publicEndpointMethods
    }
    endpointMethods
      .filter(m => publicEndpointMethods.contains(m.getName))
      .foreach { m =>
        withClue(s"${m.getName} must be @PermitAll: ") {
          isPermitAll(m) shouldBe true
          rolesOf(m) shouldBe empty
        }
      }
  }
}
