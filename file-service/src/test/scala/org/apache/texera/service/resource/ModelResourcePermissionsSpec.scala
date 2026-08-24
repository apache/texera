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

// FileService registers RolesAllowedDynamicFeature, so each endpoint is enforced by its own method-level
// annotation, and one carrying neither @PermitAll nor @RolesAllowed defaults to
// OPEN. These tests pin that contract: every endpoint carries exactly one of the
// two, the non-public ones require REGULAR/ADMIN, and only the public reads stay
// anonymous-accessible.
class ModelResourcePermissionsSpec extends AnyFlatSpec with Matchers {

  private def endpointsOf(cls: Class[_]): Seq[Method] =
    cls.getMethods.toSeq
      .filter(_.getAnnotations.exists(_.annotationType.isAnnotationPresent(classOf[HttpMethod])))

  private val endpointMethods: Seq[Method] = endpointsOf(classOf[ModelResource])

  private def isPermitAll(m: Method): Boolean = m.getAnnotation(classOf[PermitAll]) != null
  private def rolesOf(m: Method): Option[RolesAllowed] =
    Option(m.getAnnotation(classOf[RolesAllowed]))

  // Anonymous-readable, mirroring the dataset side: the public model read, the two
  // presign routes a public model's files are fetched through, the version-browsing
  // pair a logged-out visitor needs, and the two cover reads (which do their own
  // public/grant check, so a private model's cover is still refused).
  private val publicEndpointMethods: Set[String] = Set(
    "getPublicModel",
    "getPublicPresignedUrl",
    "getPublicPresignedUrlWithS3",
    "getPublicModelVersionList",
    "retrievePublicModelVersionRootFileNodes",
    "getModelCover",
    "getModelCoverUrl"
  )

  "ModelResource" should "expose HTTP endpoints (sanity check for the reflection scan)" in {
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

  it should "keep the public model endpoints @PermitAll, and them alone" in {
    val present = endpointMethods.map(_.getName).toSet.intersect(publicEndpointMethods)
    withClue("public endpoints missing from ModelResource (renamed?): ") {
      present shouldBe publicEndpointMethods
    }
    endpointMethods.filter(isPermitAll).map(_.getName).toSet shouldBe publicEndpointMethods
  }

  "ModelAccessResource" should "expose no anonymous endpoints" in {
    val accessEndpoints = endpointsOf(classOf[ModelAccessResource])
    accessEndpoints should not be empty
    Option(classOf[ModelAccessResource].getAnnotation(classOf[RolesAllowed])) should not be empty
    accessEndpoints.foreach { m =>
      withClue(s"${m.getName}: ") { isPermitAll(m) shouldBe false }
    }
  }
}
