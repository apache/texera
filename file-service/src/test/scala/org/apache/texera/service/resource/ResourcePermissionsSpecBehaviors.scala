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
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.lang.reflect.Method

/**
  * Reusable reflection-based test behaviors for verifying JAX-RS resource endpoint permissions.
  *
  * FileService registers RolesAllowedDynamicFeature, so every HTTP endpoint must carry method-level
  * annotations. Unannotated methods default to OPEN. These behaviors pin the security contract:
  * 1. Every HTTP endpoint carries exactly one of @PermitAll or @RolesAllowed.
  * 2. Non-public endpoints require REGULAR and ADMIN roles.
  * 3. Public read endpoints carry @PermitAll.
  */
trait ResourcePermissionsSpecBehaviors extends AnyFlatSpecLike with Matchers {

  def endpointsOf(cls: Class[_]): Seq[Method] =
    cls.getMethods.toSeq
      .filter(_.getAnnotations.exists(_.annotationType.isAnnotationPresent(classOf[HttpMethod])))

  def isPermitAll(m: Method): Boolean = m.getAnnotation(classOf[PermitAll]) != null

  def rolesOf(m: Method): Option[RolesAllowed] =
    Option(m.getAnnotation(classOf[RolesAllowed]))

  protected def resourcePermissions(
      resourceClass: Class[_],
      publicEndpointMethods: Set[String],
      publicTestDescription: String,
      requireExactPublicSet: Boolean = false
  ): Unit = {
    val resourceName = resourceClass.getSimpleName
    val endpointMethods = endpointsOf(resourceClass)

    resourceName should "expose HTTP endpoints (sanity check for the reflection scan)" in {
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

    it should publicTestDescription in {
      val present = endpointMethods.map(_.getName).toSet.intersect(publicEndpointMethods)
      withClue(s"public endpoints missing from $resourceName (renamed?): ") {
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
      if (requireExactPublicSet) {
        endpointMethods.filter(isPermitAll).map(_.getName).toSet shouldBe publicEndpointMethods
      }
    }
  }
}
