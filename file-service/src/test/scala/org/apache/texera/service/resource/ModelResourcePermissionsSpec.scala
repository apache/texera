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

import jakarta.annotation.security.RolesAllowed
import org.scalatest.flatspec.AnyFlatSpec

// FileService registers RolesAllowedDynamicFeature, so each endpoint is enforced by its own method-level
// annotation, and one carrying neither @PermitAll nor @RolesAllowed defaults to
// OPEN. These tests pin that contract: every endpoint carries exactly one of the
// two, the non-public ones require REGULAR/ADMIN, and only the public reads stay
// anonymous-accessible.
class ModelResourcePermissionsSpec extends AnyFlatSpec with ResourcePermissionsSpecBehaviors {

  // Anonymous-readable, mirroring the dataset side. The cover reads do their own
  // public/grant check, so a private model's cover is still refused.
  private val publicEndpointMethods: Set[String] = Set(
    "getPublicModel",
    "getPublicPresignedUrl",
    "getPublicPresignedUrlWithS3",
    "getPublicModelVersionList",
    "retrievePublicModelVersionRootFileNodes",
    "getModelCover",
    "getModelCoverUrl"
  )

  resourcePermissions(
    classOf[ModelResource],
    publicEndpointMethods,
    publicTestDescription = "keep the public model endpoints @PermitAll, and them alone",
    requireExactPublicSet = true
  )

  "ModelAccessResource" should "expose no anonymous endpoints" in {
    val accessEndpoints = endpointsOf(classOf[ModelAccessResource])
    accessEndpoints should not be empty
    Option(classOf[ModelAccessResource].getAnnotation(classOf[RolesAllowed])) should not be empty
    accessEndpoints.foreach { m =>
      withClue(s"${m.getName}: ") { isPermitAll(m) shouldBe false }
    }
  }
}
