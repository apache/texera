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

import org.scalatest.flatspec.AnyFlatSpec

// FileService registers RolesAllowedDynamicFeature, so every DatasetResource
// HTTP endpoint is now enforced by its own method-level annotation. A method
// with neither @PermitAll nor @RolesAllowed defaults to OPEN, so the contract
// these tests pin is: every endpoint carries exactly one of the two, the
// non-public ones require REGULAR/ADMIN, and the public hub reads stay
// anonymous-accessible.
class DatasetResourcePermissionsSpec extends AnyFlatSpec with ResourcePermissionsSpecBehaviors {

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

  resourcePermissions(
    classOf[DatasetResource],
    publicEndpointMethods,
    publicTestDescription =
      "keep the public dataset endpoints @PermitAll for unauthenticated visitors"
  )
}
