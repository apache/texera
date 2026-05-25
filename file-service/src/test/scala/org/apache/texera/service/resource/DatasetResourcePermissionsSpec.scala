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

import jakarta.annotation.security.PermitAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DatasetResourcePermissionsSpec extends AnyFlatSpec with Matchers {

  // Once RolesAllowedDynamicFeature is registered in FileService, every method
  // on DatasetResource is enforced. These endpoints serve unauthenticated hub
  // visitors browsing public datasets — they must remain reachable without a JWT.
  private val publicEndpointMethods = Seq(
    "getPublicPresignedUrl",
    "getPublicPresignedUrlWithS3",
    "getPublicDatasetVersionList",
    "retrievePublicDatasetVersionRootFileNodes",
    "getPublicDataset",
    "getDatasetCover"
  )

  publicEndpointMethods.foreach { methodName =>
    s"DatasetResource.$methodName" should "be @PermitAll so unauthenticated visitors can hit it" in {
      val methods = classOf[DatasetResource].getMethods.filter(_.getName == methodName)
      methods should not be empty
      methods.foreach { m =>
        withClue(s"method $methodName missing @PermitAll: ") {
          m.getAnnotation(classOf[PermitAll]) should not be null
        }
      }
    }
  }
}
