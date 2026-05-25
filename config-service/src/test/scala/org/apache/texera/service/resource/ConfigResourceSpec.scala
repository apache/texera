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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConfigResourceSpec extends AnyFlatSpec with Matchers {

  // Both endpoints are called by Angular's APP_INITIALIZER before the user has
  // a JWT (the GUI uses /config/gui to decide whether to show local-login or
  // Google-login). They MUST be @PermitAll — tagging them @RolesAllowed makes
  // the SPA's bootstrap fail with 403 and the whole site appears dead.
  "ConfigResource.getGuiConfig" should "be @PermitAll so it loads before login" in {
    val method = classOf[ConfigResource].getMethod("getGuiConfig")
    method.getAnnotation(classOf[PermitAll]) should not be null
    method.getAnnotation(classOf[RolesAllowed]) shouldBe null
  }

  "ConfigResource.getUserSystemConfig" should "be @PermitAll so it loads before login" in {
    val method = classOf[ConfigResource].getMethod("getUserSystemConfig")
    method.getAnnotation(classOf[PermitAll]) should not be null
    method.getAnnotation(classOf[RolesAllowed]) shouldBe null
  }
}
