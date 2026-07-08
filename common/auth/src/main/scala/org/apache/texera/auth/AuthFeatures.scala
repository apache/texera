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

package org.apache.texera.auth

import io.dropwizard.auth.{AuthDynamicFeature, AuthValueFactoryProvider}
import io.dropwizard.core.setup.Environment
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature

/** Registers the standard Texera auth stack on a Dropwizard service: JWT
  * authentication, `@Auth` SessionUser injection, and `@RolesAllowed`
  * enforcement. Shared by every service so the registrations don't drift apart.
  */
object AuthFeatures {

  /** Register JWT auth, the `@Auth` value factory, and the `@RolesAllowed`
    * dynamic feature on `environment`'s Jersey config.
    */
  def register(environment: Environment): Unit = {
    // Register JWT authentication filter
    environment.jersey.register(new AuthDynamicFeature(classOf[JwtAuthFilter]))
    environment.jersey.register(classOf[UnauthorizedExceptionMapper])

    // Enable @Auth annotation for injecting SessionUser
    environment.jersey.register(new AuthValueFactoryProvider.Binder(classOf[SessionUser]))

    // Enforce @RolesAllowed annotations on resource methods
    environment.jersey.register(classOf[RolesAllowedDynamicFeature])
  }
}
