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

import com.typesafe.scalalogging.LazyLogging
import jakarta.annotation.security.{DenyAll, PermitAll, RolesAllowed}
import jakarta.ws.rs.HttpMethod

import java.lang.reflect.Method

/** Startup-time guard that asserts every HTTP-mapped resource method carries an
  * access-control annotation.
  *
  * `RolesAllowedDynamicFeature` enforces `@RolesAllowed` only where it is
  * present; a method with no `@RolesAllowed`/`@PermitAll`/`@DenyAll` (and no
  * class-level equivalent) is silently public. This enforcer scans the
  * registered Jersey resources at boot and fails fast so a forgotten annotation
  * becomes a startup error instead of an open hole.
  */
object RoleAnnotationEnforcer extends LazyLogging {

  private val securityAnnotations: Seq[Class[_ <: java.lang.annotation.Annotation]] =
    Seq(classOf[RolesAllowed], classOf[PermitAll], classOf[DenyAll])

  /** Scans `resourceClasses` and throws if any HTTP-mapped method is missing an
    * access-control annotation, after logging the offending methods.
    */
  def enforce(resourceClasses: Iterable[Class[_]], serviceName: String): Unit = {
    val violations = findUnannotatedEndpoints(resourceClasses)
    if (violations.nonEmpty) {
      val message =
        s"$serviceName has HTTP endpoint(s) without an @RolesAllowed/@PermitAll/@DenyAll " +
          s"annotation; every endpoint must declare its access control explicitly:\n  " +
          violations.mkString("\n  ")
      logger.error(message)
      throw new IllegalStateException(message)
    }
  }

  /** Returns `Class#method` identifiers for every HTTP-mapped method that lacks
    * a security annotation at either the method or its declaring resource class.
    */
  def findUnannotatedEndpoints(resourceClasses: Iterable[Class[_]]): Seq[String] =
    resourceClasses.toSeq.flatMap { resourceClass =>
      val classSecured = hasSecurityAnnotation(resourceClass)
      resourceClass.getMethods.toSeq
        .filter(isHttpMethod)
        .filterNot(method => classSecured || hasSecurityAnnotation(method))
        .map(method => s"${resourceClass.getName}#${method.getName}")
    }.distinct

  /** A method is HTTP-mapped if one of its annotations is itself meta-annotated
    * with `@HttpMethod` (covers `@GET`/`@POST`/`@PUT`/`@DELETE`/`@PATCH`/
    * `@HEAD`/`@OPTIONS` and any custom verb).
    */
  private def isHttpMethod(method: Method): Boolean =
    method.getAnnotations.exists(_.annotationType.isAnnotationPresent(classOf[HttpMethod]))

  private def hasSecurityAnnotation(method: Method): Boolean =
    securityAnnotations.exists(method.isAnnotationPresent)

  private def hasSecurityAnnotation(clazz: Class[_]): Boolean =
    securityAnnotations.exists(clazz.isAnnotationPresent)
}
