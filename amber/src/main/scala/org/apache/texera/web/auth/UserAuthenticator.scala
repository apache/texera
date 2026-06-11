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

package org.apache.texera.web.auth

import com.typesafe.scalalogging.LazyLogging
import io.dropwizard.auth.Authenticator
import org.apache.texera.auth.{JwtParser, SessionUser}
import org.jose4j.jwt.consumer.JwtContext

import java.util.Optional

/** Adapter for the toastshaman Dropwizard JWT filter. The filter has already
  * verified the signature by the time this is invoked, so the work here is
  * pure claim extraction — delegated to [[JwtParser.claimsToSessionUser]]
  * so amber and the microservices produce identical [[SessionUser]] objects
  * from the same token.
  */
object UserAuthenticator extends Authenticator[JwtContext, SessionUser] with LazyLogging {
  override def authenticate(context: JwtContext): Optional[SessionUser] = {
    try {
      Optional.of(JwtParser.claimsToSessionUser(context.getJwtClaims))
    } catch {
      case e: Exception =>
        logger.error("Failed to authenticate the JwtContext", e)
        Optional.empty()
    }
  }
}
