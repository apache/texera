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

package edu.uci.ics.texera.web.auth

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.texera.auth.SessionUser
import edu.uci.ics.texera.dao.jooq.generated.enums.UserRoleEnum
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.User
import io.dropwizard.auth.Authenticator
import org.jose4j.jwt.consumer.JwtContext

import java.util.Optional

object UserAuthenticator extends Authenticator[JwtContext, SessionUser] with LazyLogging {
  override def authenticate(context: JwtContext): Optional[SessionUser] = {
    // This method will be called once the token's signature has been verified,
    // including the token secret and the expiration time
    try {
      val userName = context.getJwtClaims.getSubject
      val email = context.getJwtClaims.getClaimValue("email").asInstanceOf[String]
      val userId = context.getJwtClaims.getClaimValue("userId").asInstanceOf[Long].toInt
      val role =
        UserRoleEnum.valueOf(context.getJwtClaims.getClaimValue("role").asInstanceOf[String])
      val googleId = context.getJwtClaims.getClaimValue("googleId").asInstanceOf[String]
      val comment = context.getJwtClaims.getClaimValue("comment").asInstanceOf[String]
      val user = new User(userId, userName, email, null, googleId, null, role, comment)
      Optional.of(new SessionUser(user))
    } catch {
      case e: Exception =>
        logger.error("Failed to authenticate the JwtContext", e)
        Optional.empty()
    }

  }
}
