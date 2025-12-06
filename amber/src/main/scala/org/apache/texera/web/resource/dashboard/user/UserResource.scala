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

package org.apache.texera.web.resource.dashboard.user

import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.tables.daos.UserDao
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}

case class AffiliationUpdateRequest(uid: Int, affiliation: String)

object UserResource {
  private lazy val context = SqlServer.getInstance().createDSLContext()
  private lazy val userDao = new UserDao(context.configuration)
}

@Path("/user")
class UserResource {

  /**
   * Update the affiliation of a user.
   * Used by a first-time user to set their own affiliation.
   */
  @PUT
  @Path("/affiliation")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def updateAffiliation(request: AffiliationUpdateRequest): Unit = {
    val user = UserResource.userDao.fetchOneByUid(request.uid)
    if (user == null) {
      throw new WebApplicationException("User not found", Response.Status.NOT_FOUND)
    }

    // set affiliation; empty string: user saw the prompt and skipped
    user.setAffiliation(request.affiliation)
    UserResource.userDao.update(user)
  }
}