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

package org.apache.texera.web.resource

import io.dropwizard.auth.Auth
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables.USER_CONFIG

import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core._
import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
  * This class handles requests to read and write the user dictionary,
  * an abstract collection of (key, value) string pairs that is unique for each user
  * This is accomplished using a table called user_config.
  * The details of user_config can be found in /sql/texera_ddl.sql
  */
@Path("/user/config")
@RolesAllowed(Array("REGULAR", "ADMIN"))
@Consumes(Array(MediaType.TEXT_PLAIN))
class UserConfigResource {
  private def requireValidKey(key: String): Unit =
    if (key == null || key.trim.isEmpty) {
      throw new BadRequestException("key cannot be null or empty")
    }

  @GET
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getAllDict(@Auth sessionUser: SessionUser): Map[String, String] = {
    getDict(sessionUser.getUser.getUid)
  }

  /**
    * This method retrieves all of a user's dictionary entries in
    * the user_config table as a json object
    */
  private def getDict(uid: Integer): Map[String, String] = {
    SqlServer
      .getInstance()
      .createDSLContext()
      .select(USER_CONFIG.KEY, USER_CONFIG.VALUE)
      .from(USER_CONFIG)
      .where(USER_CONFIG.UID.eq(uid))
      .fetch()
      .asScala
      .map(entry => (entry.get(USER_CONFIG.KEY), entry.get(USER_CONFIG.VALUE)))
      .toMap
  }

  @GET
  @Produces(Array(MediaType.TEXT_PLAIN))
  @Path("/{key}")
  def getEntry(@PathParam("key") key: String, @Auth sessionUser: SessionUser): String = {
    requireValidKey(key)
    SqlServer
      .getInstance()
      .createDSLContext()
      .select(USER_CONFIG.VALUE)
      .from(USER_CONFIG)
      .where(USER_CONFIG.UID.eq(sessionUser.getUser.getUid).and(USER_CONFIG.KEY.eq(key)))
      .fetchOne(USER_CONFIG.VALUE)
  }

  /**
    * This method creates or updates an entry in the current in-session user's dictionary based on
    * the "key" and "value" attributes of the PostRequest
    */
  @PUT
  @Path("/{key}")
  def setEntry(
      @PathParam("key") key: String,
      value: String,
      @Auth sessionUser: SessionUser
  ): Unit = {
    requireValidKey(key)
    SqlServer
      .getInstance()
      .createDSLContext()
      .insertInto(USER_CONFIG, USER_CONFIG.UID, USER_CONFIG.KEY, USER_CONFIG.VALUE)
      .values(sessionUser.getUser.getUid, key, value)
      .onConflict(USER_CONFIG.UID, USER_CONFIG.KEY)
      .doUpdate()
      .set(USER_CONFIG.VALUE, value)
      .execute()
  }

  /**
    * This method deletes a key-value pair from the current in-session user's dictionary based on
    * the "key" attribute of the DeleteRequest
    */
  @DELETE
  @Path("/{key}")
  def deleteEntry(@PathParam("key") key: String, @Auth sessionUser: SessionUser): Unit = {
    requireValidKey(key)
    SqlServer
      .getInstance()
      .createDSLContext()
      .deleteFrom(USER_CONFIG)
      .where(USER_CONFIG.UID.eq(sessionUser.getUser.getUid).and(USER_CONFIG.KEY.eq(key)))
      .execute()
  }
}
