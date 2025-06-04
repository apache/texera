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

package edu.uci.ics.texera.web.resource.dashboard.admin.settings

import java.util
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}
import io.dropwizard.auth.Auth
import edu.uci.ics.texera.auth.SessionUser
import edu.uci.ics.texera.dao.SqlServer
import org.jooq.impl.DSL
import com.fasterxml.jackson.annotation.JsonProperty

case class AdminSettingsPojo(
    @JsonProperty("key") settingKey: String,
    @JsonProperty("value") settingValue: String
)

@Path("/admin/settings")
@Produces(Array(MediaType.APPLICATION_JSON))
class AdminSettingsResource {

  private val ctx = SqlServer.getInstance().createDSLContext()
  private val SS = DSL.table("site_settings")
  private val KEY = DSL.field("key", classOf[String])
  private val VAL = DSL.field("value", classOf[String])
  private val BY = DSL.field("updated_by", classOf[String])

  @GET
  def listAll(): util.List[AdminSettingsPojo] = {
    ctx
      .select(KEY, VAL)
      .from(SS)
      .fetchInto(classOf[AdminSettingsPojo])
  }

  @PUT
  @RolesAllowed(Array("ADMIN"))
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def updateAll(
      @Auth currentUser: SessionUser,
      settings: util.List[AdminSettingsPojo]
  ): Response = {
    val updatedBy = currentUser.getName

    settings
      .stream()
      .filter(s => s.settingKey != null && s.settingKey.nonEmpty)
      .forEach { s =>
        ctx
          .insertInto(SS)
          .set(KEY, s.settingKey)
          .set(VAL, s.settingValue)
          .set(BY, updatedBy)
          .onConflict(KEY)
          .doUpdate()
          .set(VAL, s.settingValue)
          .set(BY, updatedBy)
          .execute()
      }
    Response.ok().build()
  }

  @POST
  @Path("/delete")
  @RolesAllowed(Array("ADMIN"))
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def deleteSettings(
      keys: util.List[String]
  ): Response = {
    val validKeysArray: Array[String] =
      keys
        .stream()
        .filter(k => k != null && k.nonEmpty)
        .toArray((size: Int) => new Array[String](size))

    if (validKeysArray.nonEmpty) {
      ctx
        .delete(SS)
        .where(KEY.in(validKeysArray: _*))
        .execute()
    }
    Response.ok().build()
  }
}
