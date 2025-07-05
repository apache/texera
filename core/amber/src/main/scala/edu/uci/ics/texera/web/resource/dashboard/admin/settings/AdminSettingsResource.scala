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

import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}
import io.dropwizard.auth.Auth
import edu.uci.ics.texera.auth.SessionUser
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.config.DefaultsConfig
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
  private val siteSettings = DSL.table("site_settings")
  private val key = DSL.field("key", classOf[String])
  private val value = DSL.field("value", classOf[String])
  private val updatedBy = DSL.field("updated_by", classOf[String])

  @GET
  @Path("{key}")
  def getSetting(@PathParam("key") keyParam: String): AdminSettingsPojo = {
    ctx
      .select(key, value)
      .from(siteSettings)
      .where(key.eq(keyParam))
      .fetchOneInto(classOf[AdminSettingsPojo])
  }

  @PUT
  @Path("{key}")
  @RolesAllowed(Array("ADMIN"))
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def updateSetting(
      @Auth currentUser: SessionUser,
      @PathParam("key") keyParam: String,
      setting: AdminSettingsPojo
  ): Response = {
    if (setting.settingValue != null && keyParam.nonEmpty) {
      upsertSetting(keyParam, setting.settingValue, currentUser.getName)
    }
    Response.ok().build()
  }

  /**
    * Resets the specified configuration key to its default value defined in default.conf.
    */
  @POST
  @Path("/reset/{key}")
  @RolesAllowed(Array("ADMIN"))
  def resetSetting(
      @Auth currentUser: SessionUser,
      @PathParam("key") keyParam: String
  ): Response = {
    DefaultsConfig.allDefaults.get(keyParam) match {
      case Some(defaultValue) =>
        upsertSetting(keyParam, defaultValue, currentUser.getName)
        Response.ok().build()
      case None =>
        Response
          .status(Response.Status.NOT_FOUND)
          .entity(s"No default for key '$keyParam'")
          .build()
    }
  }

  private def upsertSetting(keyParam: String, valueParam: String, userName: String): Unit = {
    ctx
      .insertInto(siteSettings)
      .set(key, keyParam)
      .set(value, valueParam)
      .set(updatedBy, userName)
      .onConflict(key)
      .doUpdate()
      .set(value, valueParam)
      .set(DSL.field("updated_by", classOf[String]), userName)
      .set(DSL.field("updated_at", classOf[java.sql.Timestamp]), DSL.currentTimestamp())
      .execute()
  }
}
