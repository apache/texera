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

package edu.uci.ics.texera.web.resource.dashboard.admin.sitesettings

import java.util
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}

import io.dropwizard.auth.Auth
import edu.uci.ics.texera.auth.SessionUser
import edu.uci.ics.texera.dao.SqlServer
import org.jooq.impl.DSL

case class SiteSettingsPojo(
    settingKey: String,
    settingValue: String,
    updatedBy: String,
    updatedAt: java.sql.Timestamp
)

@Path("/admin/site-settings")
@RolesAllowed(Array("ADMIN"))
@Produces(Array(MediaType.APPLICATION_JSON))
class SiteSettingsResource {

  private val ctx = SqlServer.getInstance().createDSLContext()
  private val SS = DSL.table("site_settings")
  private val KEY = DSL.field("setting_key", classOf[String])
  private val VAL = DSL.field("setting_value", classOf[String])
  private val BY = DSL.field("updated_by", classOf[String])
  private val AT = DSL.field("updated_at", classOf[java.sql.Timestamp])

  /**
    * GET /admin/site-settings
    * List out all site settings. We include `@Auth` so the filter runs
    * and enforces ADMIN role.
    */
  @GET
  def listAll(@Auth currentUser: SessionUser): util.List[SiteSettingsPojo] = {
    import scala.jdk.CollectionConverters._
    ctx
      .select(KEY, VAL, BY, AT)
      .from(SS)
      .fetchInto(classOf[SiteSettingsPojo])
      .asScala
      .toList
      .asJava
  }

  /**
    * PUT /admin/site-settings
    * Upsert a batch of settings, recording who did it.
    */
  @PUT
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def updateAll(
      @Auth currentUser: SessionUser,
      settings: util.List[SiteSettingsPojo]
  ): Response = {
    import scala.jdk.CollectionConverters._

    val updatedBy = currentUser.getName

    ctx.transaction { _ =>
      settings.asScala.foreach { s =>
        ctx
          .insertInto(SS, KEY, VAL, BY)
          .values(s.settingKey, s.settingValue, updatedBy)
          .onDuplicateKeyUpdate()
          .set(VAL, s.settingValue)
          .set(BY, updatedBy)
          .execute()
      }
    }
    Response.ok().build()
  }
}
