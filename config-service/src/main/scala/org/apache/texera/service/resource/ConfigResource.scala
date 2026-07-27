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

import com.fasterxml.jackson.annotation.JsonProperty
import io.dropwizard.auth.Auth
import jakarta.annotation.security.{PermitAll, RolesAllowed}
import jakarta.ws.rs.core.{MediaType, Response}
import jakarta.ws.rs.{Consumes, GET, POST, PUT, Path, PathParam, Produces}
import org.apache.texera.auth.SessionUser
import org.apache.texera.common.config.{
  ApplicationConfig,
  AuthConfig,
  ComputingUnitConfig,
  DefaultsConfig,
  GuiConfig,
  UserSystemConfig
}
import org.apache.texera.dao.{SiteSettings, SqlServer}
import org.apache.texera.dao.jooq.generated.Tables.SITE_SETTINGS
import org.jooq.Condition
import org.jooq.impl.DSL

import scala.jdk.CollectionConverters._

// Wire DTO for /config/settings: the JSON contract is exactly {key, value};
// the generated jOOQ pojo would also expose updated_by/updated_at.
case class ConfigSettingPojo(
    @JsonProperty("key") settingKey: String,
    @JsonProperty("value") settingValue: String
)

@Path("/config")
@Produces(Array(MediaType.APPLICATION_JSON))
class ConfigResource {

  private def ctx = SqlServer.getInstance().createDSLContext()

  // Anonymous endpoint loaded by the frontend's APP_INITIALIZER before any user has
  // logged in. Only fields that the login page (or the logged-out branches of the
  // dashboard shell) actually need belong here — anything else lives on /gui or
  // /user-system, both of which require authentication.
  @GET
  @PermitAll
  @Path("/pre-login")
  def getPreLoginConfig: Map[String, Any] =
    Map(
      "localLogin" -> GuiConfig.guiLoginLocalLogin,
      "googleLogin" -> GuiConfig.guiLoginGoogleLogin,
      "defaultLocalUser" -> Map(
        "username" -> GuiConfig.guiLoginDefaultLocalUserUsername,
        "password" -> GuiConfig.guiLoginDefaultLocalUserPassword
      ),
      "attributionEnabled" -> GuiConfig.guiAttributionEnabled,
      "deploymentVersionCheckEnabled" -> GuiConfig.guiDeploymentVersionCheckEnabled,
      "inviteOnly" -> UserSystemConfig.inviteOnly
    )

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/gui")
  def getGuiConfig: Map[String, Any] =
    Map(
      // flags from the gui.conf
      "exportExecutionResultEnabled" -> GuiConfig.guiWorkflowWorkspaceExportExecutionResultEnabled,
      "autoAttributeCorrectionEnabled" -> GuiConfig.guiWorkflowWorkspaceAutoAttributeCorrectionEnabled,
      "selectingFilesFromDatasetsEnabled" -> GuiConfig.guiWorkflowWorkspaceSelectingFilesFromDatasetsEnabled,
      "userPresetEnabled" -> GuiConfig.guiWorkflowWorkspaceUserPresetEnabled,
      "workflowExecutionsTrackingEnabled" -> GuiConfig.guiWorkflowWorkspaceWorkflowExecutionsTrackingEnabled,
      "linkBreakpointEnabled" -> GuiConfig.guiWorkflowWorkspaceLinkBreakpointEnabled,
      "asyncRenderingEnabled" -> GuiConfig.guiWorkflowWorkspaceAsyncRenderingEnabled,
      "timetravelEnabled" -> GuiConfig.guiWorkflowWorkspaceTimetravelEnabled,
      "productionSharedEditingServer" -> GuiConfig.guiWorkflowWorkspaceProductionSharedEditingServer,
      "defaultExecutionMode" -> GuiConfig.guiWorkflowWorkspaceDefaultExecutionMode,
      "workflowEmailNotificationEnabled" -> GuiConfig.guiWorkflowWorkspaceWorkflowEmailNotificationEnabled,
      "sharingComputingUnitEnabled" -> ComputingUnitConfig.sharingComputingUnitEnabled,
      "operatorConsoleMessageBufferSize" -> GuiConfig.guiWorkflowWorkspaceOperatorConsoleMessageBufferSize,
      "pythonLanguageServerPort" -> GuiConfig.guiWorkflowWorkspacePythonLanguageServerPort,
      "activeTimeInMinutes" -> GuiConfig.guiWorkflowWorkspaceActiveTimeInMinutes,
      "copilotEnabled" -> GuiConfig.guiWorkflowWorkspaceCopilotEnabled,
      "limitColumns" -> GuiConfig.guiWorkflowWorkspaceLimitColumns,
      "pythonNotebookMigrationEnabled" -> GuiConfig.guiWorkflowWorkspacePythonNotebookMigrationEnabled,
      // flags from the auth.conf if needed
      "expirationTimeInMinutes" -> AuthConfig.jwtExpirationMinutes
    )

  // Engine configs.
  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/amber")
  def getAmberConfig: Map[String, Any] =
    Map(
      "defaultDataTransferBatchSize" -> ApplicationConfig.defaultDataTransferBatchSize
    )

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/user-system")
  def getUserSystemConfig: Map[String, Any] =
    Map(
      // flags from the user-system.conf
      "inviteOnly" -> UserSystemConfig.inviteOnly
    )

  // The site_settings keys that non-admin pages consume: dashboard branding,
  // sidebar tab toggles, and dataset upload limits — exactly the gui.* and
  // dataset.* sections of default.conf, which is also where the seeding
  // pipeline gets them. Keys declared outside those sections (e.g.
  // csv_parser_max_columns) are management-only. Deriving the set from the
  // file keeps "which section does this default live in" the single place
  // where visibility is decided.
  private val publicSettingKeys: Set[String] =
    DefaultsConfig.keysUnderSections(Set("gui", "dataset"))

  // SECURITY: every key returned here is served anonymously (see
  // /settings/public below), so `publicSettingKeys` is the anonymous-exposure
  // surface. It is derived from the gui/dataset sections of default.conf and
  // pinned by ConfigResourceSpec/DefaultsConfigSpec — adding a key under those
  // sections (or moving one in) changes what unauthenticated callers can read
  // and MUST be reviewed there. Never place a secret under gui/dataset.

  private def fetchSettings(condition: Condition): Map[String, String] =
    ctx
      .select(SITE_SETTINGS.KEY, SITE_SETTINGS.VALUE)
      .from(SITE_SETTINGS)
      .where(condition)
      .fetchMap(SITE_SETTINGS.KEY, SITE_SETTINGS.VALUE)
      .asScala
      .toMap

  // Read side for the public keys in one payload, so the dashboard doesn't
  // fire a request per key. Anonymous by design: these values render on the
  // logged-out shell (custom logo/favicon, Hub/About sidebar entries), so
  // gating them behind a login would blank the public landing pages.
  @GET
  @PermitAll
  @Path("/settings/public")
  def getPublicSettings: Map[String, String] =
    fetchSettings(SITE_SETTINGS.KEY.in(publicSettingKeys.asJava))

  // Management read over the site_settings table this service seeds at
  // startup: every row, including the ones not exposed through
  // /settings/public, in one payload for the admin settings page.
  @GET
  @RolesAllowed(Array("ADMIN"))
  @Path("/settings")
  def getAllSettings: Map[String, String] =
    fetchSettings(DSL.noCondition())

  // Single-key management read, kept for API completeness alongside the bulk
  // read above.
  @GET
  @RolesAllowed(Array("ADMIN"))
  @Path("/settings/{key}")
  def getSetting(@PathParam("key") keyParam: String): ConfigSettingPojo = {
    ctx
      .select(SITE_SETTINGS.KEY, SITE_SETTINGS.VALUE)
      .from(SITE_SETTINGS)
      .where(SITE_SETTINGS.KEY.eq(keyParam))
      .fetchOneInto(classOf[ConfigSettingPojo])
  }

  @PUT
  @RolesAllowed(Array("ADMIN"))
  @Path("/settings/{key}")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def updateSetting(
      @Auth currentUser: SessionUser,
      @PathParam("key") keyParam: String,
      setting: ConfigSettingPojo
  ): Response = {
    if (setting == null || setting.settingValue == null) {
      return Response
        .status(Response.Status.BAD_REQUEST)
        .entity("Setting value must not be null")
        .build()
    }
    // Only keys backed by a default.conf entry are writable, mirroring
    // resetSetting. This keeps site_settings within the known-default
    // namespace: an arbitrary key would be un-resettable and would pollute
    // getAllSettings forever.
    if (!DefaultsConfig.allDefaults.contains(keyParam)) {
      return Response
        .status(Response.Status.BAD_REQUEST)
        .entity(s"Unknown setting key '$keyParam'")
        .build()
    }
    SiteSettings.upsert(ctx, keyParam, setting.settingValue, currentUser.getName)
    Response.ok().build()
  }

  /**
    * Resets the specified configuration key to its default value defined in default.conf.
    */
  @POST
  @RolesAllowed(Array("ADMIN"))
  @Path("/settings/reset/{key}")
  def resetSetting(
      @Auth currentUser: SessionUser,
      @PathParam("key") keyParam: String
  ): Response = {
    DefaultsConfig.allDefaults.get(keyParam) match {
      case Some(defaultValue) =>
        SiteSettings.upsert(ctx, keyParam, defaultValue, currentUser.getName)
        Response.ok().build()
      case None =>
        Response
          .status(Response.Status.NOT_FOUND)
          .entity(s"No default for key '$keyParam'")
          .build()
    }
  }
}
