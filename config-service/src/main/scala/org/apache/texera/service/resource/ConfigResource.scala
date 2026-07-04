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
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables.SITE_SETTINGS
import org.jooq.impl.DSL

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

  // Write side of the config API, backed by the site_settings table this
  // service seeds at startup. Reads stay open to any logged-in user because
  // non-admin pages (dashboard logo/tabs, dataset upload limits) consume
  // individual keys; only mutation is ADMIN-gated.
  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
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
    if (setting.settingValue != null && keyParam.nonEmpty) {
      upsertSetting(keyParam, setting.settingValue, currentUser.getName)
    }
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
      .insertInto(SITE_SETTINGS)
      .set(SITE_SETTINGS.KEY, keyParam)
      .set(SITE_SETTINGS.VALUE, valueParam)
      .set(SITE_SETTINGS.UPDATED_BY, userName)
      .onConflict(SITE_SETTINGS.KEY)
      .doUpdate()
      .set(SITE_SETTINGS.VALUE, valueParam)
      .set(SITE_SETTINGS.UPDATED_BY, userName)
      .set(SITE_SETTINGS.UPDATED_AT, DSL.currentTimestamp())
      .execute()
  }
}
