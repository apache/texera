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

import jakarta.annotation.security.{PermitAll, RolesAllowed}
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.{GET, Path, Produces}
import org.apache.texera.common.config.{
  ApplicationConfig,
  AuthConfig,
  ComputingUnitConfig,
  GuiConfig,
  UserSystemConfig
}

@Path("/config")
@Produces(Array(MediaType.APPLICATION_JSON))
class ConfigResource {

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
}
