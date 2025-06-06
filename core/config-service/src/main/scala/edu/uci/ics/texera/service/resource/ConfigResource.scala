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

package edu.uci.ics.texera.service.resource

import edu.uci.ics.texera.config.{GuiConfig, UserSystemConfig}
import jakarta.annotation.security.RolesAllowed
import jakarta.ws.rs.{GET, Path, Produces}
import jakarta.ws.rs.core.MediaType

@Path("/config")
@Produces(Array(MediaType.APPLICATION_JSON))
class ConfigResource {

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/gui")
  def getGuiConfig: Map[String, Any] =
    Map(
      // flags from the gui.conf
      "exportExecutionResultEnabled" -> GuiConfig.guiWorkflowWorkspaceExportExecutionResultEnabled,
      "autoAttributeCorrectionEnabled" -> GuiConfig.guiWorkflowWorkspaceAutoAttributeCorrectionEnabled,
      "selectingFilesFromDatasetsEnabled" -> GuiConfig.guiWorkflowWorkspaceSelectingFilesFromDatasetsEnabled,
      "localLogin" -> GuiConfig.guiLoginLocalLogin,
      "googleLogin" -> GuiConfig.guiLoginGoogleLogin,
      "userPresetEnabled" -> GuiConfig.guiWorkflowWorkspaceUserPresetEnabled,
      "workflowExecutionsTrackingEnabled" -> GuiConfig.guiWorkflowWorkspaceWorkflowExecutionsTrackingEnabled,
      "linkBreakpointEnabled" -> GuiConfig.guiWorkflowWorkspaceLinkBreakpointEnabled,
      "asyncRenderingEnabled" -> GuiConfig.guiWorkflowWorkspaceAsyncRenderingEnabled,
      "timetravelEnabled" -> GuiConfig.guiWorkflowWorkspaceTimetravelEnabled,
      "productionSharedEditingServer" -> GuiConfig.guiWorkflowWorkspaceProductionSharedEditingServer,
      "singleFileUploadMaximumSizeMB" -> GuiConfig.guiDatasetSingleFileUploadMaximumSizeMB,
      "maxNumberOfConcurrentUploadingFileChunks" -> GuiConfig.guiDatasetMaxNumberOfConcurrentUploadingFileChunks,
      "multipartUploadChunkSizeByte" -> GuiConfig.guiDatasetMultipartUploadChunkSizeByte,
      "defaultDataTransferBatchSize" -> GuiConfig.guiWorkflowWorkspaceDefaultDataTransferBatchSize,
      "workflowEmailNotificationEnabled" -> GuiConfig.guiWorkflowWorkspaceWorkflowEmailNotificationEnabled,
      "hubEnabled" -> GuiConfig.guiDashboardHubEnabled,
      "forumEnabled" -> GuiConfig.guiDashboardForumEnabled,
      "projectEnabled" -> GuiConfig.guiDashboardProjectEnabled,
      "operatorConsoleMessageBufferSize" -> GuiConfig.guiWorkflowWorkspaceOperatorConsoleMessageBufferSize,
      "pythonLanguageServerPort" -> GuiConfig.guiWorkflowWorkspacePythonLanguageServerPort,
      "defaultLocalUser" -> Map(
        "username" -> GuiConfig.guiLoginDefaultLocalUserUsername,
        "password" -> GuiConfig.guiLoginDefaultLocalUserPassword
      )
    )

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/user-system")
  def getUserSystemConfig: Map[String, Any] =
    Map(
      // flags from the user-system.conf
      "inviteOnly" -> UserSystemConfig.inviteOnly,
      "userSystemEnabled" -> UserSystemConfig.isUserSystemEnabled
    )
}
