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

package edu.uci.ics.texera.web.resource

import edu.uci.ics.amber.engine.common.AmberConfig
import javax.ws.rs.core.MediaType
import javax.ws.rs.{GET, Path, Produces}

@Path("/gui/config")
@Produces(Array(MediaType.APPLICATION_JSON))
class GuiConfigResource {

  @GET
  def getGuiConfig: Map[String, Any] =
    Map(
      "apiUrl" -> AmberConfig.guiApiUrl,
      "exportExecutionResultEnabled" -> AmberConfig.guiWorkflowWorkspaceExportExecutionResultEnabled,
      "autoAttributeCorrectionEnabled" -> AmberConfig.guiWorkflowWorkspaceAutoAttributeCorrectionEnabled,
      "userSystemEnabled" -> AmberConfig.isUserSystemEnabled,
      "selectingFilesFromDatasetsEnabled" -> AmberConfig.guiWorkflowWorkspaceSelectingFilesFromDatasetsEnabled,
      "localLogin" -> AmberConfig.guiLoginAllowUsernamePassword,
      "googleLogin" -> AmberConfig.guiLoginAllowGoogleAccount,
      "inviteOnly" -> AmberConfig.inviteOnly,
      "userPresetEnabled" -> AmberConfig.guiWorkflowWorkspaceUserPresetEnabled,
      "workflowExecutionsTrackingEnabled" -> AmberConfig.guiWorkflowWorkspaceWorkflowExecutionsTrackingEnabled,
      "linkBreakpointEnabled" -> AmberConfig.guiWorkflowWorkspaceLinkBreakpointEnabled,
      "asyncRenderingEnabled" -> AmberConfig.guiWorkflowWorkspaceAsyncRenderingEnabled,
      "timetravelEnabled" -> AmberConfig.guiWorkflowWorkspaceTimetravelEnabled,
      "productionSharedEditingServer" -> AmberConfig.guiWorkflowWorkspaceProductionSharedEditingServer,
      "singleFileUploadMaximumSizeMB" -> AmberConfig.guiDatasetSingleFileUploadMaximumSizeMB,
      "maxNumberOfConcurrentUploadingFileChunks" -> AmberConfig.guiDatasetMaxNumberOfConcurrentUploadingFileChunks,
      "multipartUploadChunkSizeByte" -> AmberConfig.guiDatasetMultipartUploadChunkSizeByte,
      "defaultDataTransferBatchSize" -> AmberConfig.defaultDataTransferBatchSize,
      "workflowEmailNotificationEnabled" -> AmberConfig.guiWorkflowWorkspaceWorkflowEmailNotificationEnabled,
      "hubEnabled" -> AmberConfig.guiDashboardHubEnabled,
      "forumEnabled" -> AmberConfig.guiDashboardForumEnabled,
      "projectEnabled" -> AmberConfig.guiDashboardProjectEnabled,
      "operatorConsoleMessageBufferSize" -> AmberConfig.guiWorkflowWorkspaceOperatorConsoleMessageBufferSize,
      "defaultLocalUser" -> Map(
        "username" -> AmberConfig.guiLoginDefaultLocalUserUsername,
        "password" -> AmberConfig.guiLoginDefaultLocalUserPassword
      ).filter { case (_, v) => v.nonEmpty } // Only include non-empty values
    )
}
