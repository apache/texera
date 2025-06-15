/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.texera.config

import com.typesafe.config.{Config, ConfigFactory}

object GuiConfig {
  private val conf: Config = ConfigFactory.parseResources("gui.conf").resolve()

  // GUI Configuration
  // GUI Login Configuration
  val guiLoginLocalLogin: Boolean =
    conf.getBoolean("gui.login.local-login")
  val guiLoginGoogleLogin: Boolean =
    conf.getBoolean("gui.login.google-login")
  val guiLoginDefaultLocalUserUsername: String =
    if (conf.hasPath("gui.login.default-local-user.username"))
      conf.getString("gui.login.default-local-user.username")
    else ""
  val guiLoginDefaultLocalUserPassword: String =
    if (conf.hasPath("gui.login.default-local-user.password"))
      conf.getString("gui.login.default-local-user.password")
    else ""

  // GUI Dashboard Configuration
  val guiDashboardHubEnabled: Boolean = conf.getBoolean("gui.dashboard.hub-enabled")
  val guiDashboardForumEnabled: Boolean = conf.getBoolean("gui.dashboard.forum-enabled")
  val guiDashboardProjectEnabled: Boolean =
    conf.getBoolean("gui.dashboard.project-enabled")

  // GUI Workflow Workspace Configuration
  val guiWorkflowWorkspaceUserPresetEnabled: Boolean =
    conf.getBoolean("gui.workflow-workspace.user-preset-enabled")
  val guiWorkflowWorkspaceExportExecutionResultEnabled: Boolean =
    conf.getBoolean("gui.workflow-workspace.export-execution-result-enabled")
  val guiWorkflowWorkspaceAutoAttributeCorrectionEnabled: Boolean =
    conf.getBoolean("gui.workflow-workspace.auto-attribute-correction-enabled")
  val guiWorkflowWorkspaceDefaultDataTransferBatchSize: Int =
    conf.getInt("gui.workflow-workspace.default-data-transfer-batch-size")
  val guiWorkflowWorkspaceSelectingFilesFromDatasetsEnabled: Boolean =
    conf.getBoolean("gui.workflow-workspace.selecting-files-from-datasets-enabled")
  val guiWorkflowWorkspaceWorkflowExecutionsTrackingEnabled: Boolean =
    conf.getBoolean("gui.workflow-workspace.workflow-executions-tracking-enabled")
  val guiWorkflowWorkspaceLinkBreakpointEnabled: Boolean =
    conf.getBoolean("gui.workflow-workspace.link-breakpoint-enabled")
  val guiWorkflowWorkspaceAsyncRenderingEnabled: Boolean =
    conf.getBoolean("gui.workflow-workspace.async-rendering-enabled")
  val guiWorkflowWorkspaceTimetravelEnabled: Boolean =
    conf.getBoolean("gui.workflow-workspace.timetravel-enabled")
  val guiWorkflowWorkspaceProductionSharedEditingServer: Boolean =
    conf.getBoolean("gui.workflow-workspace.production-shared-editing-server")
  val guiWorkflowWorkspacePythonLanguageServerPort: String =
    conf.getString("gui.workflow-workspace.python-language-server-port")
  val guiWorkflowWorkspaceOperatorConsoleMessageBufferSize: Int =
    conf.getInt("gui.workflow-workspace.operator-console-message-buffer-size")
  val guiWorkflowWorkspaceWorkflowEmailNotificationEnabled: Boolean =
    conf.getBoolean("gui.workflow-workspace.workflow-email-notification-enabled")

  // GUI Dataset Configuration
  val guiDatasetSingleFileUploadMaximumSizeMB: Int =
    conf.getInt("gui.dataset.single-file-upload-maximum-size-mb")
  val guiDatasetMaxNumberOfConcurrentUploadingFileChunks: Int =
    conf.getInt("gui.dataset.max-number-of-concurrent-uploading-file-chunks")
  val guiDatasetMultipartUploadChunkSizeByte: Long =
    conf.getLong("gui.dataset.multipart-upload-chunk-size-byte")
}
