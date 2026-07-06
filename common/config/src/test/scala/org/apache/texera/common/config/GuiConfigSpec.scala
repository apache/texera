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

package org.apache.texera.common.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Spec for [[GuiConfig]]. Reading each value forces resolution from gui.conf, so a renamed or
  * mistyped key surfaces here as a ConfigException. Every key carries a `${?GUI_...}` override, so
  * exact-value assertions are guarded on the env var being unset (mirroring StorageConfigSpec).
  */
class GuiConfigSpec extends AnyFlatSpec with Matchers {

  private def ifUnset(env: String)(assertion: => Any): Unit =
    if (sys.env.get(env).isEmpty) assertion

  "GuiConfig boolean flags" should "resolve to their gui.conf defaults when env overrides are unset" in {
    ifUnset("GUI_LOGIN_LOCAL_LOGIN")(GuiConfig.guiLoginLocalLogin shouldBe true)
    ifUnset("GUI_LOGIN_GOOGLE_LOGIN")(GuiConfig.guiLoginGoogleLogin shouldBe true)
    ifUnset("GUI_WORKFLOW_WORKSPACE_USER_PRESET_ENABLED")(
      GuiConfig.guiWorkflowWorkspaceUserPresetEnabled shouldBe false
    )
    ifUnset("GUI_WORKFLOW_WORKSPACE_EXPORT_EXECUTION_RESULT_ENABLED")(
      GuiConfig.guiWorkflowWorkspaceExportExecutionResultEnabled shouldBe false
    )
    ifUnset("GUI_WORKFLOW_WORKSPACE_AUTO_ATTRIBUTE_CORRECTION_ENABLED")(
      GuiConfig.guiWorkflowWorkspaceAutoAttributeCorrectionEnabled shouldBe true
    )
    ifUnset("GUI_WORKFLOW_WORKSPACE_SELECTING_FILES_FROM_DATASETS_ENABLED")(
      GuiConfig.guiWorkflowWorkspaceSelectingFilesFromDatasetsEnabled shouldBe true
    )
    ifUnset("GUI_WORKFLOW_WORKSPACE_WORKFLOW_EXECUTIONS_TRACKING_ENABLED")(
      GuiConfig.guiWorkflowWorkspaceWorkflowExecutionsTrackingEnabled shouldBe false
    )
    ifUnset("GUI_WORKFLOW_WORKSPACE_LINK_BREAKPOINT_ENABLED")(
      GuiConfig.guiWorkflowWorkspaceLinkBreakpointEnabled shouldBe true
    )
    ifUnset("GUI_WORKFLOW_WORKSPACE_ASYNC_RENDERING_ENABLED")(
      GuiConfig.guiWorkflowWorkspaceAsyncRenderingEnabled shouldBe false
    )
    ifUnset("GUI_WORKFLOW_WORKSPACE_TIMETRAVEL_ENABLED")(
      GuiConfig.guiWorkflowWorkspaceTimetravelEnabled shouldBe false
    )
    ifUnset("GUI_WORKFLOW_WORKSPACE_PRODUCTION_SHARED_EDITING_SERVER")(
      GuiConfig.guiWorkflowWorkspaceProductionSharedEditingServer shouldBe false
    )
    ifUnset("GUI_WORKFLOW_WORKSPACE_WORKFLOW_EMAIL_NOTIFICATION_ENABLED")(
      GuiConfig.guiWorkflowWorkspaceWorkflowEmailNotificationEnabled shouldBe false
    )
    ifUnset("GUI_WORKFLOW_WORKSPACE_COPILOT_ENABLED")(
      GuiConfig.guiWorkflowWorkspaceCopilotEnabled shouldBe false
    )
    ifUnset("GUI_ATTRIBUTION_ENABLED")(GuiConfig.guiAttributionEnabled shouldBe false)
    ifUnset("GUI_DEPLOYMENT_VERSION_CHECK_ENABLED")(
      GuiConfig.guiDeploymentVersionCheckEnabled shouldBe false
    )
    ifUnset("GUI_WORKFLOW_WORKSPACE_PYTHON_NOTEBOOK_MIGRATION_ENABLED")(
      GuiConfig.guiWorkflowWorkspacePythonNotebookMigrationEnabled shouldBe false
    )
  }

  "GuiConfig string settings" should "resolve to their gui.conf defaults when env overrides are unset" in {
    ifUnset("GUI_WORKFLOW_WORKSPACE_DEFAULT_EXECUTION_MODE")(
      GuiConfig.guiWorkflowWorkspaceDefaultExecutionMode shouldBe "PIPELINED"
    )
    // stored as a String even though the conf value is numeric
    ifUnset("GUI_WORKFLOW_WORKSPACE_PYTHON_LANGUAGE_SERVER_PORT")(
      GuiConfig.guiWorkflowWorkspacePythonLanguageServerPort shouldBe "3000"
    )
    ifUnset("GUI_LOGIN_DEFAULT_LOCAL_USER_USERNAME")(
      GuiConfig.guiLoginDefaultLocalUserUsername shouldBe ""
    )
    ifUnset("GUI_LOGIN_DEFAULT_LOCAL_USER_PASSWORD")(
      GuiConfig.guiLoginDefaultLocalUserPassword shouldBe ""
    )
  }

  "GuiConfig integer settings" should "resolve to their gui.conf defaults when env overrides are unset" in {
    ifUnset("GUI_WORKFLOW_WORKSPACE_OPERATOR_CONSOLE_MESSAGE_BUFFER_SIZE")(
      GuiConfig.guiWorkflowWorkspaceOperatorConsoleMessageBufferSize shouldBe 100
    )
    ifUnset("GUI_WORKFLOW_WORKSPACE_ACTIVE_TIME_IN_MINUTES")(
      GuiConfig.guiWorkflowWorkspaceActiveTimeInMinutes shouldBe 15
    )
    ifUnset("GUI_WORKFLOW_WORKSPACE_LIMIT_COLUMNS")(
      GuiConfig.guiWorkflowWorkspaceLimitColumns shouldBe 15
    )
  }
}
