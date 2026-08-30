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

package org.apache.texera.demovideos.controllers

import com.microsoft.playwright._
import com.microsoft.playwright.options.{AriaRole, LoadState, WaitForSelectorState, WaitUntilState}
import org.apache.texera.demovideos.config.TestDataConfig

// ═══════════════════════════════════════════════════════════════════
// 2. NavigationControllerBuilder
//    new NavigationControllerBuilder(ctx).createNewWorkflow().execute()
//    new NavigationControllerBuilder(ctx).importWorkflow("path/to/sample.json").execute()
// ═══════════════════════════════════════════════════════════════════

class NavigationControllerBuilder(ctx: ControllerContext) extends ControllerBuilder(ctx) {

  private def gotoWorkflowList(page: Page): Unit = {
    page.navigate(
      s"${TestDataConfig.baseUrl}/user/workflow",
      new Page.NavigateOptions()
        .setWaitUntil(WaitUntilState.DOMCONTENTLOADED)
        .setTimeout(Timeouts.Long)
    )
    try {
      page.waitForLoadState(
        LoadState.NETWORKIDLE,
        new Page.WaitForLoadStateOptions().setTimeout(Timeouts.Medium)
      )
    } catch {
      case _: Exception =>
    }
  }

  private def waitForCanvas(page: Page): Unit = {
    page
      .getByTestId("navigation-workflow-canvas")
      .first()
      .waitFor(
        new Locator.WaitForOptions()
          .setState(WaitForSelectorState.VISIBLE)
          .setTimeout(Timeouts.Long)
      )
  }

  def createNewWorkflow(): this.type =
    addStep(new ControllerStep {
      override def name = "Create New Workflow"
      override def run(ctx: ControllerContext): Unit = {
        val page = ctx.page
        ctx.ensureFakeCursor()
        gotoWorkflowList(page)

        val createBtn = page
          .getByTestId("navigation-create-workflow-button")
          .or(
            page.getByRole(AriaRole.BUTTON, new Page.GetByRoleOptions().setName("Create Workflow"))
          )
          .first()
        Utils.clickWithCursor(page, createBtn)

        try waitForCanvas(page)
        catch {
          case _: Exception =>
            if (!page.url().contains("/workflow/")) {
              throw new RuntimeException("Create workflow did not open workflow editor.")
            }
        }
      }
    })

  def importWorkflow(jsonFilePath: String): this.type =
    addStep(new ControllerStep {
      override def name = s"Import Workflow from ${jsonFilePath.split("/").last}"
      override def run(ctx: ControllerContext): Unit = {
        val page = ctx.page
        ctx.ensureFakeCursor()

        val filePath = java.nio.file.Paths.get(jsonFilePath)
        if (!java.nio.file.Files.exists(filePath)) {
          throw new RuntimeException(s"Workflow JSON not found: $jsonFilePath")
        }
        // The dashboard names the uploaded workflow after the file, minus the extension.
        val fileName = filePath.getFileName.toString
        val workflowName = {
          val dot = fileName.lastIndexOf('.')
          if (dot == -1) fileName else fileName.substring(0, dot)
        }

        // Uploading from the workflow listing creates a NEW workflow named after the
        // file and appends it to the list; it is not opened automatically.
        gotoWorkflowList(page)

        val uploadBtn = page
          .getByTestId("navigation-upload-workflow-button")
          .or(page.getByTitle("Upload ZIP/JSON file as workflow"))
          .first()
        Utils.waitVisible(uploadBtn)
        val chooser = page.waitForFileChooser(
          new Page.WaitForFileChooserOptions().setTimeout(Timeouts.Medium),
          () => {
            Utils.clickWithCursor(page, uploadBtn)
          }
        )
        chooser.setFiles(filePath)

        try {
          page
            .getByText("Upload Successful")
            .first()
            .waitFor(
              new Locator.WaitForOptions()
                .setState(WaitForSelectorState.VISIBLE)
                .setTimeout(Timeouts.Long)
            )
        } catch {
          case _: Exception =>
            println("[Import] Upload confirmation not seen; falling back to the list entry")
        }

        // The upload handler refreshes the search afterwards, which re-sorts the
        // list newest-first — so the FIRST same-named entry is this upload's workflow.
        // .workflow-name is the list view, .resource-name the card view; which one
        // renders depends on the user's saved view preference.
        val entry = page
          .locator(".workflow-name, .resource-name")
          .filter(new Locator.FilterOptions().setHasText(workflowName))
          .first()
        try entry.scrollIntoViewIfNeeded()
        catch { case _: Exception => }
        Utils.clickWithCursor(page, entry)

        waitForCanvas(page)
        page.waitForTimeout(Delays.Network)
      }
    })
}
