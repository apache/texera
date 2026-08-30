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
import com.microsoft.playwright.options.{LoadState, WaitForSelectorState}
import org.apache.texera.demovideos.config.TestDataConfig

// ═══════════════════════════════════════════════════════════════════
// 1. LoginControllerBuilder
//    new LoginControllerBuilder(ctx).login("user","pass").execute()
// ═══════════════════════════════════════════════════════════════════

class LoginControllerBuilder(ctx: ControllerContext) extends ControllerBuilder(ctx) {

  def login(username: String, password: String): this.type =
    addStep(new ControllerStep {
      override def name = "Login"
      override def run(ctx: ControllerContext): Unit = {
        val page = ctx.page
        // The login form lives at /login; an already-signed-in visitor is
        // redirected away by the component itself.
        page.navigate(s"${TestDataConfig.baseUrl}/login")
        try {
          page.waitForLoadState(
            LoadState.NETWORKIDLE,
            new Page.WaitForLoadStateOptions().setTimeout(Timeouts.Medium)
          )
        } catch {
          case _: Exception =>
        }
        page.waitForTimeout(Delays.Long)

        // "access_token" is the frontend JWT key (auth.service.ts TOKEN_KEY).
        val loggedIn =
          try page.evaluate("() => !!window.localStorage.getItem('access_token')") == true
          catch { case _: Exception => false }
        if (loggedIn) {
          println("[Login] Already authenticated, skipping login form")
          return
        }
        ctx.ensureFakeCursor()

        try {
          page
            .getByTestId("login-submit")
            .first()
            .waitFor(
              new Locator.WaitForOptions()
                .setState(WaitForSelectorState.VISIBLE)
                .setTimeout(Timeouts.Long)
            )
        } catch {
          case _: Exception =>
            val shotPath =
              java.nio.file.Paths.get(TestDataConfig.videoOutputDir, "debug_login_failed.png")
            try {
              java.nio.file.Files.createDirectories(shotPath.getParent)
              page.screenshot(new Page.ScreenshotOptions().setPath(shotPath).setFullPage(true))
              println(s"[Login] Screenshot saved: $shotPath")
            } catch { case _: Exception => }
            throw new RuntimeException(s"Login page not visible. URL: ${page.url()}")
        }

        val usernameField = page
          .getByTestId("login-username")
          .or(page.getByPlaceholder("Username"))
          .first()
        usernameField.waitFor(
          // Wait for text box visible
          new Locator.WaitForOptions()
            .setState(WaitForSelectorState.VISIBLE)
            .setTimeout(Timeouts.Medium)
        )

        Utils.clickWithCursor(page, usernameField)
        usernameField.fill(username)

        // Verify input (username, password) is filled
        // if not, reenter
        val usernameVal =
          try usernameField.inputValue()
          catch { case _: Exception => "" }
        if (usernameVal != username) {
          usernameField.click(new Locator.ClickOptions().setForce(true))
          usernameField.fill("")
          usernameField.fill(username)
        }

        val passwordField = page
          .getByTestId("login-password")
          .or(page.getByPlaceholder("Password"))
          .first()
        passwordField.waitFor(
          new Locator.WaitForOptions()
            .setState(WaitForSelectorState.VISIBLE)
            .setTimeout(Timeouts.Medium)
        )
        Utils.clickWithCursor(page, passwordField)
        passwordField.fill(password)
        val passwordVal =
          try passwordField.inputValue()
          catch { case _: Exception => "" }
        if (passwordVal != password) {
          passwordField.click()
          passwordField.fill(password)
        }

        val signInBtn = page
          .getByTestId("login-submit")
          .or(page.locator("button[type='submit']:has-text('Sign in')"))
          .first()
        Utils.clickWithCursor(page, signInBtn)
        page.waitForLoadState(LoadState.NETWORKIDLE)
        page.waitForTimeout(Delays.Network)

        // If still on login page, try pressing Enter once.
        if (page.getByTestId("login-submit").count() > 0) {
          passwordField.press("Enter")
          page.waitForLoadState(LoadState.NETWORKIDLE)
          page.waitForTimeout(Delays.Network)
        }

        // If still not logged in, surface the error (if any) for debugging.
        if (page.getByTestId("login-submit").count() > 0) {
          val err = page.locator("p.error").first()
          val msg = if (err.count() > 0) err.innerText() else "Login still visible"
          throw new RuntimeException(s"Login failed: $msg")
        }
      }
    })
}
