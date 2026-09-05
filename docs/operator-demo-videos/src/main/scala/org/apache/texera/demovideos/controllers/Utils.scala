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

import com.microsoft.playwright.options.WaitForSelectorState
import com.microsoft.playwright.{Locator, Mouse, Page}

// ═══════════════════════════════════════════════════════════════════
// Utils
// ═══════════════════════════════════════════════════════════════════

object Utils {
  def waitVisible(loc: Locator): Locator = {
    loc.waitFor(new Locator.WaitForOptions().setState(WaitForSelectorState.VISIBLE))
    loc
  }

  def installFakeCursor(page: Page): Unit = {
    // Both the styles and the cursor element are wiped on every page navigation
    // (e.g., `page.navigate(".../dashboard")` in createNewWorkflow). `addInitScript`
    // re-runs after every load so the cursor follows the user across pages.
    // An IIFE (addInitScript executes raw source, so a bare function expression would
    // never run), deferring DOM setup until the document exists.
    val script =
      """
      (() => {
        const setup = () => {
        if (document.getElementById('pw-cursor-style')) return;
        const style = document.createElement('style');
        style.id = 'pw-cursor-style';
        style.textContent = `
          #pw-cursor {
            position: fixed; left: 0; top: 0;
            width: 14px; height: 14px; border-radius: 50%;
            background: rgba(255, 0, 0, 0.9);
            box-shadow: 0 0 0 3px rgba(255, 0, 0, 0.25);
            pointer-events: none; z-index: 2147483647;
            transform: translate(-50%, -50%);
          }
          .pw-click {
            position: fixed; left: 0; top: 0;
            width: 18px; height: 18px; border-radius: 50%;
            border: 3px solid rgba(255, 0, 0, 0.85);
            pointer-events: none; z-index: 2147483647;
            transform: translate(-50%, -50%);
            animation: pw-click-pop 600ms ease-out forwards;
          }
          @keyframes pw-click-pop {
            0%   { opacity: 0.9; transform: translate(-50%, -50%) scale(0.6); }
            70%  { opacity: 0.6; transform: translate(-50%, -50%) scale(2.2); }
            100% { opacity: 0.0; transform: translate(-50%, -50%) scale(2.8); }
          }
        `;
        (document.head || document.documentElement).appendChild(style);

        const ensureCursor = () => {
          if (document.getElementById('pw-cursor')) return document.getElementById('pw-cursor');
          const cursor = document.createElement('div');
          cursor.id = 'pw-cursor';
          (document.body || document.documentElement).appendChild(cursor);
          return cursor;
        };

        const move = (x, y) => {
          const c = ensureCursor();
          c.style.left = x + 'px';
          c.style.top  = y + 'px';
        };

        document.addEventListener('mousemove',  (e) => move(e.clientX, e.clientY), true);
        document.addEventListener('pointermove', (e) => move(e.clientX, e.clientY), true);

        const clickRing = (x, y) => {
          const ring = document.createElement('div');
          ring.className = 'pw-click';
          ring.style.left = x + 'px';
          ring.style.top  = y + 'px';
          (document.body || document.documentElement).appendChild(ring);
          setTimeout(() => ring.remove(), 650);
        };

        // Chromium fires pointerdown followed by the compatibility mousedown for each
        // click; listening to both would render the ring twice per click.
        document.addEventListener('pointerdown', (e) => { move(e.clientX, e.clientY); clickRing(e.clientX, e.clientY); }, true);
        };
        // Init scripts run before the document exists; the DOM work must wait.
        if (document.readyState === 'loading') {
          document.addEventListener('DOMContentLoaded', setup);
        } else {
          setup();
        }
      })()
      """

    // Persistent across navigations.
    page.addInitScript(script)
    // Run once now so the cursor is visible immediately on the current page
    // (addInitScript only fires on subsequent loads, not retroactively).
    page.evaluate(script)
  }

  // `holdMs` is the gap between mousedown and mouseup. The default 0 fires both in the same
  // tick, which some Angular handlers (notably the Run/Pause toolbar button) can miss, so
  // callers that need a click to reliably register pass a short hold.
  def clickWithCursor(page: Page, loc: Locator, steps: Int = 20, holdMs: Int = 0): Unit = {
    waitVisible(loc)
    val box = loc.boundingBox()
    if (box == null) throw new RuntimeException("No bounding box")
    val x = box.x + box.width / 2.0
    val y = box.y + box.height / 2.0
    page.mouse().move(x, y, new Mouse.MoveOptions().setSteps(steps))
    if (holdMs > 0) page.mouse().click(x, y, new Mouse.ClickOptions().setDelay(holdMs.toDouble))
    else page.mouse().click(x, y)
  }

  def hoverWithCursor(page: Page, loc: Locator, steps: Int = 20): Unit = {
    waitVisible(loc)
    val box = loc.boundingBox()
    if (box == null) throw new RuntimeException("No bounding box")
    val x = box.x + box.width / 2.0
    val y = box.y + box.height / 2.0
    page.mouse().move(x, y, new Mouse.MoveOptions().setSteps(steps))
  }
}
