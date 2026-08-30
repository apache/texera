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

import com.microsoft.playwright.options.{BoundingBox, WaitForSelectorState}
import com.microsoft.playwright.{Locator, Mouse, Page}

// ═══════════════════════════════════════════════════════════════════
// Utils
// ═══════════════════════════════════════════════════════════════════

object Utils {
  def waitVisible(loc: Locator): Locator = {
    loc.waitFor(new Locator.WaitForOptions().setState(WaitForSelectorState.VISIBLE))
    loc
  }

  // First visible match of a locator, or None; skips detached/hidden nodes.
  def firstVisible(locator: Locator): Option[Locator] = {
    val count = locator.count()
    var i = 0
    while (i < count) {
      val nth = locator.nth(i)
      try {
        if (nth.isVisible()) return Some(nth)
      } catch {
        case _: Exception =>
      }
      i += 1
    }
    None
  }

  // Lowercase-alphanumeric key for loose name matching ("Bar Chart" == "barchart").
  def normalize(s: String): String =
    s.toLowerCase.replaceAll("[^a-z0-9]", "")

  def installFakeCursor(page: Page): Unit = {
    // Both the styles and the cursor element are wiped on every page navigation
    // (e.g., `page.navigate(".../dashboard")` in createNewWorkflow). `addInitScript`
    // re-runs after every load so the cursor follows the user across pages.
    val script =
      """
      () => {
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

        document.addEventListener('mousedown',  (e) => { move(e.clientX, e.clientY); clickRing(e.clientX, e.clientY); }, true);
        document.addEventListener('pointerdown', (e) => { move(e.clientX, e.clientY); clickRing(e.clientX, e.clientY); }, true);
      }
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

  // ── Shared geometry helpers ──

  def cellBox(cell: Locator): BoundingBox = {
    val body = cell.locator("rect.body").first()
    if (body.count() > 0) body.boundingBox() else cell.boundingBox()
  }

  def nudgeCell(page: Page, cell: Locator, dx: Double, dy: Double): Unit = {
    val body = cell.locator("rect.body").first()
    val box = if (body.count() > 0) body.boundingBox() else cell.boundingBox()
    if (box == null) return
    val startX = box.x + box.width / 2.0
    val startY = box.y + box.height / 2.0
    page.mouse().move(startX, startY, new Mouse.MoveOptions().setSteps(20))
    page.mouse().down()
    page.mouse().move(startX + dx, startY + dy, new Mouse.MoveOptions().setSteps(40))
    page.mouse().up()
  }

  def overlaps(a: BoundingBox, b: BoundingBox): Boolean = {
    val ax2 = a.x + a.width; val ay2 = a.y + a.height
    val bx2 = b.x + b.width; val by2 = b.y + b.height
    ax2 > b.x && bx2 > a.x && ay2 > b.y && by2 > a.y
  }

  def centerDx(a: BoundingBox, b: BoundingBox): Double = {
    val ax = a.x + a.width / 2.0
    val bx = b.x + b.width / 2.0
    math.abs(ax - bx)
  }

  def ensureSeparated(
      page: Page,
      source: Locator,
      target: Locator,
      minSpacing: Double = 140.0
  ): Unit = {
    val srcBox = cellBox(source)
    val tgtBox = cellBox(target)
    if (srcBox == null || tgtBox == null) return

    if (overlaps(srcBox, tgtBox) || centerDx(srcBox, tgtBox) < minSpacing) {
      nudgeCell(page, target, dx = 260, dy = 0)
      page.waitForTimeout(200)
      val t1 = cellBox(target)
      if (t1 != null && (overlaps(srcBox, t1) || centerDx(srcBox, t1) < minSpacing)) {
        nudgeCell(page, target, dx = 360, dy = 120)
        page.waitForTimeout(200)
      }
    }
  }
}
