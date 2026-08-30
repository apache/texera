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
import com.microsoft.playwright.options.WaitForSelectorState
import org.apache.texera.amber.operator.metadata.OperatorMetadataGenerator

// ═══════════════════════════════════════════════════════════════════
// 3. OperatorControllerBuilder
//    new OperatorControllerBuilder(ctx)
//      .insertViaDrag("Bar Chart", dragNextTo = Some("CSVFileScan-operator-"))
//      .execute()
// ═══════════════════════════════════════════════════════════════════

class OperatorControllerBuilder(ctx: ControllerContext) extends ControllerBuilder(ctx) {

  private lazy val operatorMetadata = OperatorMetadataGenerator.allOperatorMetadata.operators
  private def groupPathByName: Map[String, Seq[String]] = OperatorGroups.pathByName

  // Some list items drag from an inner handle element rather than the outer container.
  private def dragHandle(item: Locator): Locator = {
    val draggable = Utils.firstVisible(item.locator("[draggable='true']")).orNull
    if (draggable != null && draggable.count() > 0) draggable else item
  }

  def insertViaDrag(
      operatorName: String,
      operatorType: Option[String] = None,
      canvasPosition: (Double, Double) = (0.06, 0.2),
      dragNextTo: Option[String] = None,
      autoConnectToAnchor: Boolean = false,
      connectAdditionalFrom: Option[String] = None,
      connectAdditionalFromPortIndex: Int = 0,
      connectAdditionalToInputIndex: Option[Int] = None,
      // Gap (px) between the anchor's right edge and the drop; ML scripts pass a
      // tighter value because their template has more intermediate nodes.
      dragSpacing: Double = 180.0
  ): this.type =
    addStep(new ControllerStep {
      override def name =
        s"Insert '$operatorName' via Drag${dragNextTo.map(n => s" (next to $n)").getOrElse("")}"
      override def run(ctx: ControllerContext): Unit = {
        val page = ctx.page
        ctx.ensureFakeCursor()
        val sidebarSearchText: String = operatorName

        // ── Open operator panel & resolve source ──
        val operatorsMenu = page
          .getByTestId("operator-left-panel-operators-button")
          .or(page.getByText("Operators", new Page.GetByTextOptions().setExact(true)))
          .first()
        Utils.waitVisible(operatorsMenu)
        Utils.clickWithCursor(page, operatorsMenu)

        val panelSearch = page
          .getByTestId("operator-search-input")
          .or(page.getByPlaceholder("search operator"))
          .first()
        try {
          panelSearch.waitFor(
            new Locator.WaitForOptions()
              .setState(WaitForSelectorState.VISIBLE)
              .setTimeout(Timeouts.Quick)
          )
        } catch {
          case _: Exception =>
            // Some states require one more click to switch to the Operators tab.
            Utils.clickWithCursor(page, operatorsMenu)
            panelSearch.waitFor(
              new Locator.WaitForOptions()
                .setState(WaitForSelectorState.VISIBLE)
                .setTimeout(Timeouts.Quick)
            )
        }

        // Locate operator from group
        val metadata = metadataFor(operatorName, operatorType)
        val groupPath = metadata
          .flatMap(m => groupPathByName.get(m.additionalMetadata.operatorGroupName))
          .getOrElse(Seq.empty)
        val hierarchyOperator = resolveByGroupPath(page, operatorName, operatorType)

        val operator = hierarchyOperator
          .map(dragHandle)
          .getOrElse {
            println(s"[Operator] Hierarchy fallback to search for '$operatorName'")
            val searchInput = page
              .getByTestId("operator-search-input")
              .or(page.getByPlaceholder("search operator"))
              .first()
            Utils.waitVisible(searchInput)
            Utils.clickWithCursor(page, searchInput)
            searchInput.fill(sidebarSearchText)
            page.waitForTimeout(Delays.Settle)
            dragHandle(resolveOperatorSource(page, sidebarSearchText, operatorType))
          }
        operator.scrollIntoViewIfNeeded()
        page.waitForTimeout(Delays.Tick)

        // ── Prepare canvas ──
        val canvas = page
          .getByTestId("navigation-workflow-canvas")
          .or(page.locator("svg[joint-selector='svg'], svg#v-2"))
          .first()
        Utils.waitVisible(canvas)
        canvas.scrollIntoViewIfNeeded()
        page.waitForTimeout(Delays.Tick)

        val beforeCount = page.locator("g.joint-cell.joint-element").count()
        val beforeLinkCount = page.locator("g.joint-cell.joint-link").count()
        val canvasBox = canvas.boundingBox()
        if (canvasBox == null)
          throw new RuntimeException("Drag failed: missing canvas bounding box")

        // ── Calculate drop position ──
        val anchorNode: Option[Locator] = dragNextTo.flatMap(findNodeByType(page, _))

        if (dragNextTo.isDefined && anchorNode.isEmpty) {
          println(
            s"[Operator] Warning: dragNextTo='${dragNextTo.get}' not found on canvas, using default position"
          )
        }

        val (tgtX, tgtY) = anchorNode
          .flatMap { anchor =>
            val box = Utils.cellBox(anchor)
            if (box != null) {
              Some(
                (
                  math.min(canvasBox.x + canvasBox.width - 30, box.x + box.width + dragSpacing),
                  box.y + box.height / 2.0 - 40.0 // slight lift so the label row stays readable
                )
              )
            } else None
          }
          .getOrElse {
            // No anchor: place at the canvasPosition fraction, stepping a 4-column grid
            // (~node footprint) past any existing nodes. The min() clamps keep the drop
            // inside the canvas — outside it the drop is silently lost.
            val index = Math.max(0, beforeCount)
            val baseX = canvasBox.x + canvasBox.width * canvasPosition._1
            val baseY = canvasBox.y + canvasBox.height * canvasPosition._2
            (
              math.min(canvasBox.x + canvasBox.width - 40, baseX + (index % 4) * 180),
              math.min(canvasBox.y + canvasBox.height - 40, baseY + (index / 4) * 120)
            )
          }

        // ── Perform drag with fallbacks ──
        performDrag(page, operator, tgtX, tgtY)

        val targetCount = beforeCount + 1
        if (!waitForNodeCountAtLeast(page, targetCount, maxRetries = 20)) {
          val searchInput = page
            .getByTestId("operator-search-input")
            .or(page.getByPlaceholder("search operator"))
            .first()
          Utils.waitVisible(searchInput)
          Utils.clickWithCursor(page, searchInput)
          searchInput.fill("")
          page.waitForTimeout(Delays.Tick)
          val retryOperator =
            dragHandle(resolveOperatorSource(page, sidebarSearchText, operatorType))
          performDrag(page, retryOperator, tgtX, tgtY)
        }
        if (!waitForNodeCountAtLeast(page, targetCount, maxRetries = 20)) {
          // Last fallback: insert through search + Enter when drag source is flaky.
          val searchInput = page
            .getByTestId("operator-search-input")
            .or(page.getByPlaceholder("search operator"))
            .first()
          Utils.waitVisible(searchInput)
          Utils.clickWithCursor(page, searchInput)
          searchInput.fill("")
          page.waitForTimeout(Delays.Tick)
          searchInput.fill(sidebarSearchText)
          page.waitForTimeout(Delays.Tick)
          searchInput.press("Enter")
        }
        if (!waitForNodeCountAtLeast(page, targetCount, maxRetries = 20)) {
          throw new RuntimeException(
            s"Insert failed for '$operatorName' (${operatorType.getOrElse("unknown")}). " +
              s"Canvas count did not increase from $beforeCount."
          )
        }

        val centerBtn = page.getByTitle("minimap-center-button")
        if (centerBtn.count() > 0) {
          Utils.clickWithCursor(page, centerBtn)
          page.waitForTimeout(Delays.Tick)
        }

        // ── Click the new node to select it ──
        val newNode = operatorType
          .flatMap(findNodeByType(page, _))
          .getOrElse(Utils.waitVisible(page.locator("g.joint-cell.joint-element").nth(beforeCount)))
        if (newNode.count() > 0) {
          val body = newNode.locator("rect.body").first()
          if (body.count() > 0) Utils.clickWithCursor(page, body)
          else Utils.clickWithCursor(page, newNode)
        }

        if (
          autoConnectToAnchor && dragNextTo.isDefined && anchorNode.exists(_.count() > 0) && newNode
            .count() > 0
        ) {
          // Check if the drag itself already created a link (canvas port-snapping)
          val currentLinkCount = page.locator("g.joint-cell.joint-link").count()
          if (currentLinkCount > beforeLinkCount) {
            println(
              s"[Operator] Drag already created a link for '$operatorName', skipping autoConnect"
            )
          } else {
            val connected = tryAutoConnect(
              page,
              anchorNode.get,
              newNode,
              fromPortIndex = 0,
              toPortIndex = 0
            )
            if (connected) {
              var retries = 0
              while (
                page
                  .locator("g.joint-cell.joint-link")
                  .count() <= beforeLinkCount && retries < Retries.Short
              ) {
                page.waitForTimeout(Delays.Tick)
                retries += 1
              }
              if (page.locator("g.joint-cell.joint-link").count() <= beforeLinkCount) {
                println(
                  s"[Operator] Warning: explicit connect attempted but no new link was detected for '$operatorName'"
                )
              }
            } else {
              println(s"[Operator] Warning: could not locate connectable ports for '$operatorName'")
            }
          }
        }

        if (connectAdditionalFrom.isDefined && newNode.count() > 0) {
          // Clear state from the first connection before attempting the second
          try page.keyboard().press("Escape")
          catch { case _: Exception => }
          page.waitForTimeout(Delays.Settle)

          val additionalFromNode = findNodeByType(page, connectAdditionalFrom.get)
          if (additionalFromNode.isEmpty) {
            println(
              s"[Operator] Warning: connectAdditionalFrom='${connectAdditionalFrom.get}' not found on canvas"
            )
          } else {
            val expectedLinkCount = page.locator("g.joint-cell.joint-link").count() + 1
            val targetInputPort = connectAdditionalToInputIndex.getOrElse(0)

            val inputPorts = collectInputPortCount(page, newNode)
            val alreadyConnected =
              inputPorts > 0 && page.locator("g.joint-cell.joint-link").count() >= expectedLinkCount
            if (alreadyConnected) {
              println(
                s"[Operator] Additional input port likely already connected for '$operatorName', skipping"
              )
            } else {
              try additionalFromNode.get.scrollIntoViewIfNeeded()
              catch { case _: Exception => }
              try newNode.scrollIntoViewIfNeeded()
              catch { case _: Exception => }
              page.waitForTimeout(Delays.Tick)

              val beforeExtraLinkCount = page.locator("g.joint-cell.joint-link").count()
              var connected = tryAutoConnect(
                page,
                additionalFromNode.get,
                newNode,
                fromPortIndex = connectAdditionalFromPortIndex,
                toPortIndex = targetInputPort
              )
              if (!connected) {
                page.waitForTimeout(Delays.Network)
                try page.keyboard().press("Escape")
                catch { case _: Exception => }
                page.waitForTimeout(Delays.Settle)
                connected = tryAutoConnect(
                  page,
                  additionalFromNode.get,
                  newNode,
                  fromPortIndex = connectAdditionalFromPortIndex,
                  toPortIndex = targetInputPort
                )
              }
              if (connected) {
                var retries = 0
                while (
                  page
                    .locator("g.joint-cell.joint-link")
                    .count() <= beforeExtraLinkCount && retries < Retries.Short
                ) {
                  page.waitForTimeout(Delays.Tick)
                  retries += 1
                }
                if (page.locator("g.joint-cell.joint-link").count() <= beforeExtraLinkCount) {
                  println(
                    s"[Operator] Warning: additional connect attempted but no new link was detected for '$operatorName'"
                  )
                }
              } else {
                println(
                  s"[Operator] Warning: could not connect additional input for '$operatorName'"
                )
              }
            }
          }
        }

        // ── Reposition (default placement only): with no anchor but nodes already on
        // the canvas, nudge the drop next to the last node.
        if (dragNextTo.isEmpty) {
          val referenceNode = anchorNode
            .filter(_.count() > 0)
            .orElse(
              if (beforeCount > 0)
                Some(page.locator("g.joint-cell.joint-element").nth(beforeCount - 1))
              else None
            )
            .orNull

          if (referenceNode != null && referenceNode.count() > 0) {
            val refBox = Utils.cellBox(referenceNode)
            val newBox = Utils.cellBox(newNode)
            if (refBox != null && newBox != null) {
              val targetCenterX = refBox.x + refBox.width + 220.0 + newBox.width / 2.0
              val targetCenterY = refBox.y + refBox.height / 2.0
              val currentCenterX = newBox.x + newBox.width / 2.0
              val currentCenterY = newBox.y + newBox.height / 2.0
              Utils.nudgeCell(
                page,
                newNode,
                targetCenterX - currentCenterX,
                targetCenterY - currentCenterY
              )
              page.waitForTimeout(Delays.Tick)
            }
            Utils.ensureSeparated(page, referenceNode, newNode)
          }
        }
      }
    })

  private def findNodeByType(page: Page, operatorTypeOrName: String): Option[Locator] = {
    val normalized = Utils.normalize(operatorTypeOrName)
    val relaxed = normalized.replace("operator", "")

    // Prefer startsWith over contains to avoid "csvfilescan" matching "csvoldfilescan"
    def matches(candidate: String): Boolean = {
      val c = Utils.normalize(candidate)
      c.nonEmpty && (
        c.startsWith(normalized) ||
        c.contains(normalized + "-") ||
        c.contains(normalized) ||
        (relaxed.nonEmpty && c.startsWith(relaxed))
      )
    }

    val cells = page.locator("g.joint-cell.joint-element")
    val count = cells.count()
    var i = 0
    while (i < count) {
      val cell = cells.nth(i)
      val testId =
        try Option(cell.getAttribute("data-testid")).getOrElse("")
        catch { case _: Exception => "" }
      if (matches(testId)) return Some(cell)

      val modelId =
        try Option(cell.getAttribute("model-id")).getOrElse("")
        catch { case _: Exception => "" }
      if (matches(modelId)) return Some(cell)

      val label = cell.locator("text.operator-name, .texera-operator-label, text").first()
      val labelText =
        try {
          if (label.count() > 0) Option(label.innerText()).getOrElse("") else ""
        } catch { case _: Exception => "" }
      if (matches(labelText)) {
        return Some(cell)
      }
      i += 1
    }
    None
  }

  private def collectInputPortCount(page: Page, node: Locator): Int = {
    val selector = Seq(
      "[port-group='input'][port]",
      "[port-group='in'][port]",
      "[port*='input']",
      "circle[port-group='input']"
    ).mkString(", ")
    node.locator(selector).count()
  }

  private def waitForNodeCountAtLeast(page: Page, targetCount: Int, maxRetries: Int): Boolean = {
    var retries = 0
    while (
      page.locator("g.joint-cell.joint-element").count() < targetCount && retries < maxRetries
    ) {
      page.waitForTimeout(Delays.Tick)
      retries += 1
    }
    page.locator("g.joint-cell.joint-element").count() >= targetCount
  }

  private def performDrag(page: Page, source: Locator, targetX: Double, targetY: Double): Unit = {
    val src = dragHandle(source)
    val srcBox = src.boundingBox()
    if (srcBox == null) throw new RuntimeException("Drag failed: missing source bounding box")
    val srcX = srcBox.x + srcBox.width / 2.0
    val srcY = srcBox.y + srcBox.height / 2.0
    page.mouse().move(srcX, srcY, new Mouse.MoveOptions().setSteps(25))
    page.mouse().down()
    page.mouse().move(targetX, targetY, new Mouse.MoveOptions().setSteps(35))
    page.mouse().up()
  }

  private def tryAutoConnect(
      page: Page,
      fromNode: Locator,
      toNode: Locator,
      fromPortIndex: Int = 0,
      toPortIndex: Int = 0
  ): Boolean = {
    def collectPortCenters(node: Locator, io: String): Seq[(Option[Int], Double, Double)] = {
      val selectors = if (io == "output") Seq("output") else Seq("input", "in")
      val selector = selectors.map(p => s"[port*='$p']").mkString(", ")

      val ports = node.locator(selector)
      val total = ports.count()
      var i = 0
      val seenIndices = scala.collection.mutable.Set.empty[Int]
      val buf = scala.collection.mutable.ArrayBuffer.empty[(Option[Int], Double, Double)]
      while (i < total) {
        val p = ports.nth(i)
        try {
          val visible = p.isVisible()
          val rawAttr = Option(p.getAttribute("port"))
            .orElse(Option(p.getAttribute("data-port")))
            .getOrElse("")
          if (visible) {
            val attr = rawAttr
            val idx = (".*?(?:input|output|in|out)-([0-9]+).*").r
              .findFirstMatchIn(attr)
              .map(_.group(1).toInt)
            if (!idx.exists(seenIndices.contains)) {
              // Drill down to circle.port-body for precise bounding box;
              // the wrapper <g> includes the label text and gives an offset center.
              val portBody = p.locator("circle.port-body, circle").first()
              val target =
                if (
                  portBody.count() > 0 && (try portBody.isVisible()
                  catch { case _: Exception => false })
                )
                  portBody
                else p
              val b = target.boundingBox()
              if (b != null) {
                idx.foreach(seenIndices.add)
                buf += ((idx, b.x + b.width / 2.0, b.y + b.height / 2.0))
              }
            }
          }
        } catch {
          case _: Exception =>
        }
        i += 1
      }
      buf.sortBy { case (idx, _, y) => (idx.getOrElse(Int.MaxValue), y) }.toSeq
    }

    def pickCenter(
        ports: Seq[(Option[Int], Double, Double)],
        index: Int
    ): Option[(Double, Double)] = {
      ports
        .find(_._1.contains(index))
        .orElse(ports.lift(index))
        .map { case (_, x, y) => (x, y) }
    }

    // Brief pause to let the DOM settle (important when called after another connection)
    page.waitForTimeout(Delays.Tick)

    val fromPort = pickCenter(collectPortCenters(fromNode, "output"), fromPortIndex)
    val toPort = pickCenter(collectPortCenters(toNode, "input"), toPortIndex)

    val beforeLinks = page.locator("g.joint-cell.joint-link").count()

    if (fromPort.isEmpty || toPort.isEmpty) {
      println(
        s"[tryAutoConnect] Port not found: fromPort=$fromPort toPort=$toPort (fromIdx=$fromPortIndex toIdx=$toPortIndex)"
      )

      // If a specific non-zero port index was requested, do NOT fall back to a
      // center-to-center drag — that would route the link to whichever port is
      // visible (typically port 0), silently aliasing two connections onto the
      // same port. Common case: HashJoin's port-1 has `dependencies=List(port-0)`
      // and is hidden until port-0 has schema. Returning false lets the caller's
      // retry loop wait and re-collect ports.
      if (fromPortIndex > 0 || toPortIndex > 0) return false

      val fromBox = Utils.cellBox(fromNode)
      val toBox = Utils.cellBox(toNode)
      if (fromBox == null || toBox == null) return false
      val startX = fromBox.x + fromBox.width - 2
      val startY = fromBox.y + fromBox.height / 2.0
      val endX = toBox.x + 2
      val endY = toBox.y + toBox.height / 2.0
      page.mouse().move(startX, startY, new Mouse.MoveOptions().setSteps(12))
      page.mouse().down()
      page.mouse().move(endX, endY, new Mouse.MoveOptions().setSteps(20))
      page.mouse().up()
      page.waitForTimeout(Delays.Settle)
      return page.locator("g.joint-cell.joint-link").count() > beforeLinks
    }

    page.mouse().move(fromPort.get._1, fromPort.get._2, new Mouse.MoveOptions().setSteps(12))
    page.mouse().down()
    page.waitForTimeout(Delays.Tick)
    page.mouse().move(toPort.get._1, toPort.get._2, new Mouse.MoveOptions().setSteps(25))
    page.mouse().up()
    page.waitForTimeout(Delays.Settle)
    page.locator("g.joint-cell.joint-link").count() > beforeLinks
  }

  private def metadataFor(operatorName: String, operatorType: Option[String]) = {
    val normalizedName = Utils.normalize(operatorName)
    operatorType
      .filter(_.nonEmpty)
      .flatMap(t => operatorMetadata.find(_.operatorType == t))
      .orElse(
        operatorMetadata.find(m =>
          Utils.normalize(m.additionalMetadata.userFriendlyName) == normalizedName
        )
      )
  }

  private def resolveOperatorSource(
      page: Page,
      operatorName: String,
      operatorType: Option[String]
  ): Locator = {
    val byType =
      operatorType.filter(_.nonEmpty).map(t => page.getByTestId(s"operator-item-$t").first())
    byType.foreach { loc =>
      if (loc.count() > 0) return dragHandle(loc)
    }

    val leftPanel = page.locator("#left-container")
    val exactLabel = Utils.firstVisible(
      leftPanel.getByText(operatorName, new Locator.GetByTextOptions().setExact(true))
    )
    exactLabel.foreach { label =>
      val row = label
        .locator(
          "xpath=ancestor-or-self::*[@data-testid and starts-with(@data-testid,'operator-item-')][1]"
        )
        .first()
      if (row.count() > 0) return dragHandle(row)
      return dragHandle(label)
    }

    resolveByGroupPath(page, operatorName, operatorType).foreach(item => return dragHandle(item))

    val resultItems = page.locator("#left-container [data-testid^='operator-item-']")
    Utils.firstVisible(resultItems).foreach(item => return dragHandle(item))

    val fuzzy =
      Utils.firstVisible(leftPanel.locator(s".operator-label:has-text('$operatorName')"))
    fuzzy.map(dragHandle).getOrElse {
      throw new RuntimeException(
        s"Cannot find operator source for '$operatorName' (${operatorType.getOrElse("unknown")})"
      )
    }
  }

  private def resolveByGroupPath(
      page: Page,
      operatorName: String,
      operatorType: Option[String]
  ): Option[Locator] = {
    val metadata = metadataFor(operatorName, operatorType)
    val path = metadata
      .flatMap(m => groupPathByName.get(m.additionalMetadata.operatorGroupName))
      .getOrElse(Seq.empty)
    if (path.isEmpty) return None

    val leftPanel = page.locator("#left-container")
    var scope: Locator = leftPanel

    path.zipWithIndex.foreach {
      case (group, depth) =>
        val header =
          findHeaderInScope(scope, group).orElse(findHeaderByDepth(leftPanel, group, depth)).orNull
        if (header == null || header.count() == 0) return None

        val panel =
          header.locator("xpath=ancestor::*[contains(@class,'ant-collapse-item')][1]").first()
        if (panel.count() > 0) {
          val panelClass = Option(panel.getAttribute("class")).getOrElse("")
          if (!panelClass.contains("ant-collapse-item-active")) {
            clickGroupHeader(page, header)
            page.waitForTimeout(Delays.Tick)
            val afterClass = Option(panel.getAttribute("class")).getOrElse("")
            if (!afterClass.contains("ant-collapse-item-active")) {
              clickGroupHeader(page, header)
              page.waitForTimeout(Delays.Tick)
            }
          }
          scope = panel
        } else {
          // Fallback for non-collapse style groups.
          clickGroupHeader(page, header)
          page.waitForTimeout(Delays.Tick)
          scope = leftPanel
        }
    }

    operatorType.filter(_.nonEmpty).foreach { t =>
      val candidate = scope.getByTestId(s"operator-item-$t").first()
      if (candidate.count() > 0) {
        try candidate.scrollIntoViewIfNeeded()
        catch { case _: Exception => }
        page.waitForTimeout(Delays.Tick)
        return Some(candidate)
      }
    }

    val exact = scope.getByText(operatorName, new Locator.GetByTextOptions().setExact(true)).first()
    if (exact.count() == 0) return None
    val row = exact
      .locator(
        "xpath=ancestor-or-self::*[@data-testid and starts-with(@data-testid,'operator-item-')][1]"
      )
      .first()
    val resolved = if (row.count() > 0) row else exact
    try resolved.scrollIntoViewIfNeeded()
    catch { case _: Exception => }
    page.waitForTimeout(Delays.Tick)
    Some(resolved)
  }

  // One evaluateAll instead of per-header visibility/text reads: each read is a
  // driver round-trip, and a 10-header list adds visible idle time to the recording.
  private def scanHeaders(headers: Locator): Seq[(Boolean, String)] = {
    import scala.jdk.CollectionConverters._
    val raw = headers.evaluateAll("els => els.map(e => [e.offsetParent !== null, e.innerText])")
    raw
      .asInstanceOf[java.util.List[java.util.List[Object]]]
      .asScala
      .toSeq
      .map { pair =>
        val visible = pair.get(0).asInstanceOf[Boolean]
        val label = Option(pair.get(1)).map(_.toString).getOrElse("")
        (visible, label.replaceAll("\\s+", " ").trim)
      }
  }

  private def findHeaderInScope(scope: Locator, groupName: String): Option[Locator] = {
    val headers = scope.locator(".ant-collapse-header")
    val infos = scanHeaders(headers)
    val target = Utils.normalize(groupName)

    def pick(matches: String => Boolean): Option[Locator] =
      infos.zipWithIndex.collectFirst {
        case ((true, label), i) if matches(Utils.normalize(label)) => headers.nth(i)
      }

    // Exact first — avoids matching a parent header whose name is a substring
    // of the target (e.g. "Sklearn" when looking for "Sklearn Training").
    pick(_ == target).orElse(pick(n => n.contains(target) || target.contains(n)))
  }

  private def findHeaderByDepth(root: Locator, groupName: String, depth: Int): Option[Locator] = {
    val headers = root.locator(s".operator-group[data-depth='$depth'] .ant-collapse-header")
    val infos = scanHeaders(headers)
    val target = Utils.normalize(groupName)
    infos.zipWithIndex.collectFirst {
      case ((true, label), i) if {
            val n = Utils.normalize(label)
            n == target || n.contains(target) || target.contains(n)
          } =>
        headers.nth(i)
    }
  }

  private def clickGroupHeader(page: Page, header: Locator): Unit = {
    try header.scrollIntoViewIfNeeded()
    catch { case _: Exception => }
    val arrow = header.locator(".ant-collapse-arrow, i.anticon-right, i.anticon-down").first()
    if (arrow.count() > 0) {
      try Utils.clickWithCursor(page, arrow, steps = 10)
      catch { case _: Exception => Utils.clickWithCursor(page, header, steps = 10) }
    } else {
      Utils.clickWithCursor(page, header, steps = 10)
    }
  }
}
