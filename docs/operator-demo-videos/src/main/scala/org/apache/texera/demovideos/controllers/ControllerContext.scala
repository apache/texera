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

import com.microsoft.playwright.Page
import scala.collection.mutable.ArrayBuffer

/**
  * Shared context wrapping Playwright Page, passed through all builders.
  */
class ControllerContext(val page: Page) {
  private var _fakeCursorInstalled: Boolean = false

  def ensureFakeCursor(): Unit = {
    if (!_fakeCursorInstalled) {
      Utils.installFakeCursor(page)
      _fakeCursorInstalled = true
    }
  }
}

/** A single named step that runs against a ControllerContext. */
trait ControllerStep {
  def name: String
  def run(ctx: ControllerContext): Unit
}

object ControllerStep {
  def apply(stepName: String)(action: ControllerContext => Unit): ControllerStep =
    new ControllerStep {
      override def name: String = stepName
      override def run(ctx: ControllerContext): Unit = action(ctx)
    }
}

/**
  * Base builder — subclasses accumulate steps via fluent API, then execute().
  *
  * Usage:
  *   new LoginControllerBuilder(ctx).login("u","p").logout().execute()
  */
abstract class ControllerBuilder(protected val context: ControllerContext) {
  private val steps: ArrayBuffer[ControllerStep] = ArrayBuffer.empty

  protected def addStep(step: ControllerStep): this.type = {
    steps += step
    this
  }

  def execute(): Unit = {
    steps.foreach { step =>
      println(s"[${step.name}] Executing...")
      step.run(context)
      println(s"[${step.name}] Done")
    }
  }
}
