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

package org.apache.texera.amber.util

import org.scalatest.funsuite.AnyFunSuite

class StackTraceUtilsSpec extends AnyFunSuite {

  private val TopLevelHeader = "Stack trace for developers: \n\n"
  private val CauseHeader = "\n\nCaused by:\n"

  /** Builds a throwable with no frames so the rendering can be asserted exactly. */
  private def frameless(err: Throwable): Throwable = {
    err.setStackTrace(Array.empty)
    err
  }

  private def occurrencesOf(haystack: String, needle: String): Int =
    haystack.sliding(needle.length).count(_ == needle)

  test("getStackTraceWithAllCauses renders a causeless throwable exactly") {
    val err = frameless(new RuntimeException("boom"))

    // header + toString + "\n" + (no frames) is the whole output.
    assert(
      StackTraceUtils.getStackTraceWithAllCauses(err) ==
        s"${TopLevelHeader}java.lang.RuntimeException: boom\n"
    )
  }

  test("getStackTraceWithAllCauses includes every real stack frame, newline-separated") {
    val err = new RuntimeException("with frames")
    val rendered = StackTraceUtils.getStackTraceWithAllCauses(err)

    assert(rendered.startsWith(TopLevelHeader))
    assert(rendered.contains("java.lang.RuntimeException: with frames"))
    // Frames are rendered verbatim, in order, after the throwable's own toString.
    val frameBlock = rendered.stripPrefix(s"${TopLevelHeader}${err.toString}\n")
    assert(frameBlock == err.getStackTrace.mkString("\n"))
    assert(err.getStackTrace.length > 1)
    assert(frameBlock.contains(this.getClass.getName))
  }

  test("getStackTraceWithAllCauses omits the Caused-by section when there is no cause") {
    val rendered = StackTraceUtils.getStackTraceWithAllCauses(new RuntimeException("lonely"))
    assert(!rendered.contains("Caused by:"))
  }

  test("getStackTraceWithAllCauses with topLevel=false swaps in the Caused-by header") {
    val err = frameless(new IllegalStateException("nested"))

    assert(
      StackTraceUtils.getStackTraceWithAllCauses(err, topLevel = false) ==
        s"${CauseHeader}java.lang.IllegalStateException: nested\n"
    )
    assert(
      !StackTraceUtils
        .getStackTraceWithAllCauses(err, topLevel = false)
        .contains("Stack trace for developers")
    )
  }

  test("getStackTraceWithAllCauses appends a single cause below the top-level trace") {
    val cause = frameless(new IllegalArgumentException("inner"))
    val err = frameless(new RuntimeException("outer", cause))

    assert(
      StackTraceUtils.getStackTraceWithAllCauses(err) ==
        s"${TopLevelHeader}java.lang.RuntimeException: outer\n" +
          s"${CauseHeader}java.lang.IllegalArgumentException: inner\n"
    )
  }

  test("getStackTraceWithAllCauses walks the whole chain, outermost first") {
    val root = frameless(new java.io.IOException("root"))
    val middle = frameless(new IllegalStateException("middle", root))
    val top = frameless(new RuntimeException("top", middle))

    val rendered = StackTraceUtils.getStackTraceWithAllCauses(top)

    // One developer header for the whole chain, one Caused-by per cause.
    assert(occurrencesOf(rendered, "Stack trace for developers") == 1)
    assert(occurrencesOf(rendered, "Caused by:") == 2)
    // Ordering is outermost -> innermost.
    assert(rendered.indexOf("top") < rendered.indexOf("middle"))
    assert(rendered.indexOf("middle") < rendered.indexOf("root"))
    assert(rendered.endsWith("java.io.IOException: root\n"))
  }

  test("getStackTraceWithAllCauses renders each cause's own frames, not just the top's") {
    val cause = new IllegalStateException("inner")
    val err = new RuntimeException("outer", cause)

    val rendered = StackTraceUtils.getStackTraceWithAllCauses(err)

    assert(rendered.contains(err.getStackTrace.head.toString))
    assert(rendered.contains(cause.getStackTrace.head.toString))
    // The cause's frames belong to the section that follows the Caused-by marker.
    val causeSection = rendered.substring(rendered.indexOf(CauseHeader))
    assert(causeSection.contains(cause.getStackTrace.head.toString))
  }

  test("getStackTraceWithAllCauses handles a throwable without a message") {
    val err = frameless(new RuntimeException())

    // Throwable.toString drops the ": <msg>" suffix when the message is null.
    assert(
      StackTraceUtils.getStackTraceWithAllCauses(err) ==
        s"${TopLevelHeader}java.lang.RuntimeException\n"
    )
  }

  test("getStackTraceWithAllCauses renders a deep cause chain without truncating it") {
    // The recursion is unbounded by design; a long chain must be rendered in full.
    val root = frameless(new RuntimeException("root"))
    val chain = (1 to 20).foldLeft[Throwable](root) { (cause, i) =>
      frameless(new RuntimeException(s"level-$i", cause))
    }

    val rendered = StackTraceUtils.getStackTraceWithAllCauses(chain)

    assert(occurrencesOf(rendered, "Caused by:") == 20)
    assert(rendered.startsWith(TopLevelHeader))
    assert(rendered.endsWith("java.lang.RuntimeException: root\n"))
  }
}
