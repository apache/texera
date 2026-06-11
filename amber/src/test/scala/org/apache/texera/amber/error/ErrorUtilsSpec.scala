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

package org.apache.texera.amber.error

import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.ConsoleMessageType.ERROR
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.{ControlError, ErrorLanguage}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.control.ControlThrowable

class ErrorUtilsSpec extends AnyFlatSpec with Matchers {

  // ----- safely -----

  "safely" should "rethrow ControlThrowable even when the handler is defined for it" in {
    val ct = new ControlThrowable {}
    val swallowAll: PartialFunction[Throwable, String] = { case _ => "swallowed" }
    val wrapped = ErrorUtils.safely(swallowAll)
    val thrown = intercept[ControlThrowable](wrapped(ct))
    thrown should be theSameInstanceAs ct
  }

  it should "delegate to the supplied handler when it is defined for the throwable" in {
    val handler: PartialFunction[Throwable, String] = {
      case e: IllegalStateException => s"handled:${e.getMessage}"
    }
    val wrapped = ErrorUtils.safely(handler)
    wrapped(new IllegalStateException("boom")) shouldBe "handled:boom"
  }

  it should "leave the wrapped partial function undefined for unhandled throwables" in {
    // The wrapped PartialFunction must report isDefinedAt=false for inputs the
    // user's handler does not cover, so callers can fall through to other
    // catch clauses.
    val handler: PartialFunction[Throwable, String] = {
      case _: IllegalStateException => "ok"
    }
    val wrapped = ErrorUtils.safely(handler)
    wrapped.isDefinedAt(new RuntimeException("nope")) shouldBe false
  }

  // ----- mkConsoleMessage -----

  "mkConsoleMessage" should "use Unknown Source when the throwable has no stack frames" in {
    val err = new RuntimeException("kaboom")
    err.setStackTrace(Array.empty)
    val msg = ErrorUtils.mkConsoleMessage(ActorVirtualIdentity("worker-A"), err)
    msg.workerId shouldBe "worker-A"
    msg.source shouldBe "(Unknown Source)"
    msg.title shouldBe err.toString
    msg.msgType shouldBe ERROR
    msg.message shouldBe ""
  }

  it should "encode the top stack frame as (file:line) when available" in {
    val err = new RuntimeException("kaboom")
    err.setStackTrace(
      Array(new StackTraceElement("com.x.Foo", "bar", "Foo.scala", 42))
    )
    val msg = ErrorUtils.mkConsoleMessage(ActorVirtualIdentity("worker-A"), err)
    msg.source shouldBe "(Foo.scala:42)"
    msg.message should include("Foo.scala")
  }

  // ----- mkControlError -----

  "mkControlError" should "leave errorDetails empty and language=SCALA when the cause is null" in {
    val err = new RuntimeException("no-cause")
    err.setStackTrace(Array(new StackTraceElement("Cls", "m", "F.scala", 7)))
    val ce = ErrorUtils.mkControlError(err)
    ce.errorMessage shouldBe err.toString
    ce.errorDetails shouldBe ""
    ce.language shouldBe ErrorLanguage.SCALA
    ce.stackTrace should startWith("at ")
    ce.stackTrace should include("F.scala:7")
  }

  it should "populate errorDetails with the cause's toString when present" in {
    val cause = new IllegalStateException("root")
    val err = new RuntimeException("outer", cause)
    val ce = ErrorUtils.mkControlError(err)
    ce.errorMessage shouldBe err.toString
    ce.errorDetails shouldBe cause.toString
  }

  // ----- reconstructThrowable -----

  "reconstructThrowable" should "skip stack-trace parsing for PYTHON-language errors" in {
    // Pin: PYTHON path returns a bare new Throwable(message) and never
    // touches the supplied errorDetails/stackTrace strings. The reconstructed
    // throwable will still carry the JVM-captured stack from `new Throwable`,
    // so the test only asserts what's specific to this branch.
    val ce = ControlError(
      "py.boom",
      "ignored-details",
      "at com.x.Foo.bar(Foo.scala:42)",
      ErrorLanguage.PYTHON
    )
    val reconstructed = ErrorUtils.reconstructThrowable(ce)
    reconstructed.getMessage shouldBe "py.boom"
    reconstructed.getCause shouldBe null
    // None of the parsed-stack frames should leak through on the Python path.
    reconstructed.getStackTrace.exists(f => f.getClassName == "com.x.Foo.bar") shouldBe false
  }

  it should "leave the cause null when errorDetails is empty for SCALA errors" in {
    val ce = ControlError("scala.boom", "", "", ErrorLanguage.SCALA)
    val reconstructed = ErrorUtils.reconstructThrowable(ce)
    reconstructed.getMessage shouldBe "scala.boom"
    reconstructed.getCause shouldBe null
  }

  it should "attach a cause Throwable when errorDetails is non-empty" in {
    val ce = ControlError("scala.boom", "root-cause", "", ErrorLanguage.SCALA)
    val reconstructed = ErrorUtils.reconstructThrowable(ce)
    reconstructed.getCause should not be null
    reconstructed.getCause.getMessage shouldBe "root-cause"
  }

  it should "parse stacktrace lines that match the at-className(location) pattern" in {
    val ce = ControlError(
      "scala.boom",
      "",
      "at com.x.Foo.bar(Foo.scala:42)\nat com.x.Baz.qux(Baz.scala:7)",
      ErrorLanguage.SCALA
    )
    val reconstructed = ErrorUtils.reconstructThrowable(ce)
    val frames = reconstructed.getStackTrace
    frames.length shouldBe 2
    frames(0).getClassName shouldBe "com.x.Foo.bar"
    frames(0).getFileName shouldBe "Foo.scala:42"
    frames(1).getClassName shouldBe "com.x.Baz.qux"
  }

  it should "drop lines that do not match the at-className(location) pattern" in {
    val ce = ControlError(
      "scala.boom",
      "",
      "garbage line\nat com.x.Foo.bar(Foo.scala:42)\nmore garbage",
      ErrorLanguage.SCALA
    )
    val reconstructed = ErrorUtils.reconstructThrowable(ce)
    reconstructed.getStackTrace.length shouldBe 1
  }

  // ----- getStackTraceWithAllCauses -----

  "getStackTraceWithAllCauses" should "use the developer header at the top level" in {
    val err = new RuntimeException("top")
    err.setStackTrace(Array.empty)
    val out = ErrorUtils.getStackTraceWithAllCauses(err)
    out should startWith("Stack trace for developers:")
    out should include(err.toString)
  }

  it should "recurse into nested causes with a Caused by section" in {
    val cause = new IllegalStateException("inner")
    cause.setStackTrace(Array.empty)
    val err = new RuntimeException("outer", cause)
    err.setStackTrace(Array.empty)
    val out = ErrorUtils.getStackTraceWithAllCauses(err)
    out should include("Caused by:")
    out should include("inner")
    out should include("outer")
  }

  // ----- getOperatorFromActorIdOpt -----

  "getOperatorFromActorIdOpt" should "default to unknown operator and empty worker id when the option is empty" in {
    ErrorUtils.getOperatorFromActorIdOpt(None) shouldBe ("unknown operator", "")
  }

  it should "extract operator id from a worker actor name following the WF/op/layer pattern" in {
    val actor = ActorVirtualIdentity("Worker:WF1-E1-myOp-main-0")
    val (operatorId, workerId) = ErrorUtils.getOperatorFromActorIdOpt(Some(actor))
    // The pattern is Worker:WF<n>-<operator>-<layer>-<id>; greedy on operator,
    // so layer=`main`, workerIdx=`0`, and the operator captures `E1-myOp`.
    operatorId shouldBe "E1-myOp"
    workerId shouldBe "Worker:WF1-E1-myOp-main-0"
  }

  it should "fall back to the dummy operator id for actor names that do not match the pattern" in {
    val actor = ActorVirtualIdentity("CONTROLLER")
    val (operatorId, workerId) = ErrorUtils.getOperatorFromActorIdOpt(Some(actor))
    operatorId shouldBe "__DummyOperator"
    workerId shouldBe "CONTROLLER"
  }
}
