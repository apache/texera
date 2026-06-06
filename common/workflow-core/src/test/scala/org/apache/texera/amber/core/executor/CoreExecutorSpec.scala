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

package org.apache.texera.amber.core.executor

import org.apache.texera.amber.core.state.State
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple, TupleLike}
import org.scalatest.flatspec.AnyFlatSpec

class CoreExecutorSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Shared fixtures
  // ---------------------------------------------------------------------------

  private val intAttr = new Attribute("v", AttributeType.INTEGER)
  private val schema: Schema = Schema().add(intAttr)
  private def tuple(value: Int): Tuple =
    Tuple.builder(schema).add(intAttr, value).build()

  /** Minimal concrete OperatorExecutor that overrides only `processTuple`. */
  private class IdentityExecutor extends OperatorExecutor {
    override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] =
      Iterator.single(tuple)
  }

  /** Counts how many times each lifecycle method runs. */
  private class CountingExecutor extends OperatorExecutor {
    var opens = 0
    var closes = 0
    override def open(): Unit = opens += 1
    override def close(): Unit = closes += 1
    override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] =
      Iterator.empty
  }

  // ---------------------------------------------------------------------------
  // OperatorExecutor defaults
  // ---------------------------------------------------------------------------

  "OperatorExecutor.open / close" should "default to no-ops that can be called without side effects" in {
    val exec = new IdentityExecutor
    // Calling the defaults must not throw and must not change observable state.
    exec.open()
    exec.close()
    exec.open()
    succeed
  }

  it should "honor a subclass override (counting invocations across the lifecycle)" in {
    val exec = new CountingExecutor
    exec.open()
    exec.open()
    exec.close()
    assert(exec.opens == 2)
    assert(exec.closes == 1)
  }

  "OperatorExecutor.produceStateOnStart" should "default to None for both ports" in {
    val exec = new IdentityExecutor
    assert(exec.produceStateOnStart(0).isEmpty)
    assert(exec.produceStateOnStart(42).isEmpty)
  }

  "OperatorExecutor.processState" should "default to returning the same state wrapped in Some" in {
    val exec = new IdentityExecutor
    val s = State(Map("k" -> 1, "v" -> "two"))
    val out = exec.processState(s, 0)
    assert(out.contains(s))
  }

  "OperatorExecutor.produceStateOnFinish" should "default to None for both ports" in {
    val exec = new IdentityExecutor
    assert(exec.produceStateOnFinish(0).isEmpty)
    assert(exec.produceStateOnFinish(42).isEmpty)
  }

  "OperatorExecutor.onFinish" should "default to Iterator.empty" in {
    val exec = new IdentityExecutor
    assert(exec.onFinish(0).isEmpty)
    assert(exec.onFinish(7).isEmpty)
  }

  // ---------------------------------------------------------------------------
  // OperatorExecutor multi-port wrappers
  // ---------------------------------------------------------------------------

  "OperatorExecutor.processTupleMultiPort" should "wrap each processTuple output with Option.empty port" in {
    // The default implementation must invoke the subclass's processTuple and
    // pair each emitted tuple with `None`. Pin so a regression that hard-codes
    // a specific port id breaks here.
    val exec = new IdentityExecutor
    val out = exec.processTupleMultiPort(tuple(7), port = 0).toList
    assert(out.size == 1)
    val (t, portOpt) = out.head
    assert(t == tuple(7))
    assert(portOpt.isEmpty)
  }

  it should "forward the input port to the underlying processTuple" in {
    // The default wrapper must hand the same `port` to `processTuple`, not
    // substitute a constant. Pin by capturing the port observed inside the
    // subclass.
    var seenPort = -1
    val exec = new OperatorExecutor {
      override def processTuple(t: Tuple, p: Int): Iterator[TupleLike] = {
        seenPort = p
        Iterator.single(t)
      }
    }
    exec.processTupleMultiPort(tuple(0), port = 9).toList
    assert(seenPort == 9)
  }

  it should "produce as many output pairs as the underlying processTuple emits (zero / many)" in {
    val empty = new OperatorExecutor {
      override def processTuple(t: Tuple, p: Int): Iterator[TupleLike] = Iterator.empty
    }
    assert(empty.processTupleMultiPort(tuple(0), 0).isEmpty)

    val fanOut = new OperatorExecutor {
      override def processTuple(t: Tuple, p: Int): Iterator[TupleLike] =
        Iterator(tuple(1), tuple(2), tuple(3))
    }
    val outs = fanOut.processTupleMultiPort(tuple(0), 0).toList
    assert(outs.size == 3)
    assert(outs.forall(_._2.isEmpty), "every emitted pair must have port = None under the default")
  }

  "OperatorExecutor.onFinishMultiPort" should "wrap each onFinish output with Option.empty port" in {
    val exec = new OperatorExecutor {
      override def processTuple(t: Tuple, p: Int): Iterator[TupleLike] = Iterator.empty
      override def onFinish(port: Int): Iterator[TupleLike] =
        Iterator(tuple(10), tuple(20))
    }
    val outs = exec.onFinishMultiPort(0).toList
    assert(outs.size == 2)
    assert(outs.map(_._1) == List(tuple(10), tuple(20)))
    assert(outs.forall(_._2.isEmpty))
  }

  it should "be empty by default (no onFinish override)" in {
    val exec = new IdentityExecutor
    assert(exec.onFinishMultiPort(0).isEmpty)
  }

  // ---------------------------------------------------------------------------
  // SourceOperatorExecutor overrides
  // ---------------------------------------------------------------------------

  /** Minimal source that emits a fixed-size tuple sequence. */
  private class FixedSource(values: Seq[Int]) extends SourceOperatorExecutor {
    override def produceTuple(): Iterator[TupleLike] = values.iterator.map(tuple)
  }

  "SourceOperatorExecutor.open / close" should "be no-op overrides on the source path" in {
    val src = new FixedSource(Seq.empty)
    src.open()
    src.close()
    succeed
  }

  "SourceOperatorExecutor.processTuple" should "always return Iterator.empty (sources do not consume input)" in {
    val src = new FixedSource(Seq(1, 2))
    assert(src.processTuple(tuple(99), port = 0).isEmpty)
  }

  "SourceOperatorExecutor.processTupleMultiPort" should
    "return Iterator.empty without invoking produceTuple (input-side path)" in {
    var producedCalls = 0
    val src = new SourceOperatorExecutor {
      override def produceTuple(): Iterator[TupleLike] = {
        producedCalls += 1
        Iterator.empty
      }
    }
    assert(src.processTupleMultiPort(tuple(0), 0).isEmpty)
    assert(producedCalls == 0, "input-side path must not call produceTuple")
  }

  "SourceOperatorExecutor.onFinishMultiPort" should
    "delegate to produceTuple and wrap each emitted tuple with Option.empty port" in {
    val src = new FixedSource(Seq(7, 8, 9))
    val outs = src.onFinishMultiPort(port = 0).toList
    assert(outs.size == 3)
    assert(outs.map(_._1) == List(tuple(7), tuple(8), tuple(9)))
    assert(outs.forall(_._2.isEmpty))
  }

  // ---------------------------------------------------------------------------
  // ExecFactory.newExecFromJavaClassName
  // ---------------------------------------------------------------------------

  // The factory uses Class.forName.getDeclaredConstructor reflection, so the
  // test fixtures need to be top-level (non-anonymous) classes so they have
  // accessible no-arg / (String) / (Int, Int) constructors. They live in the
  // companion object below.

  "ExecFactory.newExecFromJavaClassName" should "construct via the no-arg constructor when descString is empty" in {
    val name = classOf[CoreExecutorSpec.NoArgFixture].getName
    val exec = ExecFactory.newExecFromJavaClassName(name)
    assert(exec.isInstanceOf[CoreExecutorSpec.NoArgFixture])
  }

  it should "construct via the (String) constructor when descString is non-empty" in {
    val name = classOf[CoreExecutorSpec.StringArgFixture].getName
    val exec = ExecFactory.newExecFromJavaClassName(name, descString = "hello")
    assert(exec.isInstanceOf[CoreExecutorSpec.StringArgFixture])
    assert(exec.asInstanceOf[CoreExecutorSpec.StringArgFixture].desc == "hello")
  }

  it should "fall back to the (Int, Int) constructor when no (no-arg) constructor exists" in {
    // The (String, Int) shape exists for the (String) overload, but here we
    // construct without a descString — that triggers `getDeclaredConstructor()`
    // which throws NoSuchMethodException, so the catch falls back to
    // `getDeclaredConstructor(classOf[Int], classOf[Int])`.
    val name = classOf[CoreExecutorSpec.IntIntFixture].getName
    val exec = ExecFactory.newExecFromJavaClassName(name, idx = 3, workerCount = 7)
    val concrete = exec.asInstanceOf[CoreExecutorSpec.IntIntFixture]
    assert(concrete.idx == 3 && concrete.workerCount == 7)
  }

  it should "fall back to the (String, Int, Int) constructor when no (String) constructor exists" in {
    val name = classOf[CoreExecutorSpec.StringIntIntFixture].getName
    val exec =
      ExecFactory.newExecFromJavaClassName(name, descString = "x", idx = 1, workerCount = 4)
    val concrete = exec.asInstanceOf[CoreExecutorSpec.StringIntIntFixture]
    assert(concrete.desc == "x" && concrete.idx == 1 && concrete.workerCount == 4)
  }

  it should "throw ClassNotFoundException when the class name does not resolve" in {
    intercept[ClassNotFoundException] {
      ExecFactory.newExecFromJavaClassName("org.apache.texera.does.not.Exist")
    }
  }

  it should "propagate NoSuchMethodException when no matching constructor exists in either branch" in {
    // The fixture has only a `(Long)` constructor — neither the
    // (no-arg) nor the `(Int, Int)` fallback fits. Both branches throw
    // NoSuchMethodException, so the caller sees it propagate.
    val name = classOf[CoreExecutorSpec.LongArgFixture].getName
    intercept[NoSuchMethodException] {
      ExecFactory.newExecFromJavaClassName(name)
    }
  }

  // ---------------------------------------------------------------------------
  // ExecFactory.newExecFromJavaCode
  // ---------------------------------------------------------------------------

  "ExecFactory.newExecFromJavaCode" should
    "raise RuntimeException with compiler diagnostics when the source has syntax errors" in {
    // NOTE: a happy-path test that compiles a class implementing
    // OperatorExecutor would need the Texera classes on javac's classpath,
    // but the in-memory compiler used by JavaRuntimeCompilation does not
    // inherit sbt's test classpath (the compiler task gets a fresh
    // ToolProvider.getSystemJavaCompiler with null file-manager paths).
    // So the success path is covered indirectly by production usage; here
    // we exercise only the diagnostic-collection branch with a broken
    // source.
    val brokenSrc = "public class JavaUDFOpExec { this is not valid Java; }"
    val ex = intercept[RuntimeException] {
      ExecFactory.newExecFromJavaCode(brokenSrc)
    }
    assert(
      ex.getMessage.toLowerCase.contains("error"),
      s"expected diagnostic-bearing error message, got: ${ex.getMessage}"
    )
  }
}

object CoreExecutorSpec {
  // Concrete reflection fixtures referenced by the ExecFactory tests above.
  // They live on the companion (top-level binary names) so
  // `Class.forName(...).getDeclaredConstructor(...)` can locate them.

  class NoArgFixture extends OperatorExecutor {
    override def processTuple(t: Tuple, p: Int): Iterator[TupleLike] = Iterator.empty
  }

  class StringArgFixture(val desc: String) extends OperatorExecutor {
    override def processTuple(t: Tuple, p: Int): Iterator[TupleLike] = Iterator.empty
  }

  class IntIntFixture(val idx: Int, val workerCount: Int) extends OperatorExecutor {
    override def processTuple(t: Tuple, p: Int): Iterator[TupleLike] = Iterator.empty
  }

  class StringIntIntFixture(val desc: String, val idx: Int, val workerCount: Int)
      extends OperatorExecutor {
    override def processTuple(t: Tuple, p: Int): Iterator[TupleLike] = Iterator.empty
  }

  /** Only a `(Long)` constructor — neither factory branch matches it. */
  class LongArgFixture(val n: Long) extends OperatorExecutor {
    override def processTuple(t: Tuple, p: Int): Iterator[TupleLike] = Iterator.empty
  }
}
