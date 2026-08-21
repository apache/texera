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

class CoreExecutorReflectionSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // OperatorExecutor trait defaults
  // ---------------------------------------------------------------------------

  private val schema: Schema = Schema().add(new Attribute("v", AttributeType.INTEGER))
  private def tuple(v: Int): Tuple =
    Tuple.builder(schema).add(schema.getAttribute("v"), Integer.valueOf(v)).build()

  /** Minimal concrete subclass — only `processTuple` is abstract. */
  private class IdentityExec extends OperatorExecutor {
    override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] =
      Iterator.single(tuple)
  }

  "OperatorExecutor.open" should "default to a no-op" in {
    val exec = new IdentityExec
    exec.open() // should not throw
    succeed
  }

  "OperatorExecutor.close" should "default to a no-op" in {
    val exec = new IdentityExec
    exec.close()
    succeed
  }

  "OperatorExecutor.open / close" should
    "honor a subclass override (counting invocations across the lifecycle)" in {
    // Pin that the defaults are real method dispatches, not direct calls
    // to no-op stubs — a subclass override must be reachable from any
    // call site that invokes open / close on the trait.
    class CountingExec extends OperatorExecutor {
      var opens = 0
      var closes = 0
      override def open(): Unit = opens += 1
      override def close(): Unit = closes += 1
      override def processTuple(t: Tuple, p: Int): Iterator[TupleLike] = Iterator.empty
    }
    val exec = new CountingExec
    exec.open()
    exec.open()
    exec.close()
    assert(exec.opens == 2)
    assert(exec.closes == 1)
  }

  "OperatorExecutor.produceStateOnStart" should "default to None for any port" in {
    val exec = new IdentityExec
    assert(exec.produceStateOnStart(0).isEmpty)
    assert(exec.produceStateOnStart(7).isEmpty)
  }

  "OperatorExecutor.processState" should "default to passing the state through unchanged" in {
    val exec = new IdentityExec
    val state = State(Map[String, Any]("k" -> 1))
    assert(exec.processState(state, 0).contains(state))
  }

  "OperatorExecutor.processTupleMultiPort" should "default to delegating to processTuple with no port routing" in {
    val exec = new IdentityExec
    val out = exec.processTupleMultiPort(tuple(1), 0).toList
    assert(out.size == 1)
    assert(out.head._1.asInstanceOf[Tuple] == tuple(1))
    assert(out.head._2.isEmpty)
  }

  it should "forward the input port id to the underlying processTuple verbatim" in {
    // Pin: the default wrapper must hand the same `port` to the subclass
    // override, not substitute a constant. A regression that hard-codes
    // port = 0 would be invisible to the basic delegation test above.
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

  it should "produce as many output pairs as the underlying processTuple emits (zero / many fan-out)" in {
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

  "OperatorExecutor.produceStateOnFinish" should "default to None for any port" in {
    val exec = new IdentityExec
    assert(exec.produceStateOnFinish(0).isEmpty)
  }

  "OperatorExecutor.onFinish" should "default to an empty iterator" in {
    val exec = new IdentityExec
    assert(exec.onFinish(0).isEmpty)
  }

  "OperatorExecutor.onFinishMultiPort" should "default to delegating to onFinish with no port routing" in {
    val exec = new IdentityExec
    assert(exec.onFinishMultiPort(0).isEmpty)
  }

  // ---------------------------------------------------------------------------
  // SourceOperatorExecutor trait defaults
  // ---------------------------------------------------------------------------

  private class CountingSource extends SourceOperatorExecutor {
    override def produceTuple(): Iterator[TupleLike] =
      List(tuple(1), tuple(2), tuple(3)).iterator
  }

  "SourceOperatorExecutor.processTuple" should "always return an empty iterator" in {
    val exec = new CountingSource
    assert(exec.processTuple(tuple(99), 0).isEmpty)
  }

  "SourceOperatorExecutor.processTupleMultiPort" should "always return an empty iterator" in {
    val exec = new CountingSource
    assert(exec.processTupleMultiPort(tuple(99), 0).isEmpty)
  }

  it should "never invoke produceTuple on the input-side path" in {
    // Sources only emit through onFinishMultiPort. A regression in
    // processTupleMultiPort that accidentally drained produceTuple
    // (discarding the output) wouldn't be caught by an empty-iterator
    // check alone — pin via a counter.
    var producedCalls = 0
    val src = new SourceOperatorExecutor {
      override def produceTuple(): Iterator[TupleLike] = {
        producedCalls += 1
        Iterator.empty
      }
    }
    src.processTupleMultiPort(tuple(0), 0).toList
    assert(producedCalls == 0, "input-side path must not call produceTuple")
  }

  "SourceOperatorExecutor.onFinishMultiPort" should "delegate to produceTuple with no port routing" in {
    val exec = new CountingSource
    val out = exec.onFinishMultiPort(0).toList
    assert(out.size == 3)
    assert(out.map(_._1.asInstanceOf[Tuple]) == List(tuple(1), tuple(2), tuple(3)))
    assert(out.forall(_._2.isEmpty))
  }

  // ---------------------------------------------------------------------------
  // ExecFactory.newExecFromJavaClassName
  // ---------------------------------------------------------------------------

  "ExecFactory.newExecFromJavaClassName" should "instantiate a no-arg constructor when no descString is given" in {
    val exec = ExecFactory.newExecFromJavaClassName(
      classOf[CoreExecutorReflectionSpec.NoArgExec].getName
    )
    assert(exec.isInstanceOf[CoreExecutorReflectionSpec.NoArgExec])
  }

  it should "instantiate a (String) constructor when descString is provided" in {
    val exec = ExecFactory.newExecFromJavaClassName(
      classOf[CoreExecutorReflectionSpec.StringArgExec].getName,
      descString = "hello"
    )
    val typed = exec.asInstanceOf[CoreExecutorReflectionSpec.StringArgExec]
    assert(typed.desc == "hello")
  }

  it should "fall back to (Int, Int) constructor for parallelizable executors with no descString" in {
    val exec = ExecFactory.newExecFromJavaClassName(
      classOf[CoreExecutorReflectionSpec.IdxCountExec].getName,
      idx = 3,
      workerCount = 7
    )
    val typed = exec.asInstanceOf[CoreExecutorReflectionSpec.IdxCountExec]
    assert(typed.idx == 3)
    assert(typed.workerCount == 7)
  }

  it should "fall back to (String, Int, Int) constructor when descString is given" in {
    val exec = ExecFactory.newExecFromJavaClassName(
      classOf[CoreExecutorReflectionSpec.StringIdxCountExec].getName,
      descString = "hi",
      idx = 1,
      workerCount = 4
    )
    val typed = exec.asInstanceOf[CoreExecutorReflectionSpec.StringIdxCountExec]
    assert(typed.desc == "hi")
    assert(typed.idx == 1)
    assert(typed.workerCount == 4)
  }

  it should "raise ClassNotFoundException for unknown class names" in {
    assertThrows[ClassNotFoundException] {
      ExecFactory.newExecFromJavaClassName("does.not.exist.AtAll")
    }
  }

  it should "propagate NoSuchMethodException when no constructor matches either factory branch" in {
    // A fixture with only a `(Long)` constructor — neither the (no-arg)
    // branch (which also tries (Int, Int) on catch) nor the (String)
    // branch (which falls back to (String, Int, Int)) matches a single
    // `Long` argument. Both throws propagate as NoSuchMethodException.
    assertThrows[NoSuchMethodException] {
      ExecFactory.newExecFromJavaClassName(
        classOf[CoreExecutorReflectionSpec.LongArgExec].getName
      )
    }
  }

  // ---------------------------------------------------------------------------
  // ExecFactory.newExecFromJavaCode
  //
  // `compileCode` passes null compilation options, so the system javac resolves
  // types against its own default classpath — `java.class.path`. This module sets
  // `Test / fork := true`, so the forked test JVM is launched with the full test
  // classpath on `-cp` and javac therefore does see workflow-core's own classes;
  // a source string that names `OperatorExecutor` compiles here.
  // ---------------------------------------------------------------------------

  /** Java source for a UDF class implementing the trait; `single` echoes the input tuple. */
  private val echoUDFSource: String =
    """public class JavaUDFOpExec
      |    implements org.apache.texera.amber.core.executor.OperatorExecutor {
      |    public scala.collection.Iterator<org.apache.texera.amber.core.tuple.TupleLike>
      |            processTuple(org.apache.texera.amber.core.tuple.Tuple tuple, int port) {
      |        return scala.collection.Iterator$.MODULE$.single(tuple);
      |    }
      |}""".stripMargin

  "ExecFactory.newExecFromJavaCode" should "compile java source into a live OperatorExecutor" in {
    val exec = ExecFactory.newExecFromJavaCode(echoUDFSource)
    assert(exec.getClass.getName == "org.apache.texera.amber.operators.udf.java.JavaUDFOpExec")
    // Reachable through the trait, not merely castable to it: dispatching
    // processTuple must run the freshly compiled override...
    assert(exec.processTuple(tuple(7), 0).toList == List(tuple(7)))
    // ...and the class must inherit the trait's defaults for what it left out.
    assert(exec.onFinish(0).isEmpty)
    assert(exec.produceStateOnStart(0).isEmpty)
  }

  it should "hand back an independent instance on every call" in {
    // The factory constructs per call; two executors compiled from the same
    // source must not be the same object (nor share a class-level cache).
    val first = ExecFactory.newExecFromJavaCode(echoUDFSource)
    val second = ExecFactory.newExecFromJavaCode(echoUDFSource)
    assert(first ne second)
    assert(first.getClass.getName == second.getClass.getName)
  }

  it should "surface the compiler diagnostics when the java source does not compile" in {
    // A class that claims the trait but never implements `processTuple` is
    // rejected by javac, so no instance is ever constructed.
    val ex = intercept[RuntimeException] {
      ExecFactory.newExecFromJavaCode(
        """public class JavaUDFOpExec
          |    implements org.apache.texera.amber.core.executor.OperatorExecutor {
          |}""".stripMargin
      )
    }
    assert(ex.getMessage.contains("Error at line"))
  }

  // ---------------------------------------------------------------------------
  // JavaRuntimeCompilation.compileCode
  // ---------------------------------------------------------------------------

  "JavaRuntimeCompilation.compileCode" should "compile a self-contained Java class with no external deps" in {
    val src =
      """public class JavaUDFOpExec {
        |    public int compute() { return 42; }
        |}""".stripMargin
    val cls = JavaRuntimeCompilation.compileCode(src)
    assert(cls.getName == "org.apache.texera.amber.operators.udf.java.JavaUDFOpExec")
    val instance = cls.getDeclaredConstructor().newInstance()
    val result = cls.getMethod("compute").invoke(instance).asInstanceOf[Integer]
    assert(result == 42)
  }

  it should "raise RuntimeException with diagnostics when the source has syntax errors" in {
    val ex = intercept[RuntimeException] {
      JavaRuntimeCompilation.compileCode("public class Garbage { not valid java }")
    }
    assert(ex.getMessage.contains("Error at line"))
  }
}

private object CoreExecutorReflectionSpec {
  // Public so reflection inside ExecFactory can reach the no-arg constructor.
  class NoArgExec extends OperatorExecutor {
    override def processTuple(
        tuple: org.apache.texera.amber.core.tuple.Tuple,
        port: Int
    ): Iterator[org.apache.texera.amber.core.tuple.TupleLike] = Iterator.empty
  }

  class StringArgExec(val desc: String) extends OperatorExecutor {
    override def processTuple(
        tuple: org.apache.texera.amber.core.tuple.Tuple,
        port: Int
    ): Iterator[org.apache.texera.amber.core.tuple.TupleLike] = Iterator.empty
  }

  class IdxCountExec(val idx: Int, val workerCount: Int) extends OperatorExecutor {
    override def processTuple(
        tuple: org.apache.texera.amber.core.tuple.Tuple,
        port: Int
    ): Iterator[org.apache.texera.amber.core.tuple.TupleLike] = Iterator.empty
  }

  class StringIdxCountExec(val desc: String, val idx: Int, val workerCount: Int)
      extends OperatorExecutor {
    override def processTuple(
        tuple: org.apache.texera.amber.core.tuple.Tuple,
        port: Int
    ): Iterator[org.apache.texera.amber.core.tuple.TupleLike] = Iterator.empty
  }

  /** Only a `(Long)` constructor — neither factory branch matches. */
  class LongArgExec(val n: Long) extends OperatorExecutor {
    override def processTuple(
        tuple: org.apache.texera.amber.core.tuple.Tuple,
        port: Int
    ): Iterator[org.apache.texera.amber.core.tuple.TupleLike] = Iterator.empty
  }
}
