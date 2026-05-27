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

  // ---------------------------------------------------------------------------
  // JavaRuntimeCompilation.compileCode
  //
  // A success-path test that compiles a real OperatorExecutor subclass from a
  // string is intentionally omitted: `compiler.getTask(...)` is invoked with
  // null compilation options, which means the system javac picks up its own
  // (test) classpath rather than the project classpath. Under sbt test that
  // does not include workflow-core itself, so the compile fails with
  // "package org.apache.texera... does not exist" — a deployment-environment
  // artifact rather than a contract violation. We exercise just the diagnostic
  // path here.
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
}
