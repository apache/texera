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

package org.apache.texera.amber.engine.architecture.worker

import org.apache.texera.amber.core.executor.{
  OpExecInitInfo,
  OperatorExecutor,
  OpExecWithClassName,
  OpExecWithCode
}
import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}
import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.{
  DPInputQueueElement,
  MainThreadDelegateMessage
}
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.scalatest.flatspec.AnyFlatSpec

import java.util.concurrent.LinkedBlockingQueue

/**
  * `DataProcessorRPCHandlerInitializer` is mostly a mixin point — it stacks every worker-side
  * promise handler onto one object and exposes `dp` to them. Its own logic is `setupExecutor`,
  * the shared body behind both `initializeExecutor` (first construction) and `updateExecutor`
  * (reconfiguration): it picks a construction strategy from the `OpExecInitInfo` variant and
  * installs the result on the `DataProcessor`.
  *
  * The tests below pin which variant maps to which strategy and that the surrounding arguments
  * (`descString`, `workerIdx`, `workerCount`) reach it — `ExecFactory`'s own constructor-resolution
  * rules are already pinned by `CoreExecutorReflectionSpec` and are not re-asserted here.
  *
  * Deliberately NOT covered, and why: `debugCommand`, `evaluatePythonExpression`,
  * `retryCurrentTuple` and `noOperation` are all `???`. Asserting that they raise
  * `NotImplementedError` would only restate a Scala standard-library default and would cement
  * "unimplemented" as an intended contract. `noOperation` is additionally dead — no call site
  * exists anywhere in the repository.
  */
class DataProcessorRPCHandlerInitializerSpec extends AnyFlatSpec {

  import DataProcessorRPCHandlerInitializerSpec._

  private val workerId = ActorVirtualIdentity("Worker:WF1-dp-rpc-init-main-0")

  private def newInitializer(): DataProcessorRPCHandlerInitializer = {
    val outputHandler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit = _ => ()
    val dp =
      new DataProcessor(workerId, outputHandler, new LinkedBlockingQueue[DPInputQueueElement]())
    new DataProcessorRPCHandlerInitializer(dp)
  }

  behavior of "DataProcessorRPCHandlerInitializer.setupExecutor"

  it should "build the named class from the descriptor, worker index and worker count" in {
    val initializer = newInitializer()

    // Three distinct values, so a swap or a dropped argument cannot pass: the index is not the
    // count, and neither is the descriptor.
    initializer.setupExecutor(
      OpExecWithClassName(classOf[DescribedExec].getName, "desc-for-worker"),
      workerIdx = 3,
      workerCount = 7
    )

    val executor = initializer.dp.executor.asInstanceOf[DescribedExec]
    assert(executor.desc == "desc-for-worker")
    assert(executor.idx == 3)
    assert(executor.workerCount == 7)
  }

  it should "replace an executor that was already installed" in {
    // `updateExecutor` reconfigures a running worker through this same method, so the assignment
    // has to overwrite rather than only initialize.
    val initializer = newInitializer()
    initializer.setupExecutor(OpExecWithClassName(classOf[DescribedExec].getName, "first"), 0, 1)
    val first = initializer.dp.executor

    initializer.setupExecutor(OpExecWithClassName(classOf[DescribedExec].getName, "second"), 0, 1)

    assert(initializer.dp.executor ne first)
    assert(initializer.dp.executor.asInstanceOf[DescribedExec].desc == "second")
  }

  it should "send OpExecWithCode's code to the Java compiler, surfacing its diagnostics" in {
    // The code variant is the Java-UDF path: the string is compiled at runtime rather than looked
    // up as a class name. javac here runs on its own classpath and cannot see project classes
    // (see CoreExecutorReflectionSpec), so a compiling UDF is not expressible in a test; what is
    // observable — and what distinguishes this arm from the class-name arm — is that the code
    // itself reaches the compiler, which is why the assertion is on javac's own diagnostic.
    val initializer = newInitializer()

    val failure = intercept[RuntimeException] {
      initializer.setupExecutor(
        OpExecWithCode("public class JavaUDFOpExec { int broken = \"not an int\"; }", "java"),
        workerIdx = 0,
        workerCount = 1
      )
    }

    assert(failure.getMessage.contains("Error at line"))
    assert(failure.getMessage.contains("incompatible types"))
    assert(initializer.dp.executor == null, "a failed compilation must not install an executor")
  }

  it should "reject an empty OpExecInitInfo instead of installing a null executor" in {
    // `OpExecInitInfo` is a protobuf sealed oneof, so "no variant set" is a representable value
    // that arrives from the wire; it is rejected loudly rather than left to NPE later.
    val initializer = newInitializer()

    val failure = intercept[IllegalArgumentException] {
      initializer.setupExecutor(OpExecInitInfo.Empty, workerIdx = 0, workerCount = 1)
    }

    assert(failure.getMessage == "Empty executor initialization info")
    assert(initializer.dp.executor == null)
  }
}

private object DataProcessorRPCHandlerInitializerSpec {

  /**
    * Records everything `newExecFromJavaClassName` is asked to pass through. Public, and with the
    * `(String, Int, Int)` constructor that branch resolves to, so reflection inside `ExecFactory`
    * can reach it.
    */
  class DescribedExec(val desc: String, val idx: Int, val workerCount: Int)
      extends OperatorExecutor {
    override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator.empty
  }
}
