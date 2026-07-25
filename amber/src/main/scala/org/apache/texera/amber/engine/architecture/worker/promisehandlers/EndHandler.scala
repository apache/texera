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

package org.apache.texera.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  EmptyRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.{
  EmptyReturn,
  ReturnInvocation
}
import org.apache.texera.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.FIFOMessageElement

import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
  * The EndWorker control messages is needed to ensure all the other control messages in a worker
  * are processed before worker termination.
  */
trait EndHandler {
  this: DataProcessorRPCHandlerInitializer =>

  /**
    * The response of endWorker to the coordinator indicates that this worker has finished not only
    * the data processing logic, but also the processing of all the control messages.
    */
  override def endWorker(
      request: EmptyRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = {
    // Ensure this is really the last message that carries work. RPC acks
    // (ReturnInvocations for this worker's own fire-and-forget calls, e.g.
    // workerExecutionCompleted / portCompleted) race with EndWorker by design:
    // the coordinator decides to end the worker while its acks are still in
    // flight. The worker never awaits those acks, so an ack-only backlog is
    // safe to leave unprocessed at termination -- failing on it only makes
    // region termination retry and CI flaky. Anything else still fails loudly
    // so the region execution manager retries the kill instead of dropping
    // real work.
    val pending = dp.inputManager.inputMessageQueue.asScala.toList
    val ackOnly = pending.forall {
      case FIFOMessageElement(msg) => msg.payload.isInstanceOf[ReturnInvocation]
      case _                       => false
    }
    if (pending.nonEmpty && !ackOnly) {
      logger.warn(
        s"Received EndHandler before all messages are processed. Unprocessed messages: " +
          s"$pending"
      )
      return Future.exception(new IllegalStateException("worker still has unprocessed messages"))
    }
    if (pending.nonEmpty) {
      logger.warn(
        s"Received EndHandler with only RPC acks left in the queue; proceeding with " +
          s"termination. Pending acks: $pending"
      )
    }
    // Now we can safely acknowledge that this worker can be terminated.
    EmptyReturn()
  }
}
