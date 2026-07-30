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
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.{
  DPInputQueueElement,
  FIFOMessageElement
}

/**
  * The EndWorker control messages is needed to ensure all the other control messages in a worker
  * are processed before worker termination.
  */
trait EndHandler {
  this: DataProcessorRPCHandlerInitializer =>

  /**
    * The response of endWorker to the controller indicates that this worker has finished not only
    * the data processing logic, but also the processing of all the control messages.
    *
    * A queued reply to one of this worker's own requests does not count: it carries no work (see
    * `findUnprocessedWork`), and the controller cannot order all of its replies before `EndWorker`.
    * The controller defers region advancement to a later control round precisely so that
    * `EndWorker` follows the replies it owes (see `PortCompletedHandler`), but that only orders
    * requests it has already handled. A request of this worker's that is still queued at the
    * controller when the deferred advance runs — `workerExecutionCompleted`, which the worker
    * emits right after its last `portCompleted` — is replied to afterwards, and the controller
    * picks its input channels out of a `HashMap` (`NetworkInputGateway.tryPickControlChannel`), so
    * there is no cross-channel order to rely on.
    */
  override def endWorker(
      request: EmptyRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = {
    // Ensure this is really the last message that asks this worker to do anything.
    val pendingWork = findUnprocessedWork
    if (pendingWork.isDefined) {
      logger.warn(
        s"Received EndHandler before all messages are processed. Unprocessed message: " +
          s"${describe(pendingWork.get)}"
      )
      return Future.exception(new IllegalStateException("worker still has unprocessed messages"))
    }
    // Now we can safely acknowledge that this worker can be terminated.
    EmptyReturn()
  }

  /**
    * The first queued element that represents work, if any.
    *
    * A `ReturnInvocation` is excluded: processing one only fulfills a promise for a request this
    * worker already issued (`AmberProcessor.processDCM`), and every worker-to-controller call
    * discards its future, so nothing is pending on it. Everything else — control invocations, data,
    * embedded control messages, timer-based controls, actor commands — still blocks termination, so
    * the controller retries and this worker drains it first.
    *
    * This is the worker's own arrival queue rather than its actor mailbox, and at termination it
    * holds at most a couple of elements.
    */
  private def findUnprocessedWork: Option[DPInputQueueElement] = {
    val iterator = dp.inputManager.inputMessageQueue.iterator()
    while (iterator.hasNext) {
      val element = iterator.next()
      val isWork = element match {
        case FIFOMessageElement(message) => !message.payload.isInstanceOf[ReturnInvocation]
        case _                           => true
      }
      if (isWork) {
        return Some(element)
      }
    }
    None
  }

  /** Identifies a queued element without logging payload contents. */
  private def describe(element: DPInputQueueElement): String =
    element match {
      case FIFOMessageElement(message) =>
        s"${message.payload.getClass.getSimpleName} on ${message.channelId}"
      case other => other.getClass.getSimpleName
    }
}
