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
  ActorCommandElement,
  DPInputQueueElement,
  FIFOMessageElement,
  TimerBasedControlElement
}

/**
  * The EndWorker control message is needed to ensure all the other control messages in a worker
  * are processed before worker termination.
  *
  * EndWorker is the last *request* the coordinator sends to a worker, but NOT the last message
  * the worker receives: the coordinator sends EndWorker from inside the `portCompleted` handler
  * (see `PortCompletedHandler`), so the reply to that same `portCompleted` is emitted afterwards
  * on the same FIFO control channel and legitimately sits behind EndWorker in this worker's
  * arrival queue on every normal region teardown.
  */
trait EndHandler {
  this: DataProcessorRPCHandlerInitializer =>

  /**
    * The response of endWorker to the coordinator indicates that this worker has finished not only
    * the data processing logic, but also the processing of all the queued control messages that
    * represent work. On failure the coordinator retries EndWorker after a delay instead of
    * stopping a worker that still has real queued work.
    */
  override def endWorker(
      request: EmptyRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = {
    // `inputMessageQueue` is the DP thread's raw arrival queue, drained wholesale by the DP
    // thread's main loop before this handler runs — anything visible here arrived while
    // EndWorker was being processed.
    val pendingWork = findPendingWork
    if (pendingWork.isDefined) {
      logger.warn(
        s"Received endWorker while unprocessed work is still queued: " +
          s"${describePending(pendingWork.get)}"
      )
      return Future.exception(new IllegalStateException("worker still has unprocessed messages"))
    }
    // Now we can safely acknowledge that this worker can be terminated.
    EmptyReturn()
  }

  /**
    * A queued `ReturnInvocation` is not unprocessed work: processing it only fulfills a promise
    * for a request this worker already issued (see `AmberProcessor.processDCM`), and every
    * worker-to-coordinator call discards its future, so no continuation is pending on it.
    * Everything else — control invocations, data frames, timer-based controls, actor commands —
    * still blocks termination.
    */
  private def findPendingWork: Option[DPInputQueueElement] = {
    // LinkedBlockingQueue's iterator is weakly consistent; this decision is re-taken on every
    // termination attempt, so a concurrent put is only deferred to the coordinator's retry.
    val iterator = dp.inputManager.inputMessageQueue.iterator()
    while (iterator.hasNext) {
      val element = iterator.next()
      val isWork = element match {
        case FIFOMessageElement(msg) => !msg.payload.isInstanceOf[ReturnInvocation]
        case _                       => true
      }
      if (isWork) {
        return Some(element)
      }
    }
    None
  }

  /** Bounded description of a pending element: payload type and channel only, never payload contents. */
  private def describePending(element: DPInputQueueElement): String =
    element match {
      case FIFOMessageElement(msg) =>
        s"${msg.payload.getClass.getSimpleName} from ${msg.channelId}"
      case TimerBasedControlElement(control) =>
        s"timer-based control ${control.methodName}"
      case ActorCommandElement(cmd) =>
        s"actor command ${cmd.getClass.getSimpleName}"
    }
}
