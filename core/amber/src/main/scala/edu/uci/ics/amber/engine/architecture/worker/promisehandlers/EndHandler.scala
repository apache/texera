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

package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, EmptyRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer

/**
  * The EndWorker control messages is needed to ensure all the other control messages in a worker
  * are processed before worker termination.
  */
trait EndHandler {
  this: DataProcessorRPCHandlerInitializer =>

  /**
    * The response of endWorker to the controller indicates that this worker has finished not only
    * the data processing logic, but also , but also the processing of all the control messages.
    */
  override def endWorker(
      request: EmptyRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = {
    // Ensure this is really the last message.
    if (!dp.inputManager.inputMessageQueue.isEmpty) {
      logger.warn(
        s"Received EndHandler before all messages are processed. Unprocessed messages: " +
          s"${dp.inputManager.inputMessageQueue.peek()}"
      )
    }
    assert(dp.inputManager.inputMessageQueue.isEmpty)
    // Now we can safely acknowledge that this worker can be terminated.
    EmptyReturn()
  }
}
