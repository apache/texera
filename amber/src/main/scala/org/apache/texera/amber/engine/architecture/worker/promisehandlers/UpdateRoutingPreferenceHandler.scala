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
  UpdateRoutingPreferenceRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import org.apache.texera.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer

/** Points a least-loaded sender at the receiver the coordinator currently ranks
  * least backlogged.
  *
  * Asserts no worker state, unlike AddPartitioning. These arrive on a timer, so
  * a worker that finished between the coordinator's poll and this message would
  * otherwise throw on every remaining tick of the run -- an error per worker per
  * poll, for a message whose only effect is to move an index that a finished
  * worker will never read again.
  */
trait UpdateRoutingPreferenceHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def updateRoutingPreference(
      msg: UpdateRoutingPreferenceRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = {
    dp.outputManager.updateRoutingPreference(msg.tag, msg.receiverIndex)
    EmptyReturn()
  }

}
