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

package org.apache.texera.web.service

import com.google.protobuf.timestamp.Timestamp
import org.apache.texera.amber.core.virtualidentity.WorkflowIdentity
import org.apache.texera.amber.core.workflowruntimestate.FatalErrorType.EXECUTION_FAILURE
import org.apache.texera.amber.core.workflowruntimestate.WorkflowFatalError
import org.apache.texera.web.model.websocket.event.{TexeraWebSocketEvent, WorkflowErrorEvent}
import org.apache.texera.web.storage.ExecutionStateStore
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.collection.mutable.ArrayBuffer

/**
  * Unit tests for `WorkflowService.reportFatalErrorsToSubscribers`, the seam
  * that surfaces init-time fatal errors to the websocket. When execution
  * initialization fails, the error is recorded in the metadata store; this push
  * is what makes it visible to connected clients instead of only logged.
  */
class WorkflowServiceSpec extends AnyFlatSpec with Matchers {

  private def fatalError(message: String): WorkflowFatalError =
    WorkflowFatalError(EXECUTION_FAILURE, Timestamp(Instant.now), message, "", "", "")

  /** A WorkflowService with a subscriber collecting every event it pushes. */
  private def serviceWithCollector(): (WorkflowService, ArrayBuffer[TexeraWebSocketEvent]) = {
    val service = new WorkflowService(WorkflowIdentity(1), computingUnitId = 1, cleanUpTimeout = 30)
    val events = ArrayBuffer.empty[TexeraWebSocketEvent]
    service.connect(evt => events += evt)
    (service, events)
  }

  private def errorEventsIn(events: ArrayBuffer[TexeraWebSocketEvent]): Seq[WorkflowErrorEvent] =
    events.collect { case e: WorkflowErrorEvent => e }.toSeq

  "WorkflowService" should
    "push a WorkflowErrorEvent carrying the store's fatal error to connected subscribers" in {
    val (service, events) = serviceWithCollector()
    val store = new ExecutionStateStore()
    val err = fatalError("boom during init")
    store.metadataStore.updateState(_.addFatalErrors(err))

    service.reportFatalErrorsToSubscribers(store)

    val errorEvents = errorEventsIn(events)
    errorEvents should have size 1
    // Forwards exactly the store's fatal errors -- no more, no less.
    errorEvents.head.fatalErrors should contain theSameElementsAs Seq(err)
  }

  it should "carry every fatal error currently recorded in the store" in {
    val (service, events) = serviceWithCollector()
    val store = new ExecutionStateStore()
    val first = fatalError("first")
    val second = fatalError("second")
    store.metadataStore.updateState(_.addFatalErrors(first).addFatalErrors(second))

    service.reportFatalErrorsToSubscribers(store)

    val errorEvents = errorEventsIn(events)
    errorEvents should have size 1
    // Exactly the two recorded errors -- no extras.
    errorEvents.head.fatalErrors should contain theSameElementsAs Seq(first, second)
  }
}
