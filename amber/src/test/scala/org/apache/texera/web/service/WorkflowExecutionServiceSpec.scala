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
import org.apache.texera.amber.core.workflow.{WorkflowContext, WorkflowSettings}
import org.apache.texera.amber.core.workflowruntimestate.FatalErrorType.EXECUTION_FAILURE
import org.apache.texera.amber.core.workflowruntimestate.WorkflowFatalError
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.{
  FAILED,
  RUNNING
}
import org.apache.texera.web.model.websocket.event.{
  TexeraWebSocketEvent,
  WorkflowErrorEvent,
  WorkflowStateEvent
}
import org.apache.texera.web.model.websocket.request.{LogicalPlanPojo, WorkflowExecuteRequest}
import org.apache.texera.web.storage.ExecutionStateStore
import org.apache.texera.web.storage.ExecutionStateStore.updateWorkflowState
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.URI
import java.time.Instant
import scala.collection.mutable

/**
  * Regression guard for the consolidated init-error reporting path (#5921):
  * `WorkflowExecutionService` registers its metadata-store diff handler at
  * construction, so a fatalErrors update -- e.g. the one `errorHandler` records
  * when `executeWorkflow` fails -- surfaces as a `WorkflowErrorEvent` through the
  * normal websocket-event observable.
  *
  * The unused `coordinatorConfig` / `resultService` are passed as `null` on
  * purpose: construction must stay side-effect-free (all throwing work is in
  * `executeWorkflow`), so a future change that dereferences them during
  * construction would fail here.
  */
class WorkflowExecutionServiceSpec extends AnyFlatSpec with Matchers {

  private def buildService(
      store: ExecutionStateStore,
      errorHandler: Throwable => Unit = (_: Throwable) => ()
  ): WorkflowExecutionService = {
    val request = WorkflowExecuteRequest(
      executionName = "test",
      engineVersion = "test",
      logicalPlan = LogicalPlanPojo(List.empty, List.empty, List.empty, List.empty),
      replayFromExecution = None,
      workflowSettings = WorkflowSettings(),
      emailNotificationEnabled = false,
      computingUnitId = 0
    )
    new WorkflowExecutionService(
      null,
      new WorkflowContext(),
      null,
      request,
      store,
      errorHandler,
      None,
      new URI("vfs:///test")
    )
  }

  /** Subscribe to the metadata store's websocket-event stream and collect events. */
  private def collectEvents(
      store: ExecutionStateStore
  ): mutable.ArrayBuffer[TexeraWebSocketEvent] = {
    val events = mutable.ArrayBuffer.empty[TexeraWebSocketEvent]
    store.metadataStore.getWebsocketEventObservable.subscribe {
      (evts: Iterable[TexeraWebSocketEvent]) => events ++= evts
    }
    events
  }

  "WorkflowExecutionService" should
    "surface a recorded fatal error as a WorkflowErrorEvent via the metadata-store handler" in {
    val store = new ExecutionStateStore()
    buildService(store) // registers the diff handler at construction
    val events = collectEvents(store)

    val err =
      WorkflowFatalError(EXECUTION_FAILURE, Timestamp(Instant.now), "boom during init", "", "", "")
    store.metadataStore.updateState(_.addFatalErrors(err))

    val errorEvents = events.collect { case e: WorkflowErrorEvent => e }
    errorEvents should have size 1
    errorEvents.head.fatalErrors should contain(err)
  }

  it should "report fatal errors recorded at successive phases through the same handler" in {
    val store = new ExecutionStateStore()
    // Mirror WorkflowService's real errorHandler, which records into the
    // metadata store. The service invokes this same handler at every phase
    // (compile, runtime creation, startWorkflow failure), so invoking it
    // repeatedly here stands in for failures arising at different phases.
    val recordError: Throwable => Unit = t =>
      store.metadataStore.updateState(metadataStore =>
        updateWorkflowState(FAILED, metadataStore).addFatalErrors(
          WorkflowFatalError(EXECUTION_FAILURE, Timestamp(Instant.now), t.toString, "", "", "")
        )
      )
    buildService(store, recordError)
    val events = collectEvents(store)

    recordError(new RuntimeException("init phase"))
    recordError(new RuntimeException("runtime phase"))

    val errorEvents = events.collect { case e: WorkflowErrorEvent => e }
    errorEvents should have size 2
    errorEvents.last.fatalErrors.map(_.message) should contain allOf (
      "java.lang.RuntimeException: init phase",
      "java.lang.RuntimeException: runtime phase"
    )
  }

  it should "emit a WorkflowStateEvent when the execution state changes" in {
    val store = new ExecutionStateStore()
    buildService(store)
    val events = collectEvents(store)

    store.metadataStore.updateState(_.withState(RUNNING))

    events.collect { case e: WorkflowStateEvent => e } should not be empty
  }
}
