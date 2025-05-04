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

package edu.uci.ics.texera.web

import edu.uci.ics.amber.engine.common.Utils.objectMapper
import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import edu.uci.ics.texera.web.service.WorkflowService
import io.reactivex.rxjava3.disposables.Disposable

import javax.websocket.Session
import scala.collection.mutable

object SessionState {
  private val sessionIdToSessionState = new mutable.HashMap[String, SessionState]()

  def getState(sId: String): SessionState = {
    sessionIdToSessionState(sId)
  }

  def setState(sId: String, state: SessionState): Unit = {
    sessionIdToSessionState.put(sId, state)
  }

  def removeState(sId: String): Unit = {
    sessionIdToSessionState(sId).unsubscribe()
    sessionIdToSessionState.remove(sId)
  }

  def getAllSessionStates: Iterable[SessionState] = {
    sessionIdToSessionState.values
  }

}

class SessionState(session: Session) {
  private var currentWorkflowState: Option[WorkflowService] = None
  private var workflowSubscription = Disposable.empty()
  private var executionSubscription = Disposable.empty()

  def send(msg: TexeraWebSocketEvent): Unit = {
    session.getAsyncRemote.sendText(objectMapper.writeValueAsString(msg))
  }

  def getCurrentWorkflowState: Option[WorkflowService] = currentWorkflowState

  def unsubscribe(): Unit = {
    workflowSubscription.dispose()
    executionSubscription.dispose()
    if (currentWorkflowState.isDefined) {
      currentWorkflowState.get.disconnect()
      currentWorkflowState = None
    }
  }

  def subscribe(workflowService: WorkflowService): Unit = {
    unsubscribe()
    currentWorkflowState = Some(workflowService)
    workflowSubscription = workflowService.connect(evt =>
      session.getAsyncRemote.sendText(objectMapper.writeValueAsString(evt))
    )
    executionSubscription = workflowService.connectToExecution(evt =>
      session.getAsyncRemote.sendText(objectMapper.writeValueAsString(evt))
    )

  }
}
