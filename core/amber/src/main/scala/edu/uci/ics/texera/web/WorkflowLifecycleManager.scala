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

import akka.actor.Cancellable
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.RUNNING
import edu.uci.ics.amber.engine.common.AmberRuntime
import edu.uci.ics.amber.engine.common.executionruntimestate.ExecutionMetadataStore
import edu.uci.ics.texera.web.storage.ExecutionStateStore

import java.time.{LocalDateTime, Duration => JDuration}
import scala.concurrent.duration.DurationInt

class WorkflowLifecycleManager(id: String, cleanUpTimeout: Int, cleanUpCallback: () => Unit)
    extends LazyLogging {
  private var userCount = 0
  private var cleanUpExecution: Cancellable = Cancellable.alreadyCancelled

  private[this] def setCleanUpDeadline(status: WorkflowAggregatedState): Unit = {
    synchronized {
      if (userCount > 0 || status == RUNNING) {
        cleanUpExecution.cancel()
        logger.info(
          s"[$id] workflow state clean up postponed. current user count = $userCount, workflow status = $status"
        )
      } else {
        refreshDeadline()
      }
    }
  }

  private[this] def refreshDeadline(): Unit = {
    if (cleanUpExecution.isCancelled || cleanUpExecution.cancel()) {
      logger.info(
        s"[$id] workflow state clean up will start at ${LocalDateTime.now().plus(JDuration.ofSeconds(cleanUpTimeout))}"
      )
      cleanUpExecution = AmberRuntime.scheduleCallThroughActorSystem(cleanUpTimeout.seconds) {
        cleanUp()
      }
    }
  }

  private[this] def cleanUp(): Unit = {
    synchronized {
      if (userCount > 0) {
        // do nothing
        logger.info(s"[$id] workflow state clean up failed. current user count = $userCount")
      } else {
        cleanUpExecution.cancel()
        cleanUpCallback()
        logger.info(s"[$id] workflow state clean up completed.")
      }
    }
  }

  def increaseUserCount(): Unit = {
    synchronized {
      userCount += 1
      cleanUpExecution.cancel()
      logger.info(s"[$id] workflow state clean up postponed. current user count = $userCount")
    }
  }

  def decreaseUserCount(currentWorkflowState: Option[WorkflowAggregatedState]): Unit = {
    synchronized {
      userCount -= 1
      if (userCount == 0 && (currentWorkflowState.isEmpty || currentWorkflowState.get != RUNNING)) {
        refreshDeadline()
      } else {
        logger.info(s"[$id] workflow state clean up postponed. current user count = $userCount")
      }
    }
  }

  def registerCleanUpOnStateChange(stateStore: ExecutionStateStore): Unit = {
    cleanUpExecution.cancel()
    stateStore.metadataStore.getStateObservable.subscribe { newState: ExecutionMetadataStore =>
      setCleanUpDeadline(newState.state)
    }
  }

}
