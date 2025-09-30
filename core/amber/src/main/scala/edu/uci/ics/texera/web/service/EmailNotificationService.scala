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

package edu.uci.ics.texera.web.service

import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState

import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.{ExecutionContext, Future}

trait EmailNotifier {
  def shouldSendEmail(workflowState: WorkflowAggregatedState): Boolean

  def sendStatusEmail(state: WorkflowAggregatedState): Unit
}

class EmailNotificationService(emailNotifier: EmailNotifier) {
  private val executorService: ExecutorService = Executors.newSingleThreadExecutor()
  private implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executorService)

  def processEmailNotificationIfNeeded(
      workflowState: WorkflowAggregatedState
  ): Future[Unit] = {
    Future {
      if (emailNotifier.shouldSendEmail(workflowState)) {
        emailNotifier.sendStatusEmail(workflowState)
      }
    }.recover {
      case e: Exception =>
        println(s"Failed to send email notification: ${e.getMessage}")
    }
  }

  def shutdown(): Unit = {
    executorService.shutdown()
  }
}
