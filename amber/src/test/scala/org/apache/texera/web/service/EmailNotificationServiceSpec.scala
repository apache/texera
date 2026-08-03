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

import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

class EmailNotificationServiceSpec extends AnyFlatSpec with Matchers {

  private val completedState = WorkflowAggregatedState.COMPLETED
  private val awaitTimeout = 2.seconds

  private class RecordingNotifier(
      shouldSend: Boolean,
      failureOnSend: Option[RuntimeException] = None
  ) extends EmailNotifier {
    val evaluatedStates = new ConcurrentLinkedQueue[WorkflowAggregatedState]()
    val sentStates = new ConcurrentLinkedQueue[WorkflowAggregatedState]()

    override def shouldSendEmail(workflowState: WorkflowAggregatedState): Boolean = {
      evaluatedStates.add(workflowState)
      shouldSend
    }

    override def sendStatusEmail(state: WorkflowAggregatedState): Unit = {
      sentStates.add(state)
      failureOnSend.foreach(throw _)
    }
  }

  private def withService(notifier: EmailNotifier)(test: EmailNotificationService => Unit): Unit = {
    val service = new EmailNotificationService(notifier)
    try {
      test(service)
    } finally {
      service.shutdown()
    }
  }

  "processEmailNotificationIfNeeded" should "send the exact state when the notifier requests it" in {
    val notifier = new RecordingNotifier(shouldSend = true)

    withService(notifier) { service =>
      Await.result(service.processEmailNotificationIfNeeded(completedState), awaitTimeout)

      notifier.evaluatedStates.asScala.toList shouldBe List(completedState)
      notifier.sentStates.asScala.toList shouldBe List(completedState)
    }
  }

  it should "not send an email when the notifier declines it" in {
    val notifier = new RecordingNotifier(shouldSend = false)

    withService(notifier) { service =>
      Await.result(service.processEmailNotificationIfNeeded(completedState), awaitTimeout)

      notifier.evaluatedStates.asScala.toList shouldBe List(completedState)
      notifier.sentStates.asScala.toList shouldBe empty
    }
  }

  it should "recover from an exception raised while sending an email" in {
    val notifier = new RecordingNotifier(
      shouldSend = true,
      failureOnSend = Some(new RuntimeException("email provider unavailable"))
    )

    withService(notifier) { service =>
      noException should be thrownBy {
        Await.result(service.processEmailNotificationIfNeeded(completedState), awaitTimeout)
      }

      notifier.sentStates.asScala.toList shouldBe List(completedState)
    }
  }
}
