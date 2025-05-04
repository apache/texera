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

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState._
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowResource
import edu.uci.ics.texera.web.resource.{EmailMessage, GmailResource}
import org.hibernate.validator.internal.constraintvalidators.hv.EmailValidator

import java.net.URI
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

class WorkflowEmailNotifier(
    workflowId: Long,
    userEmail: String,
    sessionUri: URI
) extends EmailNotifier
    with LazyLogging {
  private val workflowName = WorkflowResource.getWorkflowName(workflowId.toInt)
  private val emailValidator = new EmailValidator()

  private val TerminalStates: Set[WorkflowAggregatedState] = Set(
    COMPLETED,
    PAUSED,
    FAILED,
    KILLED
  )

  override def shouldSendEmail(workflowState: WorkflowAggregatedState): Boolean =
    TerminalStates.contains(workflowState)

  override def sendStatusEmail(state: WorkflowAggregatedState): Unit = {
    if (!isValidEmail(userEmail)) {
      logger.warn(s"Invalid email address: $userEmail")
      return
    }

    val emailMessage = createEmailMessage(state)

    try {
      GmailResource.sendEmail(emailMessage, userEmail)
    } catch {
      case e: Exception => println(s"Failed to send email: ${e.getMessage}")
    }
  }

  private def isValidEmail(email: String): Boolean = emailValidator.isValid(email, null)

  private def createEmailMessage(state: WorkflowAggregatedState): EmailMessage = {
    EmailMessage(
      receiver = userEmail,
      subject = createEmailSubject(state),
      content = createEmailContent(state)
    )
  }

  private def createEmailSubject(state: WorkflowAggregatedState): String =
    s"[Texera] Workflow $workflowName ($workflowId) Status: $state"

  private def createEmailContent(state: WorkflowAggregatedState): String = {
    val timestamp = formatTimestamp(Instant.now())
    val dashboardUrl = createDashboardUrl()

    s"""
       |Hello,
       |
       |The workflow with the following details has changed its state:
       |
       |- Workflow ID: $workflowId
       |- Workflow Name: $workflowName
       |- State: $state
       |- Timestamp: $timestamp
       |
       |You can view more details by visiting: $dashboardUrl
       |
       |Regards,
       |Texera Team
    """.stripMargin.trim
  }

  private def formatTimestamp(instant: Instant): String =
    DateTimeFormatter
      .ofPattern("MMMM d, yyyy, h:mm:ss a '(UTC)'")
      .withZone(ZoneOffset.UTC)
      .format(instant)

  private def createDashboardUrl(): String = {
    val host = sessionUri.getHost
    val port = sessionUri.getPort
    val path = s"/dashboard/user/workspace/$workflowId"
    if (port == -1 || port == 80 || port == 443) {
      s"http://$host$path"
    } else {
      s"http://$host:$port$path"
    }
  }
}
