package edu.uci.ics.texera.web.service

import edu.uci.ics.texera.web.resource.{EmailMessage, GmailResource}
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.{
  COMPLETED,
  FAILED,
  KILLED,
  PAUSED,
  RUNNING
}
import org.hibernate.validator.internal.constraintvalidators.hv.EmailValidator

import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter

class WorkflowEmailNotifier(
    baseUrl: String,
    workflowName: String,
    workflowId: String,
    userEmail: String
) {
  private val emailValidator = new EmailValidator()
  private val CompletedPausedOrTerminatedStates: Set[WorkflowAggregatedState] = Set(
    COMPLETED,
    PAUSED,
    FAILED,
    KILLED
  )

  def shouldSendEmail(
      oldState: WorkflowAggregatedState,
      newState: WorkflowAggregatedState
  ): Boolean = {
    oldState == RUNNING && CompletedPausedOrTerminatedStates.contains(newState)
  }

  def sendStatusEmail(state: WorkflowAggregatedState): Unit = {
    if (!emailValidator.isValid(userEmail, null)) {
      return
    }

    val timestamp = DateTimeFormatter
      .ofPattern("MMMM d, yyyy, h:mm:ss a '(UTC)'")
      .withZone(ZoneOffset.UTC)
      .format(Instant.now())

    val dashboardUrl = s"$baseUrl/dashboard/user/workspace/$workflowId"

    val subject = s"[Texera] Workflow $workflowName ($workflowId) Status: $state"
    val content = s"""
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

    val emailMessage = EmailMessage(
      receiver = userEmail,
      subject = subject,
      content = content
    )

    GmailResource.sendEmail(emailMessage, userEmail)
  }
}
