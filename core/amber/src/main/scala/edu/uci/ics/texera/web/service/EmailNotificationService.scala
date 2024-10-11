package edu.uci.ics.texera.web.service

import edu.uci.ics.amber.engine.common.workflowruntimestate.WorkflowAggregatedState

import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.{ExecutionContext, Future}

trait EmailNotifier {
  def shouldSendEmail(oldState: WorkflowAggregatedState, newState: WorkflowAggregatedState): Boolean
  def sendStatusEmail(state: WorkflowAggregatedState): Unit
}

class EmailNotificationService(emailNotifier: EmailNotifier) {
  private val executorService: ExecutorService = Executors.newSingleThreadExecutor()
  private implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executorService)

  def sendEmailNotification(
      oldState: WorkflowAggregatedState,
      newState: WorkflowAggregatedState
  ): Future[Unit] = {
    Future {
      if (emailNotifier.shouldSendEmail(oldState, newState)) {
        emailNotifier.sendStatusEmail(newState)
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
