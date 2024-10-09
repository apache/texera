package edu.uci.ics.texera.web.service

import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState
import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.{ExecutionContext, Future}

class EmailNotificationService(emailNotifier: WorkflowEmailNotifier) {
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
    }
  }

  def shutdown(): Unit = {
    executorService.shutdown()
  }
}
