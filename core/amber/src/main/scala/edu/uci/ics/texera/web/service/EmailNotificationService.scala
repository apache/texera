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
