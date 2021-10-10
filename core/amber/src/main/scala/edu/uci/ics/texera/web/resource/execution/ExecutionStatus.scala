package edu.uci.ics.texera.web.resource.execution
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{WorkflowCompleted, WorkflowPaused}
import edu.uci.ics.amber.engine.architecture.controller.ControllerSubjects
import edu.uci.ics.texera.web.model.event.{TexeraWebSocketEvent, WorkflowCompletedEvent, WorkflowPausedEvent, WorkflowStartedEvent}
import edu.uci.ics.texera.web.resource.Observer
import edu.uci.ics.texera.web.resource.execution.ExecutionStatus._

object ExecutionStatus{
  sealed trait ExecutionStatusEnum
  case object Unknown extends ExecutionStatusEnum
  case object Started extends ExecutionStatusEnum
  case object Paused extends ExecutionStatusEnum
  case object Completed extends ExecutionStatusEnum
}

class ExecutionStatus(controllerObservables: ControllerSubjects) extends BehaviorSubject {

  var currentStatus:ExecutionStatusEnum = Unknown

  controllerObservables.workflowPaused.subscribe((evt:WorkflowPaused) => {
    currentStatus = Paused
    onNext(WorkflowPausedEvent())
  })
  controllerObservables.workflowCompleted.subscribe((evt:WorkflowCompleted) => {
    currentStatus = Completed
    onNext(WorkflowCompletedEvent())
  })

  def workflowStarted(): Unit ={
    currentStatus = Started
    onNext(WorkflowStartedEvent())
  }


  override def sendSnapshotTo(observer: Observer[TexeraWebSocketEvent]): Unit = {
     currentStatus match{
      case Unknown => //skip
      case Started => observer.onNext(WorkflowStartedEvent())
      case Paused => observer.onNext(WorkflowPausedEvent())
      case Completed => observer.onNext(WorkflowCompletedEvent())
    }
  }
}
