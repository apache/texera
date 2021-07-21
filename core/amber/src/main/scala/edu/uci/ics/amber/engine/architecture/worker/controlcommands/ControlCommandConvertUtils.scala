package edu.uci.ics.amber.engine.architecture.worker.controlcommands

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.Partitioning
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity

object ControlCommandConvertUtils {
  def controlCommandToV2(
      controlCommand: edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand[_]
  ): ControlCommand = {
    controlCommand match {
      case edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler
            .StartWorker() =>
        StartWorker()
      case edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseHandler
            .PauseWorker() =>
        PauseWorker()
      case edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ResumeHandler
            .ResumeWorker() =>
        ResumeWorker()
      case edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AddPartitioningHandler
            .AddPartitioning(tag: LinkIdentity, partitioning: Partitioning) =>
        AddPartitioning(tag, partitioning)
      case edu.uci.ics.amber.engine.architecture.worker.promisehandlers.UpdateInputLinkingHandler
            .UpdateInputLinking(identifier, inputLink) =>
        UpdateInputLinking(identifier, inputLink)
      case _ =>
        throw new UnsupportedOperationException(
          s"V1 command $controlCommand does not support converting to V2"
        )
    }

  }

  def controlCommandToV1(
      controlCommand: ControlCommand
  ): edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand[_] = {
    controlCommand match {
      case WorkerExecutionCompleted() => {
        edu.uci.ics.amber.engine.architecture.controller.promisehandlers.WorkerExecutionCompletedHandler
          .WorkerExecutionCompleted()
      }
      case _ =>
        throw new UnsupportedOperationException(
          s"V2 command $controlCommand does not support converting to V1"
        )
    }

  }
}
