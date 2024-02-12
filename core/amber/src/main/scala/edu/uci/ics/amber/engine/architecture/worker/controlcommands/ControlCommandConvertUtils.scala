package edu.uci.ics.amber.engine.architecture.worker.controlcommands

import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PortCompletedHandler.PortCompleted
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ConsoleMessageHandler.ConsoleMessageTriggered
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.WorkerExecutionCompletedHandler.WorkerExecutionCompleted
import edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers.EvaluateExpressionHandler.EvaluateExpression
import edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers.InitializeOperatorLogicHandler.InitializeOperatorLogic
import edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers.ModifyPythonOperatorLogicHandler.ModifyPythonOperatorLogic
import edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers.ReplayCurrentTupleHandler.ReplayCurrentTuple
import edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers.WorkerDebugCommandHandler.WorkerDebugCommand
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.Partitioning
import edu.uci.ics.amber.engine.architecture.worker.controlreturns.{
  ControlException,
  ControlReturnV2
}
import edu.uci.ics.amber.engine.architecture.worker.controlreturns.ControlReturnV2.Value.Empty
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AddPartitioningHandler.AddPartitioning
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.MonitoringHandler.QuerySelfWorkloadMetrics
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.OpenOperatorHandler.OpenOperator
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryCurrentInputTupleHandler.QueryCurrentInputTuple
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryStatisticsHandler.QueryStatistics
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ResumeHandler.ResumeWorker
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AddInputChannelHandler.AddInputChannel
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AssignPortHandler.AssignPort
import edu.uci.ics.amber.engine.architecture.worker.statistics.{WorkerState, WorkerStatistics}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.workflow.PhysicalLink

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.jdk.CollectionConverters.ListHasAsScala

object ControlCommandConvertUtils {
  def controlCommandToV2(
      controlCommand: ControlCommand[_]
  ): ControlCommandV2 = {
    controlCommand match {
      case StartWorker() =>
        StartWorkerV2()
      case PauseWorker() =>
        PauseWorkerV2()
      case ResumeWorker() =>
        ResumeWorkerV2()
      case OpenOperator() =>
        OpenOperatorV2()
      case AssignPort(portId, input) =>
        AssignPortV2(portId, input)
      case AddPartitioning(tag: PhysicalLink, partitioning: Partitioning) =>
        AddPartitioningV2(tag, partitioning)
      case AddInputChannel(channelId, portId) =>
        AddInputChannelV2(channelId, portId)
      case QueryStatistics() =>
        QueryStatisticsV2()
      case QueryCurrentInputTuple() =>
        QueryCurrentInputTupleV2()
      case InitializeOperatorLogic(code, isSource, schema) =>
        InitializeOperatorLogicV2(
          code,
          isSource,
          schema.getAttributes.asScala.foldLeft(ListMap[String, String]())((list, attr) =>
            list + (attr.getName -> attr.getType.toString)
          )
        )
      case ReplayCurrentTuple() =>
        ReplayCurrentTupleV2()
      case ModifyPythonOperatorLogic(code, isSource) =>
        ModifyOperatorLogicV2(code, isSource)
      case EvaluateExpression(expression) =>
        EvaluateExpressionV2(expression)
      case WorkerDebugCommand(cmd) =>
        WorkerDebugCommandV2(cmd)
      case QuerySelfWorkloadMetrics() =>
        QuerySelfWorkloadMetricsV2()
      case _ =>
        throw new UnsupportedOperationException(
          s"V1 controlCommand $controlCommand cannot be converted to V2"
        )
    }

  }

  def controlCommandToV1(
      controlCommand: ControlCommandV2
  ): ControlCommand[_] = {
    controlCommand match {
      case WorkerExecutionCompletedV2() =>
        WorkerExecutionCompleted()
      case PythonConsoleMessageV2(message) =>
        ConsoleMessageTriggered(message)
      case PortCompletedV2(portId, input) => PortCompleted(portId, input)
      case _ =>
        throw new UnsupportedOperationException(
          s"V2 controlCommand $controlCommand cannot be converted to V1"
        )
    }
  }

  def controlReturnToV1(
      controlReturnV2: ControlReturnV2
  ): Any = {
    controlReturnV2.value match {
      case Empty                                                        => ()
      case _: ControlReturnV2.Value.CurrentInputTupleInfo               => null
      case selfWorkloadReturn: ControlReturnV2.Value.SelfWorkloadReturn =>
        // TODO: convert real samples back from PythonUDF.
        //  this is left hardcoded now since sampling is not currently enabled for PythonUDF.
        (
          selfWorkloadReturn.value.metrics,
          List[mutable.HashMap[ActorVirtualIdentity, List[Long]]]()
        )
      case exp: ControlReturnV2.Value.ControlException => ControlException(exp.value.msg)
      case _                                           => controlReturnV2.value.value
    }
  }

  def controlReturnToV2(controlReturn: Any): ControlReturnV2 = {
    controlReturn match {
      case _: Unit =>
        ControlReturnV2(Empty)
      case workerState: WorkerState =>
        ControlReturnV2(
          ControlReturnV2.Value.WorkerState(workerState)
        )
      case workerStatistics: WorkerStatistics =>
        ControlReturnV2(
          ControlReturnV2.Value.WorkerStatistics(workerStatistics)
        )
      case _ =>
        throw new UnsupportedOperationException(
          s"V1 controlReturn $controlReturn cannot be converted to V2"
        )

    }
  }

}
