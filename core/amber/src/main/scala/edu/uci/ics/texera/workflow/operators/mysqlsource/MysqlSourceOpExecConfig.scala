package edu.uci.ics.texera.workflow.operators.mysqlsource

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.UseAll
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.OneOnEach
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{ActorLayer, GeneratorWorkerLayer}
import edu.uci.ics.amber.engine.architecture.worker.WorkerState
import edu.uci.ics.amber.engine.common.ambertag.{LayerTag, OperatorIdentifier}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class MysqlSourceOpExecConfig(
    tag: OperatorIdentifier,
    opExec: Int => SourceOperatorExecutor
) extends OpExecConfig(tag) {

  override lazy val topology: Topology = {
    new Topology(
      Array(
        new GeneratorWorkerLayer(
          LayerTag(tag, "main"),
          opExec,
          1,
          UseAll(), // it's source operator
          OneOnEach()
        )
      ),
      Array(),
      Map()
    )
  }

  override def assignBreakpoint(
      topology: Array[ActorLayer],
      states: mutable.AnyRefMap[ActorRef, WorkerState.Value],
      breakpoint: GlobalBreakpoint
)(implicit timeout: Timeout, ec: ExecutionContext, log: LoggingAdapter): Unit = {
    breakpoint.partition(topology(0).layer.filter(states(_) != WorkerState.Completed))
  }

  override def getInputNum(from: OperatorIdentifier): Int = ???

}
