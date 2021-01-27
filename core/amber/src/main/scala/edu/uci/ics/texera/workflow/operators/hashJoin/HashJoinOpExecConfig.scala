package edu.uci.ics.texera.workflow.operators.hashJoin

import akka.actor.ActorRef
import akka.util.Timeout
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.UseAll
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.worker.WorkerState
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.ambertag.{AmberTag, LayerTag, OperatorIdentifier}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.amber.error.WorkflowRuntimeError
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class HashJoinOpExecConfig(
    override val tag: OperatorIdentifier,
    val opExec: Int => OperatorExecutor,
    val probeAttributeName: String,
    val buildAttributeName: String
) extends OpExecConfig(tag) {

  var buildTableTag: LayerTag = _

  override lazy val topology: Topology = {
    new Topology(
      Array(
        new WorkerLayer(
          LayerTag(tag, "main"),
          opExec,
          Constants.defaultNumWorkers,
          UseAll(),
          RoundRobinDeployment()
        )
      ),
      Array(),
      Map()
    )
  }

  private def getBuildTableOpIdentifier(): OperatorIdentifier = {
    var buildOpId: Option[OperatorIdentifier] =
      inputToOrdinalMapping.keys.find(opId => (inputToOrdinalMapping(opId) == 0))
    buildOpId match {
      case Some(opId) => return opId
      case None =>
        val error = WorkflowRuntimeError(
          "No operator identifier has input num 0",
          "HashJoinOpExecConfig",
          Map()
        )
        opExecConfigLogger.logError(error)
        throw new WorkflowRuntimeException(error)
    }
  }

  override def runtimeCheck(
      workflow: Workflow
  ): Option[mutable.HashMap[AmberTag, mutable.HashMap[AmberTag, mutable.HashSet[LayerTag]]]] = {
    assert(workflow.inLinks(tag).nonEmpty)
    buildTableTag = workflow.operators(getBuildTableOpIdentifier()).topology.layers.last.tag
    Some(
      mutable.HashMap[AmberTag, mutable.HashMap[AmberTag, mutable.HashSet[LayerTag]]](
        workflow
          .inLinks(tag)
          .filter(_ != getBuildTableOpIdentifier())
          .flatMap(x => workflow.getSources(x))
          .map(x =>
            x -> mutable
              .HashMap[AmberTag, mutable.HashSet[LayerTag]](tag -> mutable.HashSet(buildTableTag))
          )
          .toSeq: _*
      )
    )
  }

  override def requiredShuffle: Boolean = true

  override def getShuffleHashFunction(layerTag: LayerTag): ITuple => Int = {
    if (layerTag == buildTableTag) { t: ITuple =>
      t.asInstanceOf[Tuple].getField(buildAttributeName).hashCode()
    } else { t: ITuple =>
      t.asInstanceOf[Tuple].getField(probeAttributeName).hashCode()
    }
  }

  override def assignBreakpoint(
      topology: Array[WorkerLayer],
      states: mutable.AnyRefMap[ActorRef, WorkerState.Value],
      breakpoint: GlobalBreakpoint
  )(implicit timeout: Timeout, ec: ExecutionContext) = {
    breakpoint.partition(topology(0).layer.filter(states(_) != WorkerState.Completed))
  }

}
