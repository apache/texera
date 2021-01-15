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
import edu.uci.ics.amber.engine.common.ambertag.{AmberTag, LayerTag, OperatorIdentifier}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.operators.source.FileScanOpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class HashJoinOpExecConfig(
    override val tag: OperatorIdentifier,
    val opExec: Int => OperatorExecutor,
    val probeAttribute: String,
    val buildAttribute: String
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

  override def runtimeCheck(
      workflow: Workflow
  ): Option[mutable.HashMap[AmberTag, mutable.HashMap[AmberTag, mutable.HashSet[LayerTag]]]] = {
    assert(workflow.inLinks(tag).nonEmpty)
    var tmp = workflow.inLinks(tag).head
    var tableSize = Long.MaxValue
    for (tag <- workflow.inLinks(tag)) {
      workflow.operators(tag) match {
        case config: FileScanOpExecConfig =>
          if (tableSize > config.totalBytes) {
            tableSize = config.totalBytes
            tmp = tag
          }
        case _ =>
      }
    }
    buildTableTag = workflow.operators(tmp).topology.layers.last.tag
    Some(
      mutable.HashMap[AmberTag, mutable.HashMap[AmberTag, mutable.HashSet[LayerTag]]](
        workflow
          .inLinks(tag)
          .filter(_ != tmp)
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
      t.asInstanceOf[Tuple].getField(buildAttribute).hashCode()
    } else { t: ITuple =>
      t.asInstanceOf[Tuple].getField(probeAttribute).hashCode()
    }
  }

  override def getInputNum(from: OperatorIdentifier): Int = {
    if (from == OperatorIdentifier(buildTableTag.workflow, buildTableTag.operator)) {
      0
    } else {
      1
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
