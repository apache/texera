package Engine.Operators.HashJoin

import Engine.Architecture.Breakpoint.GlobalBreakpoint.GlobalBreakpoint
import Engine.Architecture.Controller.Workflow
import Engine.Architecture.DeploySemantics.DeployStrategy.{RandomDeployment, RoundRobinDeployment}
import Engine.Architecture.DeploySemantics.DeploymentFilter.{FollowPrevious, UseAll}
import Engine.Architecture.DeploySemantics.Layer.{ActorLayer, ProcessorWorkerLayer}
import Engine.Architecture.LinkSemantics.LinkStrategy
import Engine.Architecture.Worker.WorkerState
import Engine.Common.AmberTag.{AmberTag, LayerTag, OperatorTag}
import Engine.Common.tuple.Tuple
import Engine.Operators.OperatorMetadata
import Engine.Operators.Scan.FileScanMetadata
import Engine.Operators.Scan.HDFSFileScan.HDFSFileScanMetadata
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class HashJoinMetadata[K](
    tag: OperatorTag,
    val numWorkers: Int,
    val innerTableIndex: Int,
    val outerTableIndex: Int
) extends OperatorMetadata(tag) {
  var innerTableTag: LayerTag = _

  override lazy val topology: Topology = {
    new Topology(
      Array(
        new ProcessorWorkerLayer(
          LayerTag(tag, "main"),
          _ => new HashJoinTupleProcessor[K](innerTableTag, innerTableIndex, outerTableIndex),
          numWorkers,
          UseAll(),
          RoundRobinDeployment()
        )
      ),
      Array(),
      Map()
    )
  }

  override def requiredShuffle: Boolean = true

  override def getShuffleHashFunction(layerTag: LayerTag): Tuple => Int = {
    if (layerTag == innerTableTag) { t: Tuple =>
      t.get(innerTableIndex).hashCode()
    } else { t: Tuple =>
      t.get(outerTableIndex).hashCode()
    }
  }

  override def runtimeCheck(
      workflow: Workflow
  ): Option[mutable.HashMap[AmberTag, mutable.HashMap[AmberTag, mutable.HashSet[LayerTag]]]] = {
    assert(workflow.inLinks(tag).nonEmpty)
    var tmp = workflow.inLinks(tag).head
    var tableSize = Long.MaxValue
    for (tag <- workflow.inLinks(tag)) {
      workflow.operators(tag) match {
        case metadata: FileScanMetadata =>
          if (tableSize > metadata.totalBytes) {
            tableSize = metadata.totalBytes
            tmp = tag
          }
        case _ =>
      }
    }
    innerTableTag = workflow.operators(tmp).topology.layers.last.tag
    Some(
      mutable.HashMap[AmberTag, mutable.HashMap[AmberTag, mutable.HashSet[LayerTag]]](
        workflow
          .inLinks(tag)
          .filter(_ != tmp)
          .flatMap(x => workflow.getSources(x))
          .map(x =>
            x -> mutable
              .HashMap[AmberTag, mutable.HashSet[LayerTag]](tag -> mutable.HashSet(innerTableTag))
          )
          .toSeq: _*
      )
    )
  }

  override def assignBreakpoint(
      topology: Array[ActorLayer],
      states: mutable.AnyRefMap[ActorRef, WorkerState.Value],
      breakpoint: GlobalBreakpoint
  )(implicit timeout: Timeout, ec: ExecutionContext, log: LoggingAdapter): Unit = {
    breakpoint.partition(topology(0).layer.filter(states(_) != WorkerState.Completed))
  }
}
