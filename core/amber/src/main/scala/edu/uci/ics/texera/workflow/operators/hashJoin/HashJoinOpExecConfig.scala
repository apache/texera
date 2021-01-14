package edu.uci.ics.texera.workflow.operators.hashJoin

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.UseAll
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.ambertag.{AmberTag, LayerTag, OperatorIdentifier}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor

import scala.collection.mutable

class HashJoinOpExecConfig(
    override val tag: OperatorIdentifier,
    val opExec: Int => OperatorExecutor,
    val outerAttribute: String,
    val innerAttribute: String
) extends OpExecConfig(tag) {

  var innerTableTag: LayerTag = _

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

  override def requiredShuffle: Boolean = true

  override def getShuffleHashFunction(layerTag: LayerTag): ITuple => Int = {

    if (layerTag == innerTableTag) { t: Tuple =>
      t.get(innerTableIndex).hashCode()
    } else { t: Tuple =>
      t.get(outerTableIndex).hashCode()
    }
  }

}
