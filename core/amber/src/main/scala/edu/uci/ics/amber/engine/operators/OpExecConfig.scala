package edu.uci.ics.amber.engine.operators

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{
  WorkerInfo,
  WorkerLayer,
  WorkerWorkloadInfo
}
import edu.uci.ics.amber.engine.architecture.linksemantics.LinkStrategy
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState._
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  LinkIdentity,
  OperatorIdentity
}
import edu.uci.ics.texera.web.workflowruntimestate.{OperatorRuntimeStats, WorkflowAggregatedState}
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}

import scala.collection.mutable
import scala.jdk.CollectionConverters.asScalaSet

object ShuffleType extends Enumeration {
  val HASH_BASED, RANGE_BASED, NONE =
    Value
}

abstract class OpExecConfig(val id: OperatorIdentity) extends Serializable {

  lazy val topology: Topology = null
  var inputToOrdinalMapping = new mutable.HashMap[LinkIdentity, (Int, String)]()
  var outputToOrdinalMapping = new mutable.HashMap[LinkIdentity, (Int, String)]()
  var attachedBreakpoints = new mutable.HashMap[String, GlobalBreakpoint[_]]()
  var caughtLocalExceptions = new mutable.HashMap[ActorVirtualIdentity, Throwable]()
  var workerToWorkloadInfo = new mutable.HashMap[ActorVirtualIdentity, WorkerWorkloadInfo]()
  var shuffleType: ShuffleType.Value = ShuffleType.NONE

  def getAllWorkers: Iterable[ActorVirtualIdentity] = topology.layers.flatMap(l => l.identifiers)

  def getWorker(id: ActorVirtualIdentity): WorkerInfo = {
    val layer = topology.layers.find(l => l.workers.contains(id)).get
    layer.workers(id)
  }

  def getWorkerWorkloadInfo(id: ActorVirtualIdentity): WorkerWorkloadInfo = {
    if (!workerToWorkloadInfo.contains(id)) {
      workerToWorkloadInfo(id) = WorkerWorkloadInfo(0L, 0L)
    }
    workerToWorkloadInfo(id)
  }

  def setAllWorkerState(state: WorkerState): Unit = {
    topology.layers.foreach { layer =>
      (0 until layer.numWorkers).foreach(layer.states.update(_, state))
    }
  }

  def getLayerFromWorkerID(id: ActorVirtualIdentity): WorkerLayer =
    topology.layers.find(_.identifiers.contains(id)).get

  def getOperatorStatistics: OperatorRuntimeStats =
    OperatorRuntimeStats(getState, getInputRowCount, getOutputRowCount)

  def getState: WorkflowAggregatedState = {
    val workerStates = getAllWorkerStates
    if (workerStates.isEmpty) {
      return WorkflowAggregatedState.UNINITIALIZED
    }
    if (workerStates.forall(_ == COMPLETED)) {
      return WorkflowAggregatedState.COMPLETED
    }
    if (workerStates.exists(_ == RUNNING)) {
      return WorkflowAggregatedState.RUNNING
    }
    val unCompletedWorkerStates = workerStates.filter(_ != COMPLETED)
    if (unCompletedWorkerStates.forall(_ == UNINITIALIZED)) {
      WorkflowAggregatedState.UNINITIALIZED
    } else if (unCompletedWorkerStates.forall(_ == PAUSED)) {
      WorkflowAggregatedState.PAUSED
    } else if (unCompletedWorkerStates.forall(_ == READY)) {
      WorkflowAggregatedState.READY
    } else {
      WorkflowAggregatedState.UNKNOWN
    }
  }

  def getAllWorkerStates: Iterable[WorkerState] = topology.layers.flatMap(l => l.states)

  def getInputRowCount: Long = topology.layers.head.statistics.map(_.inputTupleCount).sum

  def getOutputRowCount: Long = topology.layers.last.statistics.map(_.outputTupleCount).sum

  def requiresShuffle: Boolean = shuffleType != ShuffleType.NONE

  def getRangeShuffleMinAndMax: (Long, Long) = (Long.MinValue, Long.MaxValue)

  def setInputToOrdinalMapping(input: LinkIdentity, ordinal: Integer, name: String): Unit = {
    this.inputToOrdinalMapping.update(input, (ordinal, name))
  }

  def setOutputToOrdinalMapping(output: LinkIdentity, ordinal: Integer, name: String): Unit = {
    this.outputToOrdinalMapping.update(output, (ordinal, name))
  }

  /**
    * Tells whether the input on this link is blocking i.e. the operator doesn't output anything till this link
    * outputs all its tuples
    */
  def isInputBlocking(input: LinkIdentity): Boolean = false

  /**
    * Some operators process their inputs in a particular order. Eg: 2 phase hash join first
    * processes the build input, then the probe input.
    */
  def getInputProcessingOrder(): Array[LinkIdentity] = null

  def getPartitionColumnIndices(layer: LayerIdentity): Array[Int] = ???

  def assignBreakpoint(breakpoint: GlobalBreakpoint[_]): Array[ActorVirtualIdentity]

  class Topology(
      var layers: Array[WorkerLayer],
      var links: Array[LinkStrategy]
  ) extends Serializable {
    // topology only supports a chain of layers,
    // the link order must follow the layer order
    assert(layers.length == links.length + 1)
    (0 until links.length).foreach(i => {
      assert(layers(i) == links(i).from)
      assert(layers(i + 1) == links(i).to)
    })
  }

}
