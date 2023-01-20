package edu.uci.ics.amber.engine.architecture.deploysemantics.layer

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.locationpreference.{
  LocationPreference,
  PreferController
}
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{
  COMPLETED,
  PAUSED,
  READY,
  RUNNING,
  UNINITIALIZED
}
import edu.uci.ics.amber.engine.architecture.worker.statistics.{WorkerState, WorkerStatistics}
import edu.uci.ics.amber.engine.common.virtualidentity.util.makeLayer
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  LinkIdentity,
  OperatorIdentity,
  WorkerIdentity
}
import edu.uci.ics.amber.engine.common.{Constants, IOperatorExecutor}
import edu.uci.ics.texera.web.workflowruntimestate.{OperatorRuntimeStats, WorkflowAggregatedState}
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.workflow.{HashPartition, PartitionInfo, SinglePartition}
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}
import org.jgrapht.traverse.TopologicalOrderIterator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.{ClassTag, classTag}

object NewOpExecConfig {
  type NewOpExecConfig = OpExecConfigImpl[_ <: IOperatorExecutor]

  def oneToOneLayer[T <: IOperatorExecutor: ClassTag](
      opId: OperatorIdentity,
      opExec: ((Int, NewOpExecConfig)) => T
  ): OpExecConfigImpl[T] = oneToOneLayer(layerId = makeLayer(opId, "main"), opExec)

  def oneToOneLayer[T <: IOperatorExecutor: ClassTag](
      layerId: LayerIdentity,
      opExec: ((Int, NewOpExecConfig)) => T
  ): OpExecConfigImpl[T] = OpExecConfigImpl(layerId, initIOperatorExecutor = opExec)

  def manyToOneLayer[T <: IOperatorExecutor: ClassTag](
      opId: OperatorIdentity,
      opExec: ((Int, NewOpExecConfig)) => T
  ): OpExecConfigImpl[T] = manyToOneLayer(makeLayer(opId, "main"), opExec)

  def manyToOneLayer[T <: IOperatorExecutor: ClassTag](
      layerId: LayerIdentity,
      opExec: ((Int, NewOpExecConfig)) => T
  ): OpExecConfigImpl[T] = {
    OpExecConfigImpl(
      layerId,
      initIOperatorExecutor = opExec,
      numWorkers = 1,
      partitionRequirement = List(Option(SinglePartition())),
      derivePartition = _ => SinglePartition()
    )
  }

  def localLayer[T <: IOperatorExecutor: ClassTag](
      opId: OperatorIdentity,
      opExec: ((Int, NewOpExecConfig)) => T
  ): OpExecConfigImpl[T] = localLayer(makeLayer(opId, "main"), opExec)

  def localLayer[T <: IOperatorExecutor: ClassTag](
      layerId: LayerIdentity,
      opExec: ((Int, NewOpExecConfig)) => T
  ): OpExecConfigImpl[T] = {
    manyToOneLayer(layerId, opExec).copy(locationPreference = Option(new PreferController()))
  }

  def hashLayer[T <: IOperatorExecutor: ClassTag](
      opId: OperatorIdentity,
      opExec: ((Int, NewOpExecConfig)) => T,
      hashColumnIndices: Array[Int]
  ): OpExecConfigImpl[T] = hashLayer(makeLayer(opId, "main"), opExec, hashColumnIndices)

  def hashLayer[T <: IOperatorExecutor: ClassTag](
      layerId: LayerIdentity,
      opExec: ((Int, NewOpExecConfig)) => T,
      hashColumnIndices: Array[Int]
  ): OpExecConfigImpl[T] = {
    OpExecConfigImpl[T](
      id = layerId,
      initIOperatorExecutor = opExec,
      partitionRequirement = List(Option(HashPartition(hashColumnIndices))),
      derivePartition = _ => HashPartition(hashColumnIndices)
    )
  }

  case class OpExecConfigImpl[T <: IOperatorExecutor: ClassTag](
      id: LayerIdentity,
      // function to create an operator executor instance
      // parameters: 1: worker index, 2: this worker layer object
      initIOperatorExecutor: ((Int, OpExecConfigImpl[_ <: IOperatorExecutor])) => T,
      // preference of parallelism (number of workers)
      numWorkers: Int = Constants.numWorkerPerNode,
      // preference of worker placement
      locationPreference: Option[LocationPreference] = None,
      // requirement of partition policy (hash/range/single/none) on inputs
      partitionRequirement: List[Option[PartitionInfo]] = List(),
      // derive the output partition info given the input partitions
      // if not specified, by default the output partition is the same as input partition
      derivePartition: List[PartitionInfo] => PartitionInfo = inputParts => inputParts.head,
      // input/output ports of the physical operator
      // for operators with multiple input/output ports: must set these variables properly
      inputPorts: List[InputPort] = List(InputPort("")),
      outputPorts: List[OutputPort] = List(OutputPort("")),
      // mapping of all input/output operators connected on a specific input/output port index
      inputToOrdinalMapping: Map[LinkIdentity, Int] = Map(),
      outputToOrdinalMapping: Map[LinkIdentity, Int] = Map(),
      // input ports that are blocking
      blockingInputs: List[Int] = List(),
      // execution dependency of ports
      dependency: Map[Int, Int] = Map()
  ) {

    /*
     * Variables related to runtime information
     */

    // workers of this operator
    var workers: Map[WorkerIdentity, WorkerInfo] =
      Map[WorkerIdentity, WorkerInfo]()

    var attachedBreakpoints = new mutable.HashMap[String, GlobalBreakpoint[_]]()
    var caughtLocalExceptions = new mutable.HashMap[WorkerIdentity, Throwable]()
    var workerToWorkloadInfo = new mutable.HashMap[WorkerIdentity, WorkerWorkloadInfo]()

    /*
     * Helper functions related to compile-time operations
     */

    // creates a copy with the specified port information
    def withPorts(operatorInfo: OperatorInfo) = {
      this.copy(inputPorts = operatorInfo.inputPorts, outputPorts = operatorInfo.outputPorts)
    }

    // creates a copy with an additional input port
    def addInput(from: LayerIdentity, port: Int) = {
      assert(port < this.inputPorts.size, s"cannot add input on port $port, all ports: $inputPorts")
      this.copy(inputToOrdinalMapping =
        inputToOrdinalMapping + (LinkIdentity(from, this.id) -> port)
      )
    }

    // creates a copy with an additional input port
    def addOutput(to: LayerIdentity, port: Int) = {
      assert(
        port < this.outputPorts.size,
        s"cannot add output on port $port, all ports: $outputPorts"
      )
      this.copy(outputToOrdinalMapping =
        outputToOrdinalMapping + (LinkIdentity(this.id, to) -> port)
      )
    }

    // creates a copy with the new ID
    def withId(id: LayerIdentity) = this.copy(id = id)

    // creates a copy with the number of workers specified
    def withNumWorkers(numWorkers: Int) = this.copy(numWorkers = numWorkers)

    // return the runtime class of the corresponding OperatorExecutor
    def opExecClass: Class[_] = classTag[T].runtimeClass

    // returns all input links on a specific input port
    def getInputLinks(portIndex: Int): List[LinkIdentity] = {
      inputToOrdinalMapping.filter(p => p._2 == portIndex).keys.toList
    }

    // returns all the input operators on a specific input port
    def getInputOperators(portIndex: Int): List[LayerIdentity] = {
      getInputLinks(portIndex).map(link => link.from)
    }

    /**
      * Tells whether the input on this link is blocking i.e. the operator doesn't output anything till this link
      * outputs all its tuples
      */
    def isInputBlocking(input: LinkIdentity): Boolean = {
      inputToOrdinalMapping.get(input).exists(port => blockingInputs.contains(port))
    }

    /**
      * Some operators process their inputs in a particular order. Eg: 2 phase hash join first
      * processes the build input, then the probe input.
      */
    def getInputProcessingOrder(): Array[LinkIdentity] = {
      val dependencyDag = new DirectedAcyclicGraph[LinkIdentity, DefaultEdge](classOf[DefaultEdge])
      dependency.foreach(dep => {
        val prevInOrder = inputToOrdinalMapping.find(pair => pair._2 == dep._2).get._1
        val nextInOrder = inputToOrdinalMapping.find(pair => pair._2 == dep._1).get._1
        if (!dependencyDag.containsVertex(prevInOrder)) {
          dependencyDag.addVertex(prevInOrder)
        }
        if (!dependencyDag.containsVertex(nextInOrder)) {
          dependencyDag.addVertex(nextInOrder)
        }
        dependencyDag.addEdge(prevInOrder, nextInOrder)
      })
      val topologicalIterator =
        new TopologicalOrderIterator[LinkIdentity, DefaultEdge](dependencyDag)
      val processingOrder = new ArrayBuffer[LinkIdentity]()
      while (topologicalIterator.hasNext) {
        processingOrder.append(topologicalIterator.next())
      }
      processingOrder.toArray
    }

    /*
     * Functions related to runtime operations
     */

    def assignBreakpoint(breakpoint: GlobalBreakpoint[_]): Array[ActorVirtualIdentity] = {
      identifiers
    }

    def isBuilt: Boolean = workers.nonEmpty

    def identifiers: Array[ActorVirtualIdentity] = workers.values.map(_.id).toArray

    def states: Array[WorkerState] = workers.values.map(_.state).toArray

    def statistics: Array[WorkerStatistics] = workers.values.map(_.stats).toArray

    def getAllWorkers: Iterable[WorkerIdentity] = workers.keys

    def getWorker(id: WorkerIdentity): WorkerInfo = {
      workers(id)
    }

    def getWorkerWorkloadInfo(id: WorkerIdentity): WorkerWorkloadInfo = {
      if (!workerToWorkloadInfo.contains(id)) {
        workerToWorkloadInfo(id) = WorkerWorkloadInfo(0L, 0L)
      }
      workerToWorkloadInfo(id)
    }

    def setAllWorkerState(state: WorkerState): Unit = {
      (0 until numWorkers).foreach(states.update(_, state))
    }

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

    def getAllWorkerStates: Iterable[WorkerState] = states

    def getInputRowCount: Long = statistics.map(_.inputTupleCount).sum

    def getOutputRowCount: Long = statistics.map(_.outputTupleCount).sum

  }
}
