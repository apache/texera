package edu.uci.ics.amber.engine.architecture.deploysemantics.layer

import akka.actor.{ActorContext, ActorRef, Deploy}
import akka.remote.RemoteScope
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.deploysemantics.locationpreference.{AddressInfo, LocationPreference, PreferController, RoundRobinPreference}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.RegisterActorRef
import edu.uci.ics.amber.engine.architecture.pythonworker.PythonWorkflowWorker
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings._
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{COMPLETED, PAUSED, READY, RUNNING, UNINITIALIZED}
import edu.uci.ics.amber.engine.architecture.worker.statistics.{WorkerState, WorkerStatistics}
import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentityUtil.makeLayer
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LayerIdentity, LinkIdentity, OperatorIdentity}
import edu.uci.ics.amber.engine.common.{Constants, IOperatorExecutor}
import edu.uci.ics.texera.web.workflowruntimestate.{OperatorRuntimeStats, WorkflowAggregatedState}
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.workflow
import edu.uci.ics.texera.workflow.common.workflow.{HashPartition, PartitionInfo, RangePartition, RoundRobin, Singleton}
import edu.uci.ics.texera.workflow.operators.udf.pythonV2.PythonUDFOpExecV2
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}
import org.jgrapht.traverse.TopologicalOrderIterator

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.{ClassTag, classTag}

object WorkerLayer {
  type WorkerLayer = WorkerLayerImpl[_ <: IOperatorExecutor]

  def oneToOneLayer(
      opId: OperatorIdentity,
      opExec: ((Int, WorkerLayer)) => IOperatorExecutor
  ): WorkerLayer = oneToOneLayer(layerId = makeLayer(opId, "main"), opExec)

  def oneToOneLayer(
      layerId: LayerIdentity,
      opExec: ((Int, WorkerLayer)) => IOperatorExecutor
  ): WorkerLayer = {
    WorkerLayerImpl(
      layerId,
      initIOperatorExecutor = opExec
    )
  }

  def manyToOneLayer(
      opId: OperatorIdentity,
      opExec: ((Int, WorkerLayer)) => IOperatorExecutor
  ): WorkerLayer = manyToOneLayer(makeLayer(opId, "main"), opExec)

  def manyToOneLayer(
      layerId: LayerIdentity,
      opExec: ((Int, WorkerLayer)) => IOperatorExecutor
  ): WorkerLayer = {
    WorkerLayerImpl(
      layerId,
      initIOperatorExecutor = opExec,
      partitionRequirement = List(Option(Singleton())),
      derivePartition = _ => Singleton()
    )
  }

  def localLayer(
      opId: OperatorIdentity,
      opExec: ((Int, WorkerLayer)) => IOperatorExecutor
  ): WorkerLayer = localLayer(makeLayer(opId, "main"), opExec)

  def localLayer(
      layerId: LayerIdentity,
      opExec: ((Int, WorkerLayer)) => IOperatorExecutor
  ): WorkerLayer = {
    manyToOneLayer(layerId, opExec).copy(locationPreference = Option(new PreferController()))
  }

  def hashLayer(
      opId: OperatorIdentity,
      opExec: ((Int, WorkerLayer)) => IOperatorExecutor,
      hashColumnIndices: Array[Int]
  ): WorkerLayer = hashLayer(makeLayer(opId, "main"), opExec, hashColumnIndices)

  def hashLayer(
      layerId: LayerIdentity,
      opExec: ((Int, WorkerLayer)) => IOperatorExecutor,
      hashColumnIndices: Array[Int]
  ): WorkerLayer = {
    WorkerLayerImpl(
      id = layerId,
      initIOperatorExecutor = opExec,
      partitionRequirement = List(Option(HashPartition(hashColumnIndices))),
      derivePartition = _ => HashPartition(hashColumnIndices)
    )
  }

}

case class WorkerLayerImpl[T <: IOperatorExecutor: ClassTag](
    id: LayerIdentity,
    initIOperatorExecutor: ((Int, WorkerLayerImpl[_ <: IOperatorExecutor])) => T,
    // preference of parallelism (number of workers)
    numWorkers: Int = Constants.numWorkerPerNode,
    // preference of worker placement
    locationPreference: Option[LocationPreference] = None,
    // requirement of partition policy (hash/range/singleton) on inputs
    partitionRequirement: List[Option[PartitionInfo]] = List(),
    // derive the output partition info given the input partitions
    // the compiler ensure the partition requirements are satisfied
    derivePartition: List[PartitionInfo] => PartitionInfo = inputPartitions => inputPartitions.head,
    // input ports / output ports of the physical operator
    inputPorts: List[InputPort] = List(InputPort("")),
    outputPorts: List[OutputPort] = List(OutputPort("")),
    inputToOrdinalMapping: Map[LinkIdentity, Int] = Map(),
    outputToOrdinalMapping: Map[LinkIdentity, Int] = Map(),
    // the ports that are blocking
    blockingInputs: List[Int] = List(),
    // the execution dependency of ports
    dependency: Map[Int, Int] = Map()
) {

  // runtime related variables

  // workers of this operator
  var workers: ListMap[ActorVirtualIdentity, WorkerInfo] =
    ListMap[ActorVirtualIdentity, WorkerInfo]()

  var attachedBreakpoints = new mutable.HashMap[String, GlobalBreakpoint[_]]()
  var caughtLocalExceptions = new mutable.HashMap[ActorVirtualIdentity, Throwable]()
  var workerToWorkloadInfo = new mutable.HashMap[ActorVirtualIdentity, WorkerWorkloadInfo]()

  // helper functions

  def withPorts(operatorInfo: OperatorInfo) = {
    this.copy(inputPorts = operatorInfo.inputPorts, outputPorts = operatorInfo.outputPorts)
  }

  def addInput(from: LayerIdentity, port: Int) = {
    assert(port < this.inputPorts.size, s"cannot add input on port $port, all ports: $inputPorts")
    this.copy(inputToOrdinalMapping = inputToOrdinalMapping + (LinkIdentity(from, this.id) -> port))
  }

  def addOutput(to: LayerIdentity, port: Int) = {
    assert(
      port < this.outputPorts.size,
      s"cannot add output on port $port, all ports: $outputPorts"
    )
    this.copy(outputToOrdinalMapping = outputToOrdinalMapping + (LinkIdentity(this.id, to) -> port))
  }

  def withId(id: LayerIdentity) = this.copy(id = id)

  def withNumWorkers(numWorkers: Int) = this.copy(numWorkers = numWorkers)

  def opExecClass: Class[_] = classTag[T].runtimeClass

  def getInputLink(portIndex: Int): LinkIdentity = {
    inputToOrdinalMapping.find(p => p._2 == portIndex).get._1
  }

  // compile-time related functions

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
    val topologicalIterator = new TopologicalOrderIterator[LinkIdentity, DefaultEdge](dependencyDag)
    val processingOrder = new ArrayBuffer[LinkIdentity]()
    while (topologicalIterator.hasNext) {
      processingOrder.append(topologicalIterator.next())
    }
    processingOrder.toArray
  }

  def getPartitionColumnIndices(link: LinkIdentity): Array[Int] = {
    val port = inputToOrdinalMapping.get(link)
    if (port.isEmpty) {
      return Array()
    }
    val reqOpt = this.partitionRequirement.lift(port.get).flatten
    if (reqOpt.isEmpty) {
      return Array()
    }

    val partitionColumnIndices = reqOpt.get match {
      case HashPartition(hashColumnIndices) => hashColumnIndices
      case RangePartition(rangeColumnIndices, _, _) => rangeColumnIndices
      case Singleton() => List()
      case RoundRobin() => List()
      case workflow.Any() => List()
    }

    partitionColumnIndices.toArray
  }

  def assignBreakpoint(breakpoint: GlobalBreakpoint[_]): Array[ActorVirtualIdentity] = {
    identifiers
  }

  // runtime related functions

  def isBuilt: Boolean = workers.nonEmpty

  def identifiers: Array[ActorVirtualIdentity] = workers.values.map(_.id).toArray

  def states: Array[WorkerState] = workers.values.map(_.state).toArray

  def statistics: Array[WorkerStatistics] = workers.values.map(_.stats).toArray

  def getAllWorkers: Iterable[ActorVirtualIdentity] = workers.keys

  def getWorker(id: ActorVirtualIdentity): WorkerInfo = {
    workers(id)
  }

  def getWorkerWorkloadInfo(id: ActorVirtualIdentity): WorkerWorkloadInfo = {
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

  def getRangeShuffleMinAndMax: (Long, Long) = (Long.MinValue, Long.MaxValue)

  def build(
      addressInfo: AddressInfo,
      parentNetworkCommunicationActorRef: ActorRef,
      context: ActorContext,
      workerToLayer: mutable.HashMap[ActorVirtualIdentity, WorkerLayer],
      workerToOperatorExec: mutable.HashMap[ActorVirtualIdentity, IOperatorExecutor]
  ): Unit = {
    (0 until numWorkers).foreach(i => {
      val operatorExecutor: IOperatorExecutor = initIOperatorExecutor((i, this))
      val workerId: ActorVirtualIdentity =
        ActorVirtualIdentity(s"Worker:WF${id.workflow}-${id.operator}-${id.layerID}-$i")
      val locationPreference = this.locationPreference.getOrElse(new RoundRobinPreference())
      val address = locationPreference.getPreferredLocation(addressInfo, this, i)

      val allUpstreamLinkIds = inputToOrdinalMapping.keySet
      workerToOperatorExec(workerId) = operatorExecutor
      val ref: ActorRef = context.actorOf(
        if (operatorExecutor.isInstanceOf[PythonUDFOpExecV2]) {
          PythonWorkflowWorker
            .props(
              workerId,
              operatorExecutor,
              parentNetworkCommunicationActorRef,
              allUpstreamLinkIds
            )
            .withDeploy(Deploy(scope = RemoteScope(address)))
        } else {
          WorkflowWorker
            .props(
              workerId,
              operatorExecutor,
              parentNetworkCommunicationActorRef,
              allUpstreamLinkIds
            )
            .withDeploy(Deploy(scope = RemoteScope(address)))
        }
      )
      parentNetworkCommunicationActorRef ! RegisterActorRef(workerId, ref)
      workerToLayer(workerId) = this
      workerId -> WorkerInfo(
        workerId,
        UNINITIALIZED,
        WorkerStatistics(UNINITIALIZED, 0, 0)
      )
    })
  }
}
