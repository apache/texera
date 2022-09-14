package edu.uci.ics.amber.engine.architecture.deploysemantics.layer

import akka.actor.{ActorContext, ActorRef, Address, Deploy}
import akka.remote.RemoteScope
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.{DeploymentFilter, FollowPrevious, ForceLocal, UseAll}
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.{DeployStrategy, OneOnEach, RandomDeployment, RoundRobinDeployment}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.RegisterActorRef
import edu.uci.ics.amber.engine.architecture.pythonworker.PythonWorkflowWorker
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.{HashBasedShufflePartitioning, OneToOnePartitioning, Partitioning, RangeBasedShufflePartitioning, RoundRobinPartitioning}
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{COMPLETED, PAUSED, READY, RUNNING, UNINITIALIZED}
import edu.uci.ics.amber.engine.architecture.worker.statistics.{WorkerState, WorkerStatistics}
import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentityUtil.makeLayer
import edu.uci.ics.amber.engine.common.{Constants, IOperatorExecutor}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LayerIdentity, LinkIdentity, OperatorIdentity}
import edu.uci.ics.texera.web.workflowruntimestate.{OperatorRuntimeStats, WorkflowAggregatedState}
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.operators.udf.pythonV2.PythonUDFOpExecV2
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}
import org.jgrapht.traverse.TopologicalOrderIterator

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.{ClassTag, classTag}


object WorkerLayer {
  type WorkerLayer = WorkerLayerImpl[_ <: IOperatorExecutor]

  def oneToOneLayer
  (
    opId: OperatorIdentity,
    opExec: ((Int, WorkerLayer)) => IOperatorExecutor,
  ): WorkerLayer = oneToOneLayer(layerId = makeLayer(opId, "main"), opExec)

  def oneToOneLayer
  (
    layerId: LayerIdentity,
    opExec: ((Int, WorkerLayer)) => IOperatorExecutor,
  ): WorkerLayer = {
    WorkerLayerImpl(
      layerId,
      initIOperatorExecutor = opExec,
      deploymentFilter = UseAll(),
      deployStrategy = RoundRobinDeployment(),
    )
  }

  def manyToOneLayer
  (
    opId: OperatorIdentity,
    opExec: ((Int, WorkerLayer)) => IOperatorExecutor,
  ): WorkerLayer = manyToOneLayer(makeLayer(opId, "main"), opExec)


  def manyToOneLayer
  (
    layerId: LayerIdentity,
    opExec: ((Int, WorkerLayer)) => IOperatorExecutor,
  ): WorkerLayer = {
    WorkerLayerImpl(
      layerId,
      initIOperatorExecutor = opExec,
      numWorkers = 1,
      deploymentFilter = UseAll(),
      deployStrategy = RoundRobinDeployment(),
    )
  }


  def sqlSourceLayer
  (
    opId: OperatorIdentity,
    opExec: ((Int, WorkerLayer)) => IOperatorExecutor,
  ): WorkerLayer = {
    WorkerLayerImpl(
      id = makeLayer(opId, "main"),
      initIOperatorExecutor = opExec,
      numWorkers = 1,
      deploymentFilter = UseAll(),
      deployStrategy = OneOnEach(),
    )
  }

  def localLayer
  (
    opId: OperatorIdentity,
    opExec: ((Int, WorkerLayer)) => IOperatorExecutor,
  ): WorkerLayer = localLayer(makeLayer(opId, "main"), opExec)

  def localLayer
  (
    layerId: LayerIdentity,
    opExec: ((Int, WorkerLayer)) => IOperatorExecutor,
  ): WorkerLayer = {
    WorkerLayerImpl(
      id = layerId,
      initIOperatorExecutor = opExec,
      numWorkers = 1,
      deploymentFilter = ForceLocal(),
      deployStrategy = RandomDeployment(),
    )
  }

  def hashLayer
  (
    opId: OperatorIdentity,
    opExec: ((Int, WorkerLayer)) => IOperatorExecutor,
    hashColumnIndices: Array[Int]
  ): WorkerLayer = hashLayer(makeLayer(opId, "main"), opExec, hashColumnIndices)

  def hashLayer
  (
    layerId: LayerIdentity,
    opExec: ((Int, WorkerLayer)) => IOperatorExecutor,
    hashColumnIndices: Array[Int]
  ): WorkerLayer = {
    val partitionRequirement = HashBasedShufflePartitioning(
      Constants.defaultBatchSize, List(), hashColumnIndices)
    oneToOneLayer(layerId, opExec).copy(partitionRequirement = List(Option(partitionRequirement)))
  }

}


case class WorkerLayerImpl[T <: IOperatorExecutor: ClassTag]
(
    id: LayerIdentity,
    initIOperatorExecutor: ((Int, WorkerLayerImpl[_ <: IOperatorExecutor])) => T,
    numWorkers: Int = Constants.numWorkerPerNode,
    inputPorts: List[InputPort] = List(InputPort("")),
    outputPorts: List[OutputPort] = List(OutputPort("")),
    deploymentFilter: DeploymentFilter = UseAll(),
    deployStrategy: DeployStrategy = RoundRobinDeployment(),
    partitionRequirement: List[Option[Partitioning]] = List(),
    inputToOrdinalMapping: Map[LinkIdentity, Int] = Map(),
    outputToOrdinalMapping: Map[LinkIdentity, Int] = Map(),
    blockingInputs: List[Int] = List(),
    dependency: Map[Int, Int] = Map(),
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
    assert(port < this.outputPorts.size, s"cannot add output on port $port, all ports: $outputPorts")
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
    if (reqOpt.get == Partitioning.Empty) {
      return Array()
    }
    val req = reqOpt.get.asInstanceOf[Partitioning.NonEmpty]

    val partitionColumnIndices = req match {
      case OneToOnePartitioning(_, _) =>
        throw new RuntimeException("partition requirement cannot be one-to-one")
      case RoundRobinPartitioning(_, _) =>
        throw new RuntimeException("partition requirement cannot be round-robin")
      case HashBasedShufflePartitioning(_, _, hashColumnIndices) => hashColumnIndices
      case RangeBasedShufflePartitioning(_, _, rangeColumnIndices, _, _) => rangeColumnIndices
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
             prev: Array[WorkerLayer],
             all: Array[Address],
             parentNetworkCommunicationActorRef: ActorRef,
             context: ActorContext,
             allUpstreamLinkIds: Set[LinkIdentity],
             workerToLayer: mutable.HashMap[ActorVirtualIdentity, WorkerLayer],
             workerToOperatorExec: mutable.HashMap[ActorVirtualIdentity, IOperatorExecutor]
           ): Unit = {
    deployStrategy.initialize(deploymentFilter.filter(prev, all, context.self.path.address))
    workers = ListMap((0 until numWorkers).map { i =>
      val operatorExecutor: IOperatorExecutor = initIOperatorExecutor((i, this))
      val workerId: ActorVirtualIdentity =
        ActorVirtualIdentity(s"Worker:WF${id.workflow}-${id.operator}-${id.layerID}-$i")
      val address: Address = deployStrategy.next()
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
    }: _*)
  }
}
