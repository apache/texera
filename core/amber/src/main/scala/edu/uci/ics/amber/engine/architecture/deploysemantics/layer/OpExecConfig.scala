package edu.uci.ics.amber.engine.architecture.deploysemantics.layer

import akka.actor.{ActorContext, ActorRef, Address, Deploy, Props}
import akka.remote.RemoteScope
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.ControllerConfig
import edu.uci.ics.amber.engine.architecture.deploysemantics.locationpreference.{
  AddressInfo,
  LocationPreference,
  PreferController,
  RoundRobinPreference
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  NetworkSenderActorRef,
  RegisterActorRef
}
import edu.uci.ics.amber.engine.architecture.pythonworker.PythonWorkflowWorker
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker
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
  OperatorIdentity
}
import edu.uci.ics.amber.engine.common.{Constants, IOperatorExecutor, ISourceOperatorExecutor}
import edu.uci.ics.texera.web.workflowruntimestate.{OperatorRuntimeStats, WorkflowAggregatedState}
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.workflow.{HashPartition, PartitionInfo, SinglePartition}
import edu.uci.ics.texera.workflow.operators.udf.python.PythonUDFOpExecV2
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}
import org.jgrapht.traverse.TopologicalOrderIterator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait OpExecFunc extends (((Int, OpExecConfig)) => IOperatorExecutor) with java.io.Serializable

object OpExecConfig {

  def oneToOneLayer(opId: OperatorIdentity, opExec: OpExecFunc): OpExecConfig =
    oneToOneLayer(layerId = makeLayer(opId, "main"), opExec)

  def oneToOneLayer(layerId: LayerIdentity, opExec: OpExecFunc): OpExecConfig =
    OpExecConfig(layerId, initIOperatorExecutor = opExec)

  def manyToOneLayer(opId: OperatorIdentity, opExec: OpExecFunc): OpExecConfig =
    manyToOneLayer(makeLayer(opId, "main"), opExec)

  def manyToOneLayer(layerId: LayerIdentity, opExec: OpExecFunc): OpExecConfig = {
    OpExecConfig(
      layerId,
      initIOperatorExecutor = opExec,
      numWorkers = 1,
      partitionRequirement = List(Option(SinglePartition())),
      derivePartition = _ => SinglePartition()
    )
  }

  def localLayer(opId: OperatorIdentity, opExec: OpExecFunc): OpExecConfig =
    localLayer(makeLayer(opId, "main"), opExec)

  def localLayer(layerId: LayerIdentity, opExec: OpExecFunc): OpExecConfig = {
    manyToOneLayer(layerId, opExec).copy(locationPreference = Option(new PreferController()))
  }

  def hashLayer(
      opId: OperatorIdentity,
      opExec: OpExecFunc,
      hashColumnIndices: Array[Int]
  ): OpExecConfig = hashLayer(makeLayer(opId, "main"), opExec, hashColumnIndices)

  def hashLayer(
      layerId: LayerIdentity,
      opExec: OpExecFunc,
      hashColumnIndices: Array[Int]
  ): OpExecConfig = {
    OpExecConfig(
      id = layerId,
      initIOperatorExecutor = opExec,
      partitionRequirement = List(Option(HashPartition(hashColumnIndices))),
      derivePartition = _ => HashPartition(hashColumnIndices)
    )
  }

}

case class OpExecConfig(
    id: LayerIdentity,
    // function to create an operator executor instance
    // parameters: 1: worker index, 2: this worker layer object
    initIOperatorExecutor: OpExecFunc,
    // preference of parallelism (total number of workers)
    numWorkers: Int = Constants.currentWorkerNum,
    // preference of worker placement
    locationPreference: Option[LocationPreference] = None,
    // requirement of partition policy (hash/range/single/none) on inputs
    partitionRequirement: List[Option[PartitionInfo]] = List(),
    // derive the output partition info given the input partitions
    // if not specified, by default the output partition is the same as input partition
    derivePartition: List[PartitionInfo] => PartitionInfo = inputParts => inputParts.head,
    // input/output ports of the physical operator
    // for operators with multiple input/output ports: must set these variables properly
    inputPorts: List[InputPort] = List(InputPort()),
    outputPorts: List[OutputPort] = List(OutputPort()),
    // mapping of all input/output operators connected on a specific input/output port index
    inputToOrdinalMapping: Map[LinkIdentity, Int] = Map(),
    outputToOrdinalMapping: Map[LinkIdentity, Int] = Map(),
    // input ports that are blocking
    blockingInputs: List[Int] = List(),
    // execution dependency of ports
    dependency: Map[Int, Int] = Map(),
    isOneToManyOp: Boolean = false
) {

  // all the "dependee" links are also blocking inputs
  lazy val realBlockingInputs: List[Int] = (blockingInputs ++ dependency.values).distinct

  // return the runtime class of the corresponding OperatorExecutor
  lazy private val tempOperatorInstance: IOperatorExecutor = initIOperatorExecutor((0, this))
  lazy val opExecClass: Class[_ <: IOperatorExecutor] =
    tempOperatorInstance.getClass

  /*
   * Variables related to runtime information
   */

  // workers of this operator
  var workers: Map[ActorVirtualIdentity, WorkerInfo] =
    Map[ActorVirtualIdentity, WorkerInfo]()
  // actor props of each worker, it's not constructed as an actor yet for recovery purposes
  val workerToActorProps = new mutable.HashMap[ActorVirtualIdentity, Props]()

  var attachedBreakpoints = new mutable.HashMap[String, GlobalBreakpoint[_]]()
  var caughtLocalExceptions = new mutable.HashMap[ActorVirtualIdentity, Throwable]()
  var workerToWorkloadInfo = new mutable.HashMap[ActorVirtualIdentity, WorkerWorkloadInfo]()

  /*
   * Helper functions related to compile-time operations
   */

  def isSourceOperator: Boolean =
    classOf[ISourceOperatorExecutor].isAssignableFrom(opExecClass)

  def isPythonOperator: Boolean =
    classOf[PythonUDFOpExecV2].isAssignableFrom(opExecClass)

  def getPythonCode: String = {
    if (!isPythonOperator) {
      throw new RuntimeException("operator " + id + " is not a python operator")
    }
    tempOperatorInstance.asInstanceOf[PythonUDFOpExecV2].getCode
  }

  // creates a copy with the specified port information
  def withPorts(operatorInfo: OperatorInfo): OpExecConfig = {
    this.copy(inputPorts = operatorInfo.inputPorts, outputPorts = operatorInfo.outputPorts)
  }

  def withInputPorts(inputs: List[InputPort]): OpExecConfig = {
    this.copy(inputPorts = inputs)
  }
  def withOutputPorts(outputs: List[OutputPort]): OpExecConfig = {
    this.copy(outputPorts = outputs)
  }

  // creates a copy with an additional input operator specified on an input port
  def addInput(from: LayerIdentity, fromPort: Int, toPort: Int): OpExecConfig = {
    this.copy(inputToOrdinalMapping =
      inputToOrdinalMapping + (LinkIdentity(from, fromPort, this.id, toPort) -> toPort)
    )
  }

  // creates a copy with an additional output operator specified on an output port
  def addOutput(to: LayerIdentity, fromPort: Int, toPort: Int): OpExecConfig = {
    this.copy(outputToOrdinalMapping =
      outputToOrdinalMapping + (LinkIdentity(this.id, fromPort, to, toPort) -> fromPort)
    )
  }

  // creates a copy with a removed input operator
  def removeInput(link: LinkIdentity): OpExecConfig = {
    this.copy(inputToOrdinalMapping = inputToOrdinalMapping - link)
  }

  // creates a copy with a removed output operator
  def removeOutput(link: LinkIdentity): OpExecConfig = {
    this.copy(outputToOrdinalMapping = outputToOrdinalMapping - link)
  }

  // creates a copy with the new ID
  def withId(id: LayerIdentity): OpExecConfig = this.copy(id = id)

  // creates a copy with the number of workers specified
  def withNumWorkers(numWorkers: Int): OpExecConfig = this.copy(numWorkers = numWorkers)

  // creates a copy with the specified property that whether this operator is one-to-many
  def withIsOneToManyOp(isOneToManyOp: Boolean): OpExecConfig =
    this.copy(isOneToManyOp = isOneToManyOp)

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
    inputToOrdinalMapping.get(input).exists(port => realBlockingInputs.contains(port))
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

  def build(
      addressInfo: AddressInfo,
      parentNetworkCommunicationActorRef: NetworkSenderActorRef,
      context: ActorContext,
      workerToLayer: mutable.HashMap[ActorVirtualIdentity, OpExecConfig],
      controllerConf: ControllerConfig
  ): Unit = {
    workers = (0 until numWorkers)
      .map(i => {
        val workerId: ActorVirtualIdentity =
          ActorVirtualIdentity(s"Worker:WF${id.workflow}-${id.operator}-${id.layerID}-$i")
        val locationPreference = this.locationPreference.getOrElse(new RoundRobinPreference())
        val preferredAddress = locationPreference.getPreferredLocation(addressInfo, this, i)

        val workflowWorker = if (this.isPythonOperator) {
          PythonWorkflowWorker.props(workerId, i, this, parentNetworkCommunicationActorRef)
        } else {
          WorkflowWorker.props(
            workerId,
            i,
            this,
            parentNetworkCommunicationActorRef,
            controllerConf.supportFaultTolerance
          )
        }
        workerToActorProps(workerId) = workflowWorker
        val ref =
          context.actorOf(workflowWorker.withDeploy(Deploy(scope = RemoteScope(preferredAddress))))

        parentNetworkCommunicationActorRef ! RegisterActorRef(workerId, ref)
        workerToLayer(workerId) = this
        (
          workerId,
          WorkerInfo(
            workerId,
            UNINITIALIZED,
            WorkerStatistics(UNINITIALIZED, 0, 0),
            ref
          )
        )
      })
      .toMap
  }

  def recover(actorId: ActorVirtualIdentity, address: Address, context: ActorContext): ActorRef = {
    val newRef =
      context.actorOf(workerToActorProps(actorId).withDeploy(Deploy(scope = RemoteScope(address))))
    workers(actorId).ref = newRef
    newRef
  }
}
