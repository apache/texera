package edu.uci.ics.amber.engine.architecture.deploysemantics

import akka.actor.Deploy
import akka.remote.RemoteScope
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.common.AkkaActorService
import edu.uci.ics.amber.engine.architecture.controller.execution.OperatorExecution
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{
  OpExecInitInfo,
  OpExecInitInfoWithCode
}
import edu.uci.ics.amber.engine.architecture.deploysemantics.locationpreference.{
  AddressInfo,
  LocationPreference,
  PreferController,
  RoundRobinPreference
}
import edu.uci.ics.amber.engine.architecture.pythonworker.PythonWorkflowWorker
import edu.uci.ics.amber.engine.architecture.scheduling.config.OperatorConfig
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.{
  WorkerReplayInitialization,
  FaultToleranceConfig,
  StateRestoreConfig
}
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils
import edu.uci.ics.amber.engine.common.virtualidentity._
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort, PhysicalLink, PortIdentity}
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.common.workflow._
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}
import org.jgrapht.traverse.TopologicalOrderIterator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object PhysicalOp {

  /** all source operators should use sourcePhysicalOp to give the following configs:
    *  1) it initializes at the controller jvm.
    *  2) it only has 1 worker actor.
    *  3) it has no input ports.
    */
  def sourcePhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      logicalOpId: OperatorIdentity,
      opExecInitInfo: OpExecInitInfo
  ): PhysicalOp =
    sourcePhysicalOp(
      PhysicalOpIdentity(logicalOpId, "main"),
      workflowId,
      executionId,
      opExecInitInfo
    )

  def sourcePhysicalOp(
      physicalOpId: PhysicalOpIdentity,
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      opExecInitInfo: OpExecInitInfo
  ): PhysicalOp =
    PhysicalOp(
      physicalOpId,
      workflowId,
      executionId,
      opExecInitInfo,
      parallelizable = false,
      locationPreference = Option(new PreferController())
    )

  def oneToOnePhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      logicalOpId: OperatorIdentity,
      opExecInitInfo: OpExecInitInfo
  ): PhysicalOp =
    oneToOnePhysicalOp(
      PhysicalOpIdentity(logicalOpId, "main"),
      workflowId,
      executionId,
      opExecInitInfo
    )

  def oneToOnePhysicalOp(
      physicalOpId: PhysicalOpIdentity,
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      opExecInitInfo: OpExecInitInfo
  ): PhysicalOp =
    PhysicalOp(physicalOpId, workflowId, executionId, opExecInitInfo = opExecInitInfo)

  def manyToOnePhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      logicalOpId: OperatorIdentity,
      opExecInitInfo: OpExecInitInfo
  ): PhysicalOp =
    manyToOnePhysicalOp(
      PhysicalOpIdentity(logicalOpId, "main"),
      workflowId,
      executionId,
      opExecInitInfo
    )

  def manyToOnePhysicalOp(
      physicalOpId: PhysicalOpIdentity,
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      opExecInitInfo: OpExecInitInfo
  ): PhysicalOp = {
    PhysicalOp(
      physicalOpId,
      workflowId,
      executionId,
      opExecInitInfo,
      parallelizable = false,
      partitionRequirement = List(Option(SinglePartition())),
      derivePartition = _ => SinglePartition()
    )
  }

  def localPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      logicalOpId: OperatorIdentity,
      opExecInitInfo: OpExecInitInfo
  ): PhysicalOp =
    localPhysicalOp(
      PhysicalOpIdentity(logicalOpId, "main"),
      workflowId,
      executionId,
      opExecInitInfo
    )

  def localPhysicalOp(
      physicalOpId: PhysicalOpIdentity,
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      opExecInitInfo: OpExecInitInfo
  ): PhysicalOp = {
    manyToOnePhysicalOp(physicalOpId, workflowId, executionId, opExecInitInfo)
      .withLocationPreference(Option(new PreferController()))
  }

  def hashPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      logicalOpId: OperatorIdentity,
      opExec: OpExecInitInfo,
      hashColumnIndices: List[Int]
  ): PhysicalOp =
    hashPhysicalOp(
      PhysicalOpIdentity(logicalOpId, "main"),
      workflowId,
      executionId,
      opExec,
      hashColumnIndices
    )

  def hashPhysicalOp(
      physicalOpId: PhysicalOpIdentity,
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      opExecInitInfo: OpExecInitInfo,
      hashColumnIndices: List[Int]
  ): PhysicalOp = {
    PhysicalOp(
      physicalOpId,
      workflowId,
      executionId,
      opExecInitInfo,
      partitionRequirement = List(Option(HashPartition(hashColumnIndices))),
      derivePartition = _ => HashPartition(hashColumnIndices)
    )
  }

}

case class PhysicalOp(
    // the identifier of this PhysicalOp
    id: PhysicalOpIdentity,
    // the workflow id number
    workflowId: WorkflowIdentity,
    // the execution id number
    executionId: ExecutionIdentity,
    // information regarding initializing an operator executor instance
    opExecInitInfo: OpExecInitInfo,
    // preference of parallelism
    parallelizable: Boolean = true,
    // preference of worker placement
    locationPreference: Option[LocationPreference] = None,
    // requirement of partition policy (hash/range/single/none) on inputs
    partitionRequirement: List[Option[PartitionInfo]] = List(),
    // derive the output partition info given the input partitions
    // if not specified, by default the output partition is the same as input partition
    derivePartition: List[PartitionInfo] => PartitionInfo = inputParts => inputParts.head,
    // input/output ports of the physical operator
    // for operators with multiple input/output ports: must set these variables properly
    inputPorts: Map[PortIdentity, (InputPort, List[PhysicalLink], Schema)] = Map.empty,
    outputPorts: Map[PortIdentity, (OutputPort, List[PhysicalLink], Schema)] = Map.empty,
    // input ports that are blocking
    blockingInputs: List[PortIdentity] = List(),
    isOneToManyOp: Boolean = false,
    // hint for number of workers
    suggestedWorkerNum: Option[Int] = None
) extends LazyLogging {

  // all the "dependee" links are also blocking inputs
  private lazy val realBlockingInputs: List[PortIdentity] =
    (blockingInputs ++ inputPorts.values.flatMap({
      case (port, _, _) => port.dependencies
    })).distinct

  private lazy val isInitWithCode: Boolean = opExecInitInfo.isInstanceOf[OpExecInitInfoWithCode]

  /**
    * Helper functions related to compile-time operations
    */

  def isSourceOperator: Boolean = {
    inputPorts.isEmpty
  }

  def isSinkOperator: Boolean = {
    outputPorts.isEmpty
  }

  def isPythonOperator: Boolean = {
    isInitWithCode // currently, only Python operators are initialized with code
  }

  def getPythonCode: String = {
    if (!isPythonOperator) {
      throw new RuntimeException("operator " + id + " is not a python operator")
    }
    opExecInitInfo.asInstanceOf[OpExecInitInfoWithCode].codeGen(0, this, OperatorConfig.empty)
  }

  /**
    * creates a copy with the location preference information
    */
  def withLocationPreference(preference: Option[LocationPreference]): PhysicalOp = {
    this.copy(locationPreference = preference)
  }

  /**
    * creates a copy with the input ports
    */
  def withInputPorts(
      inputs: List[InputPort],
      inputPortToSchemaMapping: mutable.Map[PortIdentity, Schema]
  ): PhysicalOp = {
    this.copy(inputPorts =
      inputs.map(input => input.id -> (input, List(), inputPortToSchemaMapping(input.id))).toMap
    )
  }

  /**
    * creates a copy with the output ports
    */
  def withOutputPorts(
      outputs: List[OutputPort],
      outputPortToSchemaMapping: mutable.Map[PortIdentity, Schema]
  ): PhysicalOp = {
    this.copy(outputPorts =
      outputs
        .map(output => output.id -> (output, List(), outputPortToSchemaMapping(output.id)))
        .toMap
    )
  }

  /**
    * creates a copy with suggested worker number. This is only to be used by Python UDF operators.
    */
  def withSuggestedWorkerNum(workerNum: Int): PhysicalOp = {
    this.copy(suggestedWorkerNum = Some(workerNum))
  }

  /**
    * creates a copy with the new id
    */
  def withId(id: PhysicalOpIdentity): PhysicalOp = this.copy(id = id)

  /**
    * creates a copy with the partition requirements
    */
  def withPartitionRequirement(partitionRequirements: List[Option[PartitionInfo]]): PhysicalOp = {
    this.copy(partitionRequirement = partitionRequirements)
  }

  /**
    * creates a copy with the partition info derive function
    */
  def withDerivePartition(derivePartition: List[PartitionInfo] => PartitionInfo): PhysicalOp = {
    this.copy(derivePartition = derivePartition)
  }

  /**
    * creates a copy with the parallelizable specified
    */
  def withParallelizable(parallelizable: Boolean): PhysicalOp =
    this.copy(parallelizable = parallelizable)

  /**
    * creates a copy with the specified property that whether this operator is one-to-many
    */
  def withIsOneToManyOp(isOneToManyOp: Boolean): PhysicalOp =
    this.copy(isOneToManyOp = isOneToManyOp)

  /**
    * creates a copy with the blocking input port indices
    */
  def withBlockingInputs(blockingInputs: List[PortIdentity]): PhysicalOp = {
    this.copy(blockingInputs = blockingInputs)
  }

  /**
    * creates a copy with an additional input link specified on an input port
    */
  def addInputLink(link: PhysicalLink): PhysicalOp = {
    assert(link.toOpId == id)
    assert(inputPorts.contains(link.toPortId))
    val (port, existingLinks, schema) = inputPorts(link.toPortId)
    val newLinks = existingLinks :+ link
    this.copy(
      inputPorts = inputPorts + (link.toPortId -> (port, newLinks, schema))
    )
  }

  /**
    * creates a copy with an additional output link specified on an output port
    */
  def addOutputLink(link: PhysicalLink): PhysicalOp = {
    assert(link.fromOpId == id)
    assert(outputPorts.contains(link.fromPortId))
    val (port, existingLinks, schema) = outputPorts(link.fromPortId)
    val newLinks = existingLinks :+ link
    this.copy(
      outputPorts = outputPorts + (link.fromPortId -> (port, newLinks, schema))
    )
  }

  /**
    * creates a copy with a removed input link
    */
  def removeInputLink(linkToRemove: PhysicalLink): PhysicalOp = {
    val portId = linkToRemove.toPortId
    val (port, existingLinks, schema) = inputPorts(portId)
    this.copy(
      inputPorts =
        inputPorts + (portId -> (port, existingLinks.filter(link => link != linkToRemove), schema))
    )
  }

  /**
    * creates a copy with a removed output link
    */
  def removeOutputLink(linkToRemove: PhysicalLink): PhysicalOp = {
    val portId = linkToRemove.fromPortId
    val (port, existingLinks, schema) = outputPorts(portId)
    this.copy(
      outputPorts =
        outputPorts + (portId -> (port, existingLinks.filter(link => link != linkToRemove), schema))
    )
  }

  /**
    * returns all output links. Optionally, if a specific portId is provided, returns the links connected to that portId.
    */
  def getOutputLinks(portIdOpt: Option[PortIdentity] = None): List[PhysicalLink] = {
    outputPorts.values
      .flatMap(_._2)
      .toList
      .filter(link =>
        portIdOpt match {
          case Some(portId) => link.fromPortId == portId
          case None         => true
        }
      )
  }

  /**
    * returns all input links. Optionally, if a specific portId is provided, returns the links connected to that portId.
    */
  def getInputLinks(portIdOpt: Option[PortIdentity] = None): List[PhysicalLink] = {
    inputPorts.values
      .flatMap(_._2)
      .toList
      .filter(link =>
        portIdOpt match {
          case Some(portId) => link.toPortId == portId
          case None         => true
        }
      )
  }

  /**
    * Tells whether the input on this link is blocking i.e. the operator doesn't output anything till this link
    * outputs all its tuples
    */
  def isInputLinkBlocking(link: PhysicalLink): Boolean = {
    val blockingLinks = realBlockingInputs.flatMap(portId => getInputLinks(Some(portId)))
    blockingLinks.contains(link)
  }

  /**
    * Some operators process their inputs in a particular order. Eg: 2 phase hash join first
    * processes the build input, then the probe input.
    */
  def getInputLinksInProcessingOrder: List[PhysicalLink] = {
    val dependencyDag = {
      new DirectedAcyclicGraph[PhysicalLink, DefaultEdge](classOf[DefaultEdge])
    }
    inputPorts.values
      .map(_._1)
      .flatMap(port => port.dependencies.map(dependee => port.id -> dependee))
      .foreach({
        case (depender: PortIdentity, dependee: PortIdentity) =>
          val upstreamLink = getInputLinks(Some(dependee)).head
          val downstreamLink = getInputLinks(Some(depender)).head
          if (!dependencyDag.containsVertex(upstreamLink)) {
            dependencyDag.addVertex(upstreamLink)
          }
          if (!dependencyDag.containsVertex(downstreamLink)) {
            dependencyDag.addVertex(downstreamLink)
          }
          dependencyDag.addEdge(upstreamLink, downstreamLink)
      })
    val topologicalIterator =
      new TopologicalOrderIterator[PhysicalLink, DefaultEdge](dependencyDag)
    val processingOrder = new ArrayBuffer[PhysicalLink]()
    while (topologicalIterator.hasNext) {
      processingOrder.append(topologicalIterator.next())
    }
    processingOrder.toList
  }

  def build(
      controllerActorService: AkkaActorService,
      operatorExecution: OperatorExecution,
      operatorConfig: OperatorConfig,
      stateRestoreConfig: Option[StateRestoreConfig],
      replayLoggingConfig: Option[FaultToleranceConfig]
  ): Unit = {
    val addressInfo = AddressInfo(
      controllerActorService.getClusterNodeAddresses,
      controllerActorService.self.path.address
    )

    operatorConfig.workerConfigs.foreach(workerConfig => {
      val workerId = workerConfig.workerId
      val workerIndex = VirtualIdentityUtils.getWorkerIndex(workerId)
      val locationPreference = this.locationPreference.getOrElse(new RoundRobinPreference())
      val preferredAddress = locationPreference.getPreferredLocation(addressInfo, this, workerIndex)

      val workflowWorker = if (this.isPythonOperator) {
        PythonWorkflowWorker.props(workerConfig)
      } else {
        WorkflowWorker.props(
          workerConfig,
          physicalOp = this,
          operatorConfig,
          WorkerReplayInitialization(
            stateRestoreConfig,
            replayLoggingConfig
          )
        )
      }
      // Note: At this point, we don't know if the actor is fully initialized.
      // Thus, the ActorRef returned from `controllerActorService.actorOf` is ignored.
      controllerActorService.actorOf(
        workflowWorker.withDeploy(Deploy(scope = RemoteScope(preferredAddress)))
      )
      operatorExecution.initWorkerExecution(workerId)
    })
  }
}
