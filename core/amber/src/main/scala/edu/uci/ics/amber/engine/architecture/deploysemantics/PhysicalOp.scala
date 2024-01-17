package edu.uci.ics.amber.engine.architecture.deploysemantics

import akka.actor.Deploy
import akka.remote.RemoteScope
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.common.AkkaActorService
import edu.uci.ics.amber.engine.architecture.controller.OperatorExecution
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{
  OpExecInitInfo,
  OpExecInitInfoWithCode,
  OpExecInitInfoWithFunc
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
  WorkerReplayLoggingConfig,
  WorkerStateRestoreConfig
}
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils
import edu.uci.ics.amber.engine.common.virtualidentity._
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.common.workflow.{HashPartition, PartitionInfo, SinglePartition}
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpExec
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}
import org.jgrapht.traverse.TopologicalOrderIterator

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
      locationPreference = Option(new PreferController()),
      inputPorts = List.empty
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
    // input/output schemas
    schemaInfo: Option[OperatorSchemaInfo] = None,
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
    inputPortToLinkIdMapping: Map[Int, List[PhysicalLinkIdentity]] = Map(),
    outputPortToLinkIdMapping: Map[Int, List[PhysicalLinkIdentity]] = Map(),
    // input ports that are blocking
    blockingInputs: List[Int] = List(),
    // execution dependency of ports: (depender -> dependee), where dependee needs to finish first.
    dependencies: Map[Int, Int] = Map(),
    isOneToManyOp: Boolean = false,
    // hint for number of workers
    suggestedWorkerNum: Option[Int] = None
) extends LazyLogging {

  // all the "dependee" links are also blocking inputs
  private lazy val realBlockingInputs: List[Int] = (blockingInputs ++ dependencies.values).distinct

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

  def isHashJoinOperator: Boolean = {
    opExecInitInfo match {
      case OpExecInitInfoWithCode(codeGen) => false
      case OpExecInitInfoWithFunc(opGen) =>
        opGen(0, this, OperatorConfig.empty).isInstanceOf[HashJoinOpExec[_]]
    }
  }

  def getPythonCode: String = {
    if (!isPythonOperator) {
      throw new RuntimeException("operator " + id + " is not a python operator")
    }
    opExecInitInfo.asInstanceOf[OpExecInitInfoWithCode].codeGen(0, this, OperatorConfig.empty)
  }

  def getOutputSchema: Schema = {
    if (!isPythonOperator) {
      throw new RuntimeException("operator " + id + " is not a python operator")
    }
    schemaInfo.get.outputSchemas.head
  }

  /**
    * creates a copy with the specified port information
    */
  def withPorts(operatorInfo: OperatorInfo): PhysicalOp = {
    this.copy(inputPorts = operatorInfo.inputPorts, outputPorts = operatorInfo.outputPorts)
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
  def withInputPorts(inputs: List[InputPort]): PhysicalOp = {
    this.copy(inputPorts = inputs)
  }

  /**
    * creates a copy with the output ports
    */
  def withOutputPorts(outputs: List[OutputPort]): PhysicalOp = {
    this.copy(outputPorts = outputs)
  }

  /**
    * creates a copy with the blocking input port indices
    */
  def withBlockingInputs(blockingInputs: List[Int]): PhysicalOp = {
    this.copy(blockingInputs = blockingInputs)
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
    * creates a copy with the dependencies specified
    */
  def withDependencies(dependencies: Map[Int, Int]): PhysicalOp =
    this.copy(dependencies = dependencies)

  /**
    * creates a copy with the specified property that whether this operator is one-to-many
    */
  def withIsOneToManyOp(isOneToManyOp: Boolean): PhysicalOp =
    this.copy(isOneToManyOp = isOneToManyOp)

  /**
    * creates a copy with the schema information
    */
  def withOperatorSchemaInfo(schemaInfo: OperatorSchemaInfo): PhysicalOp =
    this.copy(schemaInfo = Some(schemaInfo))

  /**
    * creates a copy with an additional input operator specified on an input port
    */
  def addInput(fromOp: PhysicalOp, fromPort: Int, toPort: Int): PhysicalOp = {
    val link = PhysicalLink(fromOp, fromPort, this, toPort)
    addInput(link)
  }

  /**
    * creates a copy with an additional input operator specified on an input port
    */
  def addInput(link: PhysicalLink): PhysicalOp = {
    assert(link.toOp.id == id)
    val existingLinks = inputPortToLinkIdMapping.getOrElse(link.toPort, List())
    val newLinks = existingLinks :+ link.id
    this.copy(
      inputPortToLinkIdMapping = inputPortToLinkIdMapping + (link.toPort -> newLinks)
    )
  }

  /**
    * creates a copy with an additional output operator specified on an output port
    */
  def addOutput(toOp: PhysicalOp, fromPort: Int, toPort: Int): PhysicalOp = {
    val link = PhysicalLink(this, fromPort, toOp, toPort)
    addOutput(link)
  }

  /**
    * creates a copy with an additional output operator specified on an output port
    */
  def addOutput(link: PhysicalLink): PhysicalOp = {
    assert(link.fromOp.id == id)
    val existingLinks = outputPortToLinkIdMapping.getOrElse(link.fromPort, List())
    val newLinks = existingLinks :+ link.id
    this.copy(
      outputPortToLinkIdMapping = outputPortToLinkIdMapping + (link.fromPort -> newLinks)
    )
  }

  /**
    * creates a copy with a removed input operator, we use the identity to do equality check.
    */
  def removeInput(linkToRemove: PhysicalLink): PhysicalOp = {
    val (portIdx, existingLinks) = inputPortToLinkIdMapping
      .find({
        case (_, links) => links.contains(linkToRemove.id)
      })
      .getOrElse(throw new IllegalArgumentException(s"unexpected link to remove: $linkToRemove"))
    this.copy(
      inputPortToLinkIdMapping =
        inputPortToLinkIdMapping + (portIdx -> existingLinks.filter(linkId =>
          linkId != linkToRemove.id
        ))
    )
  }

  /**
    * creates a copy with a removed output operator, we use the identity to do equality check.
    */
  def removeOutput(linkToRemove: PhysicalLink): PhysicalOp = {
    val (portIdx, existingLinks) = outputPortToLinkIdMapping
      .find({
        case (_, links) => links.contains(linkToRemove.id)
      })
      .getOrElse(throw new IllegalArgumentException(s"unexpected link to remove: $linkToRemove"))
    this.copy(
      outputPortToLinkIdMapping =
        outputPortToLinkIdMapping + (portIdx -> existingLinks.filter(linkId =>
          linkId != linkToRemove.id
        ))
    )
  }

  /**
    * returns all input links on a specific input port
    */
  def getLinksOnInputPort(portIndex: Int): List[PhysicalLinkIdentity] = {
    inputPortToLinkIdMapping(portIndex)
  }

  /**
    * returns all the input operators on a specific input port
    */
  def getOpsOnInputPort(portIndex: Int): List[PhysicalOpIdentity] = {
    getLinksOnInputPort(portIndex).map(linkId => linkId.from)
  }

  /**
    * returns all output links on a specific output port
    */
  def getLinksOnOutputPort(portIndex: Int): List[PhysicalLinkIdentity] = {
    outputPortToLinkIdMapping(portIndex)
  }

  /**
    * Tells whether the input on this link is blocking i.e. the operator doesn't output anything till this link
    * outputs all its tuples
    */
  def isInputLinkBlocking(linkId: PhysicalLinkIdentity): Boolean = {
    val blockingLinkIds = realBlockingInputs.flatMap(portIdx => inputPortToLinkIdMapping(portIdx))
    blockingLinkIds.contains(linkId)
  }

  def getAllInputLinkIds: List[PhysicalLinkIdentity] = {
    inputPortToLinkIdMapping.values.flatten.toList
  }

  def getAllOutputLinkIds: List[PhysicalLinkIdentity] = {
    outputPortToLinkIdMapping.values.flatten.toList
  }

  def getPortIdxForInputLinkId(linkId: PhysicalLinkIdentity): Int = {
    inputPortToLinkIdMapping
      .find {
        case (_, links) => links.contains(linkId)
      }
      .map(_._1)
      .get
  }

  def getPortIdxForOutputLinkId(linkId: PhysicalLinkIdentity): Int = {
    outputPortToLinkIdMapping
      .find {
        case (_, links) => links.contains(linkId)
      }
      .map(_._1)
      .get
  }

  /**
    * Some operators process their inputs in a particular order. Eg: 2 phase hash join first
    * processes the build input, then the probe input.
    */
  def getInputLinksInProcessingOrder: List[PhysicalLinkIdentity] = {
    val dependencyDag =
      new DirectedAcyclicGraph[PhysicalLinkIdentity, DefaultEdge](classOf[DefaultEdge])
    dependencies.foreach({
      case (depender: Int, dependee: Int) =>
        val upstreamLink = inputPortToLinkIdMapping(dependee).head
        val downstreamLink = inputPortToLinkIdMapping(depender).head
        if (!dependencyDag.containsVertex(upstreamLink)) {
          dependencyDag.addVertex(upstreamLink)
        }
        if (!dependencyDag.containsVertex(downstreamLink)) {
          dependencyDag.addVertex(downstreamLink)
        }
        dependencyDag.addEdge(upstreamLink, downstreamLink)
    })
    val topologicalIterator =
      new TopologicalOrderIterator[PhysicalLinkIdentity, DefaultEdge](dependencyDag)
    val processingOrder = new ArrayBuffer[PhysicalLinkIdentity]()
    while (topologicalIterator.hasNext) {
      processingOrder.append(topologicalIterator.next())
    }
    processingOrder.toList
  }

  def build(
      controllerActorService: AkkaActorService,
      opExecution: OperatorExecution,
      operatorConfig: OperatorConfig,
      stateRestoreConfigGen: ActorVirtualIdentity => Option[WorkerStateRestoreConfig],
      replayLoggingConfigGen: ActorVirtualIdentity => Option[WorkerReplayLoggingConfig]
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
            stateRestoreConfigGen(workerId),
            replayLoggingConfigGen(workerId)
          )
        )
      }
      // Note: At this point, we don't know if the actor is fully initialized.
      // Thus, the ActorRef returned from `controllerActorService.actorOf` is ignored.
      controllerActorService.actorOf(
        workflowWorker.withDeploy(Deploy(scope = RemoteScope(preferredAddress)))
      )
      opExecution.initializeWorkerInfo(workerId)
    })
  }
}
