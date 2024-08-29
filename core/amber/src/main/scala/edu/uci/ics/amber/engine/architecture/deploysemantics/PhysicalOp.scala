package edu.uci.ics.amber.engine.architecture.deploysemantics

import akka.actor.Deploy
import akka.remote.RemoteScope
import com.fasterxml.jackson.annotation.JsonProperty
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.common.AkkaActorService
import edu.uci.ics.amber.engine.architecture.controller.execution.OperatorExecution
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{OpExecInitInfo, OpExecInitInfoWithCode}
import edu.uci.ics.amber.engine.architecture.deploysemantics.locationpreference.{AddressInfo, LocationPreference, PreferController, RoundRobinPreference}
import edu.uci.ics.amber.engine.architecture.pythonworker.PythonWorkflowWorker
import edu.uci.ics.amber.engine.architecture.scheduling.config.OperatorConfig
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.{FaultToleranceConfig, StateRestoreConfig, WorkerReplayInitialization}
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils
import edu.uci.ics.amber.engine.common.virtualidentity._
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort, PhysicalLink, PortIdentity}
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.common.workflow._
import net.minidev.json.annotate.JsonIgnore
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}
import org.jgrapht.traverse.TopologicalOrderIterator

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

case object SchemaPropagationFunc {
  private type JavaSchemaPropagationFunc =
    java.util.function.Function[Map[PortIdentity, Schema], Map[PortIdentity, Schema]]
      with java.io.Serializable
  def apply(javaFunc: JavaSchemaPropagationFunc): SchemaPropagationFunc =
    SchemaPropagationFunc(inputSchemas => javaFunc.apply(inputSchemas))

}

case class SchemaPropagationFunc(func: Map[PortIdentity, Schema] => Map[PortIdentity, Schema])

class SchemaNotAvailableException(message: String) extends Exception(message)

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
                      ): PhysicalOp = {
    new PhysicalOp(
      id = physicalOpId,
      workflowId = workflowId,
      executionId = executionId,
      opExecInitInfo = opExecInitInfo,
      parallelizable = false,
      locationPreference = Option(new PreferController())
    )
  }

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
                        ): PhysicalOp = {
    new PhysicalOp(
      id = physicalOpId,
      workflowId = workflowId,
      executionId = executionId,
      opExecInitInfo = opExecInitInfo
    )
  }

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
    new PhysicalOp(
      id = physicalOpId,
      workflowId = workflowId,
      executionId = executionId,
      opExecInitInfo = opExecInitInfo,
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
}

class PhysicalOp(
                  @JsonProperty("id") var id: PhysicalOpIdentity,
                  @JsonProperty("workflowId") var workflowId: WorkflowIdentity,
                  @JsonProperty("executionId") var executionId: ExecutionIdentity,
                  @JsonIgnore var opExecInitInfo: OpExecInitInfo,
                  @JsonProperty("parallelizable") var parallelizable: Boolean = true,
                  @JsonProperty("locationPreference") var locationPreference: Option[LocationPreference] = None,
                  @JsonProperty("partitionRequirement") var partitionRequirement: List[Option[PartitionInfo]] = List(),
                  @JsonIgnore var derivePartition: List[PartitionInfo] => PartitionInfo = inputParts => inputParts.head,
                  @JsonProperty("inputPorts") var inputPorts: Map[PortIdentity, (InputPort, List[PhysicalLink], Either[Throwable, Schema])] = Map.empty,
                  @JsonProperty("outputPorts") var outputPorts: Map[PortIdentity, (OutputPort, List[PhysicalLink], Either[Throwable, Schema])] = Map.empty,
                  @JsonIgnore var propagateSchema: SchemaPropagationFunc = SchemaPropagationFunc(schemas => schemas),
                  @JsonProperty("isOneToManyOp") var isOneToManyOp: Boolean = false,
                  @JsonProperty("suggestedWorkerNum") var suggestedWorkerNum: Option[Int] = None
                ) extends LazyLogging {

  // Auxiliary constructor to allow creation with fewer parameters
  def this(
            id: PhysicalOpIdentity,
            workflowId: WorkflowIdentity,
            executionId: ExecutionIdentity,
            opExecInitInfo: OpExecInitInfo
          ) = {
    this(
      id,
      workflowId,
      executionId,
      opExecInitInfo,
      parallelizable = true,
      locationPreference = None,
      partitionRequirement = List(),
      derivePartition = inputParts => inputParts.head,
      inputPorts = Map.empty,
      outputPorts = Map.empty,
      propagateSchema = SchemaPropagationFunc(schemas => schemas),
      isOneToManyOp = false,
      suggestedWorkerNum = None
    )
  }

  // Other methods and helper functions

  private lazy val dependeeInputs: List[PortIdentity] =
    inputPorts.values
      .flatMap {
        case (port, _, _) => port.dependencies
      }
      .toList
      .distinct

  private lazy val isInitWithCode: Boolean = opExecInitInfo.isInstanceOf[OpExecInitInfoWithCode]

  def isSourceOperator: Boolean = inputPorts.isEmpty

  def isSinkOperator: Boolean = outputPorts.forall(port => port._2._2.isEmpty)

  def isPythonBased: Boolean = {
    opExecInitInfo match {
      case opExecInfo: OpExecInitInfoWithCode =>
        val (_, language) = opExecInfo.codeGen(0, 0)
        language == "python" || language == "r-tuple" || language == "r-table"
      case _ => false
    }
  }

  def getPythonCode: String = {
    val (code, _) = opExecInitInfo.asInstanceOf[OpExecInitInfoWithCode].codeGen(0, 0)
    code
  }

  def withLocationPreference(preference: Option[LocationPreference]): PhysicalOp = {
    new PhysicalOp(
      id,
      workflowId,
      executionId,
      opExecInitInfo,
      parallelizable,
      preference,
      partitionRequirement,
      derivePartition,
      inputPorts,
      outputPorts,
      propagateSchema,
      isOneToManyOp,
      suggestedWorkerNum
    )
  }

  def withInputPorts(inputs: List[InputPort]): PhysicalOp = {
    val newInputPorts = inputs.map(input =>
      input.id -> (input, List.empty[PhysicalLink], Left(new SchemaNotAvailableException("schema is not available")))
    ).toMap
    new PhysicalOp(
      id,
      workflowId,
      executionId,
      opExecInitInfo,
      parallelizable,
      locationPreference,
      partitionRequirement,
      derivePartition,
      newInputPorts,
      outputPorts,
      propagateSchema,
      isOneToManyOp,
      suggestedWorkerNum
    )
  }

  def withOutputPorts(outputs: List[OutputPort]): PhysicalOp = {
    val newOutputPorts = outputs.map(output =>
      output.id -> (output, List.empty[PhysicalLink], Left(new SchemaNotAvailableException("schema is not available")))
    ).toMap
    new PhysicalOp(
      id,
      workflowId,
      executionId,
      opExecInitInfo,
      parallelizable,
      locationPreference,
      partitionRequirement,
      derivePartition,
      inputPorts,
      newOutputPorts,
      propagateSchema,
      isOneToManyOp,
      suggestedWorkerNum
    )
  }

  def withSuggestedWorkerNum(workerNum: Int): PhysicalOp = {
    new PhysicalOp(
      id,
      workflowId,
      executionId,
      opExecInitInfo,
      parallelizable,
      locationPreference,
      partitionRequirement,
      derivePartition,
      inputPorts,
      outputPorts,
      propagateSchema,
      isOneToManyOp,
      Some(workerNum)
    )
  }

  def withPartitionRequirement(partitionRequirements: List[Option[PartitionInfo]]): PhysicalOp = {
    new PhysicalOp(
      id,
      workflowId,
      executionId,
      opExecInitInfo,
      parallelizable,
      locationPreference,
      partitionRequirements,
      derivePartition,
      inputPorts,
      outputPorts,
      propagateSchema,
      isOneToManyOp,
      suggestedWorkerNum
    )
  }

  def withDerivePartition(derivePartition: List[PartitionInfo] => PartitionInfo): PhysicalOp = {
    new PhysicalOp(
      id,
      workflowId,
      executionId,
      opExecInitInfo,
      parallelizable,
      locationPreference,
      partitionRequirement,
      derivePartition,
      inputPorts,
      outputPorts,
      propagateSchema,
      isOneToManyOp,
      suggestedWorkerNum
    )
  }

  def withParallelizable(parallelizable: Boolean): PhysicalOp = {
    new PhysicalOp(
      id,
      workflowId,
      executionId,
      opExecInitInfo,
      parallelizable,
      locationPreference,
      partitionRequirement,
      derivePartition,
      inputPorts,
      outputPorts,
      propagateSchema,
      isOneToManyOp,
      suggestedWorkerNum
    )
  }

  def withIsOneToManyOp(isOneToManyOp: Boolean): PhysicalOp = {
    new PhysicalOp(
      id,
      workflowId,
      executionId,
      opExecInitInfo,
      parallelizable,
      locationPreference,
      partitionRequirement,
      derivePartition,
      inputPorts,
      outputPorts,
      propagateSchema,
      isOneToManyOp,
      suggestedWorkerNum
    )
  }

  private def withInputSchema(
                               portId: PortIdentity,
                               schema: Either[Throwable, Schema]
                             ): PhysicalOp = {
    new PhysicalOp(
      id,
      workflowId,
      executionId,
      opExecInitInfo,
      parallelizable,
      locationPreference,
      partitionRequirement,
      derivePartition,
      inputPorts.updated(portId, inputPorts(portId).copy(_3 = schema)),
      outputPorts,
      propagateSchema,
      isOneToManyOp,
      suggestedWorkerNum
    )
  }

  private def withOutputSchema(
                                portId: PortIdentity,
                                schema: Either[Throwable, Schema]
                              ): PhysicalOp = {
    new PhysicalOp(
      id,
      workflowId,
      executionId,
      opExecInitInfo,
      parallelizable,
      locationPreference,
      partitionRequirement,
      derivePartition,
      inputPorts,
      outputPorts.updated(portId, outputPorts(portId).copy(_3 = schema)),
      propagateSchema,
      isOneToManyOp,
      suggestedWorkerNum
    )
  }

  def withPropagateSchema(func: SchemaPropagationFunc): PhysicalOp = {
    new PhysicalOp(
      id,
      workflowId,
      executionId,
      opExecInitInfo,
      parallelizable,
      locationPreference,
      partitionRequirement,
      derivePartition,
      inputPorts,
      outputPorts,
      func,
      isOneToManyOp,
      suggestedWorkerNum
    )
  }

  def addInputLink(link: PhysicalLink): PhysicalOp = {
    assert(link.toOpId == id)
    assert(inputPorts.contains(link.toPortId))
    val (port, existingLinks, schema) = inputPorts(link.toPortId)
    val newLinks = existingLinks :+ link
    new PhysicalOp(
      id,
      workflowId,
      executionId,
      opExecInitInfo,
      parallelizable,
      locationPreference,
      partitionRequirement,
      derivePartition,
      inputPorts + (link.toPortId -> (port, newLinks, schema)),
      outputPorts,
      propagateSchema,
      isOneToManyOp,
      suggestedWorkerNum
    )
  }

  def addOutputLink(link: PhysicalLink): PhysicalOp = {
    assert(link.fromOpId == id)
    assert(outputPorts.contains(link.fromPortId))
    val (port, existingLinks, schema) = outputPorts(link.fromPortId)
    val newLinks = existingLinks :+ link
    new PhysicalOp(
      id,
      workflowId,
      executionId,
      opExecInitInfo,
      parallelizable,
      locationPreference,
      partitionRequirement,
      derivePartition,
      inputPorts,
      outputPorts + (link.fromPortId -> (port, newLinks, schema)),
      propagateSchema,
      isOneToManyOp,
      suggestedWorkerNum
    )
  }

  def removeInputLink(linkToRemove: PhysicalLink): PhysicalOp = {
    val portId = linkToRemove.toPortId
    val (port, existingLinks, schema) = inputPorts(portId)
    new PhysicalOp(
      id,
      workflowId,
      executionId,
      opExecInitInfo,
      parallelizable,
      locationPreference,
      partitionRequirement,
      derivePartition,
      inputPorts + (portId -> (port, existingLinks.filter(link => link != linkToRemove), schema)),
      outputPorts,
      propagateSchema,
      isOneToManyOp,
      suggestedWorkerNum
    )
  }

  def removeOutputLink(linkToRemove: PhysicalLink): PhysicalOp = {
    val portId = linkToRemove.fromPortId
    val (port, existingLinks, schema) = outputPorts(portId)
    new PhysicalOp(
      id,
      workflowId,
      executionId,
      opExecInitInfo,
      parallelizable,
      locationPreference,
      partitionRequirement,
      derivePartition,
      inputPorts,
      outputPorts + (portId -> (port, existingLinks.filter(link => link != linkToRemove), schema)),
      propagateSchema,
      isOneToManyOp,
      suggestedWorkerNum
    )
  }

  def propagateSchema(newInputSchema: Option[(PortIdentity, Schema)] = None): PhysicalOp = {
    val updatedOp = newInputSchema.foldLeft(this) {
      case (op, (portId, schema)) => op.withInputSchema(portId, Right(schema))
    }

    val inputSchemas = updatedOp.inputPorts.collect {
      case (portId, (_, _, Right(schema))) => portId -> schema
    }

    if (updatedOp.inputPorts.size == inputSchemas.size) {
      val schemaPropagationResult = Try(propagateSchema.func(inputSchemas))
      schemaPropagationResult match {
        case Success(schemaMapping) =>
          schemaMapping.foldLeft(updatedOp) {
            case (op, (portId, schema)) =>
              op.withOutputSchema(portId, Right(schema))
          }
        case Failure(exception) =>
          updatedOp.outputPorts.keys.foldLeft(updatedOp) { (op, portId) =>
            op.withOutputSchema(portId, Left(exception))
          }
      }
    } else {
      updatedOp
    }
  }

  def getOutputLinks(portId: PortIdentity): List[PhysicalLink] = {
    outputPorts.values
      .flatMap(_._2)
      .filter(link => link.fromPortId == portId)
      .toList
  }

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

  def isInputLinkDependee(link: PhysicalLink): Boolean = {
    dependeeInputs.contains(link.toPortId)
  }

  def isOutputLinkBlocking(link: PhysicalLink): Boolean = {
    this.outputPorts(link.fromPortId)._1.blocking
  }

  def getInputLinksInProcessingOrder: List[PhysicalLink] = {
    val dependencyDag = new DirectedAcyclicGraph[PhysicalLink, DefaultEdge](classOf[DefaultEdge])
    inputPorts.values
      .map(_._1)
      .flatMap(port => port.dependencies.map(dependee => port.id -> dependee))
      .foreach {
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
      }
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

      val workflowWorker = if (this.isPythonBased) {
        PythonWorkflowWorker.props(workerConfig)
      } else {
        WorkflowWorker.props(
          workerConfig,
          WorkerReplayInitialization(
            stateRestoreConfig,
            replayLoggingConfig
          )
        )
      }
      controllerActorService.actorOf(
        workflowWorker.withDeploy(Deploy(scope = RemoteScope(preferredAddress)))
      )
      operatorExecution.initWorkerExecution(workerId)
    })
  }
}
