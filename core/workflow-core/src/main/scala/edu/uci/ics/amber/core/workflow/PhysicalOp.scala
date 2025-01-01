package edu.uci.ics.amber.core.workflow

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties}
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.executor.{OpExecWithCode, OpExecInitInfo}
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
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
  ): PhysicalOp =
    PhysicalOp(
      physicalOpId,
      workflowId,
      executionId,
      opExecInitInfo,
      parallelizable = false,
      locationPreference = Some(PreferController)
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
    PhysicalOp(physicalOpId, workflowId, executionId, opExecInitInfo)

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
      .withLocationPreference(Some(PreferController))
  }
}

// @JsonIgnore is not working when directly annotated to fields of a case class
// https://stackoverflow.com/questions/40482904/jsonignore-doesnt-work-in-scala-case-class
@JsonIgnoreProperties(
  Array(
    "opExecInitInfo", // function type, ignore it
    "derivePartition", // function type, ignore it
    "inputPorts", // may contain very long stacktrace, ignore it
    "outputPorts", // same reason with above
    "propagateSchema", // function type, so ignore it
    "locationPreference", // ignore it for the deserialization
    "partitionRequirement" // ignore it for deserialization
  )
)
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
    inputPorts: Map[PortIdentity, (InputPort, List[PhysicalLink], Either[Throwable, Schema])] =
      Map.empty,
    outputPorts: Map[PortIdentity, (OutputPort, List[PhysicalLink], Either[Throwable, Schema])] =
      Map.empty,
    // schema propagation function
    propagateSchema: SchemaPropagationFunc = SchemaPropagationFunc(schemas => schemas),
    isOneToManyOp: Boolean = false,
    // hint for number of workers
    suggestedWorkerNum: Option[Int] = None
) extends LazyLogging {

  // all the "dependee" links are also blocking
  private lazy val dependeeInputs: List[PortIdentity] =
    inputPorts.values
      .flatMap({
        case (port, _, _) => port.dependencies
      })
      .toList
      .distinct

  /**
    * Helper functions related to compile-time operations
    */
  @JsonIgnore
  def isSourceOperator: Boolean = {
    inputPorts.isEmpty
  }

  /**
    * Helper function used to determine whether the input link is a materialized link.
    */
  @JsonIgnore
  def isSinkOperator: Boolean = {
    outputPorts.forall(port => port._2._2.isEmpty)
  }

  @JsonIgnore // this is needed to prevent the serialization issue
  def isPythonBased: Boolean = {
    opExecInitInfo match {
      case OpExecWithCode(_, language) =>
        language == "python" || language == "r-tuple" || language == "r-table"
      case _ => false
    }
  }

  /**
    * creates a copy with the location preference information
    */
  def withLocationPreference(preference: Option[LocationPreference]): PhysicalOp = {
    this.copy(locationPreference = preference)
  }

  /**
    * Creates a copy of the PhysicalOp with the specified input ports. Each input port is associated
    * with an empty list of links and a None schema, reflecting the absence of predefined connections
    * and schema information.
    *
    * @param inputs A list of InputPort instances to set as the new input ports.
    * @return A new instance of PhysicalOp with the input ports updated.
    */
  def withInputPorts(inputs: List[InputPort]): PhysicalOp = {
    this.copy(inputPorts =
      inputs
        .map(input =>
          input.id -> (input, List
            .empty[PhysicalLink], Left(new SchemaNotAvailableException("schema is not available")))
        )
        .toMap
    )
  }

  /**
    * Creates a copy of the PhysicalOp with the specified output ports. Each output port is
    * initialized with an empty list of links and a None schema, indicating
    * the absence of outbound connections and schema details at this stage.
    *
    * @param outputs A list of OutputPort instances to set as the new output ports.
    * @return A new instance of PhysicalOp with the output ports updated.
    */
  def withOutputPorts(outputs: List[OutputPort]): PhysicalOp = {
    this.copy(outputPorts =
      outputs
        .map(output =>
          output.id -> (output, List
            .empty[PhysicalLink], Left(new SchemaNotAvailableException("schema is not available")))
        )
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
    * Creates a copy of the PhysicalOp with the schema of a specified input port updated.
    * The schema can either be a successful schema definition or an error represented as a Throwable.
    *
    * @param portId The identity of the port to update.
    * @param schema The new schema, or error, to be associated with the port, encapsulated within an Either.
    *               A Right value represents a successful schema, while a Left value represents an error (Throwable).
    * @return A new instance of PhysicalOp with the updated input port schema or error information.
    */
  private def withInputSchema(
      portId: PortIdentity,
      schema: Either[Throwable, Schema]
  ): PhysicalOp = {
    this.copy(inputPorts = inputPorts.updatedWith(portId) {
      case Some((port, links, _)) => Some((port, links, schema))
      case None                   => None
    })
  }

  /**
    * Creates a copy of the PhysicalOp with the schema of a specified output port updated.
    * Similar to `withInputSchema`, the schema can either represent a successful schema definition
    * or an error, encapsulated as an Either type.
    *
    * @param portId The identity of the port to update.
    * @param schema The new schema, or error, to be associated with the port, encapsulated within an Either.
    *               A Right value indicates a successful schema, while a Left value indicates an error (Throwable).
    * @return A new instance of PhysicalOp with the updated output port schema or error information.
    */
  private def withOutputSchema(
      portId: PortIdentity,
      schema: Either[Throwable, Schema]
  ): PhysicalOp = {
    this.copy(outputPorts = outputPorts.updatedWith(portId) {
      case Some((port, links, _)) => Some((port, links, schema))
      case None                   => None
    })
  }

  /**
    * creates a copy with the schema propagation function.
    */
  def withPropagateSchema(func: SchemaPropagationFunc): PhysicalOp = {
    this.copy(propagateSchema = func)
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
    * creates a copy with an input schema updated, and if all input schemas are available, propagate
    * the schema change to output schemas.
    * @param newInputSchema optionally provide a schema for an input port.
    */
  def propagateSchema(newInputSchema: Option[(PortIdentity, Schema)] = None): PhysicalOp = {
    // Update the input schema if a new one is provided
    val updatedOp = newInputSchema.foldLeft(this) { (op, schemaEntry) =>
      val (portId, schema) = schemaEntry
      op.inputPorts(portId)._3 match {
        case Left(_) =>
          op.withInputSchema(portId, Right(schema))
        case Right(existingSchema) if existingSchema != schema =>
          throw new IllegalArgumentException(
            s"Conflict schemas received on port ${portId.id}, $existingSchema != $schema"
          )
        case _ =>
          op
      }
    }

    // Extract input schemas, checking if all are defined
    val inputSchemas = updatedOp.inputPorts.collect {
      case (portId, (_, _, Right(schema))) => portId -> schema
    }

    if (updatedOp.inputPorts.size == inputSchemas.size) {
      // All input schemas are available, propagate to output schema
      val schemaPropagationResult = Try(propagateSchema.func(inputSchemas))
      schemaPropagationResult match {
        case Success(schemaMapping) =>
          schemaMapping.foldLeft(updatedOp) {
            case (op, (portId, schema)) =>
              op.withOutputSchema(portId, Right(schema))
          }
        case Failure(exception) =>
          // apply the exception to all output ports in case of failure
          updatedOp.outputPorts.keys.foldLeft(updatedOp) { (op, portId) =>
            op.withOutputSchema(portId, Left(exception))
          }
      }
    } else {
      // Not all input schemas are defined, return the updated operation without changes
      updatedOp
    }
  }

  /**
    * returns all output links. Optionally, if a specific portId is provided, returns the links connected to that portId.
    */
  def getOutputLinks(portId: PortIdentity): List[PhysicalLink] = {
    outputPorts.values
      .flatMap(_._2)
      .filter(link => link.fromPortId == portId)
      .toList
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
    * Tells whether the input port the link connects to is depended by another input .
    */
  def isInputLinkDependee(link: PhysicalLink): Boolean = {
    dependeeInputs.contains(link.toPortId)
  }

  /**
    * Tells whether the output on this link is blocking i.e. the operator doesn't output anything till this link
    * outputs all its tuples.
    */
  def isOutputLinkBlocking(link: PhysicalLink): Boolean = {
    this.outputPorts(link.fromPortId)._1.blocking
  }

  /**
    * Some operators process their inputs in a particular order. Eg: 2 phase hash join first
    * processes the build input, then the probe input.
    */
  @JsonIgnore
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
}
