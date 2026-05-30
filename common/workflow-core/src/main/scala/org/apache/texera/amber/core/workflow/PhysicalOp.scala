/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.core.workflow

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.amber.util.serde.{PhysicalOpDeserializer, PhysicalOpSerializer}
import org.apache.texera.amber.core.executor.{OpExecInitInfo, OpExecWithCode}
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity.{
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
      partitionDeriveSpec = ToSingle()
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

  /**
    * Rebuilds a [[PhysicalOp]] from the data produced by the serializable
    * `inputPortsSerialized` / `outputPortsSerialized` views.
    *
    * The runtime `inputPorts` / `outputPorts` maps carry per-port link lists and an
    * `Either[Throwable, Schema]` that are not directly serializable, so they are emitted
    * as slimmed-down views (dropping links, mapping the `Either` to an `Option[Schema]`).
    * Here the real maps are rebuilt with EMPTY link lists; the per-port link lists are
    * then rehydrated at the [[PhysicalPlan]] level by replaying `links`.
    *
    * This is invoked by the custom `PhysicalOpDeserializer` (registered on
    * `JSONUtils.objectMapper`) rather than via a `@JsonCreator`, because
    * jackson-module-scala binds case classes to their primary constructor and does not
    * reliably honor a companion-object creator.
    */
  def fromSerialized(
      id: PhysicalOpIdentity,
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      opExecInitInfo: OpExecInitInfo,
      parallelizable: Boolean,
      locationPreference: Option[LocationPreference],
      partitionRequirement: List[Option[PartitionInfo]],
      partitionDeriveSpec: DerivePartitionSpec,
      inputPortsSerialized: Map[PortIdentity, (InputPort, Option[Schema])],
      outputPortsSerialized: Map[PortIdentity, (OutputPort, Option[Schema])],
      isOneToManyOp: Boolean,
      suggestedWorkerNum: Option[Int],
      pveName: String
  ): PhysicalOp = {
    def schemaEither(schemaOpt: Option[Schema]): Either[Throwable, Schema] =
      schemaOpt match {
        case Some(schema) => Right(schema)
        case None         => Left(new SchemaNotAvailableException("schema is not available"))
      }

    val rebuiltInputPorts = inputPortsSerialized.map {
      case (portId, (port, schemaOpt)) =>
        portId -> ((port, List.empty[PhysicalLink], schemaEither(schemaOpt)))
    }
    val rebuiltOutputPorts = outputPortsSerialized.map {
      case (portId, (port, schemaOpt)) =>
        portId -> ((port, List.empty[PhysicalLink], schemaEither(schemaOpt)))
    }

    PhysicalOp(
      id = id,
      workflowId = workflowId,
      executionId = executionId,
      opExecInitInfo = opExecInitInfo,
      parallelizable = parallelizable,
      locationPreference = locationPreference,
      partitionRequirement = partitionRequirement,
      partitionDeriveSpec = partitionDeriveSpec,
      inputPorts = rebuiltInputPorts,
      outputPorts = rebuiltOutputPorts,
      isOneToManyOp = isOneToManyOp,
      suggestedWorkerNum = suggestedWorkerNum,
      pveName = pveName
    )
  }
}

// JSON (de)serialization of PhysicalOp is fully delegated to a dedicated
// serializer/deserializer pair, because several fields cannot go through the default
// jackson-module-scala case-class binding:
//   - `inputPorts` / `outputPorts` hold per-port link lists and an
//     `Either[Throwable, Schema]`; they are emitted as the slimmed-down
//     `inputPortsSerialized` / `outputPortsSerialized` views (links dropped, `Either`
//     collapsed to `Option[Schema]`) and the link lists are rehydrated at the
//     `PhysicalPlan` level by replaying `links`.
//   - `derivePartition` / `propagateSchema` are functions: `derivePartition` is rebuilt
//     lazily from the serializable `partitionDeriveSpec`, and `propagateSchema` falls back
//     to its identity default on deserialize (it is only consulted at compile time).
//   - `partitionRequirement` (`List[Option[PartitionInfo]]`) needs explicit handling so the
//     polymorphic `PartitionInfo` type discriminator survives the `Option` wrapper.
// See `PhysicalOpSerializer` / `PhysicalOpDeserializer`.
@JsonSerialize(using = classOf[PhysicalOpSerializer])
@JsonDeserialize(using = classOf[PhysicalOpDeserializer])
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
    // serializable description of how the output partition is derived from the input
    // partitions. If not specified, by default the output partition is the same as the
    // (first) input partition (see [[Passthrough]]).
    partitionDeriveSpec: DerivePartitionSpec = Passthrough(),
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
    suggestedWorkerNum: Option[Int] = None,
    // name of the PVE to execute within
    pveName: String = ""
) extends LazyLogging {

  // derive the output partition info given the input partitions. Rebuilt lazily from the
  // serializable `partitionDeriveSpec` so that it survives a JSON round-trip.
  @JsonIgnore lazy val derivePartition: List[PartitionInfo] => PartitionInfo =
    partitionDeriveSpec.toFunction

  /**
    * Serializable view of [[inputPorts]] used by [[PhysicalOpSerializer]] for JSON output:
    * the per-port link lists are dropped (rehydrated at the [[PhysicalPlan]] level by
    * replaying links) and the `Either[Throwable, Schema]` is collapsed to an
    * `Option[Schema]`.
    */
  @JsonIgnore
  def inputPortsSerialized: Map[PortIdentity, (InputPort, Option[Schema])] =
    inputPorts.map {
      case (portId, (port, _, schema)) => portId -> ((port, schema.toOption))
    }

  /**
    * Serializable view of [[outputPorts]]; see [[inputPortsSerialized]].
    */
  @JsonIgnore
  def outputPortsSerialized: Map[PortIdentity, (OutputPort, Option[Schema])] =
    outputPorts.map {
      case (portId, (port, _, schema)) => portId -> ((port, schema.toOption))
    }

  // all the "dependee" links are also blocking
  lazy val dependeeInputs: List[PortIdentity] =
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

  @JsonIgnore // this is needed to prevent the serialization issue
  def isPythonBased: Boolean = {
    opExecInitInfo match {
      case OpExecWithCode(_, language) =>
        language == "python" || language == "r-tuple" || language == "r-table"
      case _ => false
    }
  }

  @JsonIgnore
  def getCode: String = {
    opExecInitInfo match {
      case OpExecWithCode(code, _) => code
      case _                       => throw new IllegalAccessError("No code information in this physical operator")
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
    * creates a copy with the partition-derivation spec. The runtime `derivePartition`
    * function is rebuilt lazily from this spec.
    */
  def withDerivePartition(partitionDeriveSpec: DerivePartitionSpec): PhysicalOp = {
    this.copy(partitionDeriveSpec = partitionDeriveSpec)
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

  def withPveName(name: String): PhysicalOp = {
    this.copy(pveName = name)
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
  def getInputPortDependencyPairs: List[PortIdentity] = {
    val dependencyDag = {
      new DirectedAcyclicGraph[PortIdentity, DefaultEdge](classOf[DefaultEdge])
    }
    inputPorts.values
      .map(_._1)
      .flatMap(port => port.dependencies.map(dependee => port.id -> dependee))
      .foreach({
        case (depender: PortIdentity, dependee: PortIdentity) =>
          if (!dependencyDag.containsVertex(dependee)) {
            dependencyDag.addVertex(dependee)
          }
          if (!dependencyDag.containsVertex(depender)) {
            dependencyDag.addVertex(depender)
          }
          dependencyDag.addEdge(dependee, depender)
      })
    val topologicalIterator =
      new TopologicalOrderIterator[PortIdentity, DefaultEdge](dependencyDag)
    val processingOrder = new ArrayBuffer[PortIdentity]()
    while (topologicalIterator.hasNext) {
      processingOrder.append(topologicalIterator.next())
    }
    processingOrder.toList
  }
}
