package edu.uci.ics.amber.operator.udf.scala

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.executor.{OpExecWithCode, SourceOperatorExecutor}
import edu.uci.ics.amber.operator.{LogicalOp, PortDescription, StateTransferFunc}
import edu.uci.ics.amber.core.tuple.{Attribute, Schema, TupleLike}
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow._
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}

import scala.util.{Success, Try}

class ScalaUDFOpDesc extends LogicalOp {
  @JsonProperty(
    required = true,
    defaultValue =
        "import edu.uci.ics.amber.core.executor.SourceOperatorExecutor;\n" +
          "import edu.uci.ics.amber.core.tuple.TupleLike;\n" +
          "import edu.uci.ics.amber.core.tuple.Tuple;\n" +
          "import scala.Function1;\n" +
          "\n" +
          "class ScalaUDFOpExec extends OperatorExecutor {\n" +
          "    override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {\n" +
          "        // Your UDF logic here\n" +
          "        Iterator(tuple)\n" +
          "    }\n" +
          "}"
  )
  @JsonSchemaTitle("Scala UDF script")
  @JsonPropertyDescription("input your code here")
  var code: String = ""

  @JsonProperty(required = true, defaultValue = "true")
  @JsonSchemaTitle("Retain input columns")
  @JsonPropertyDescription("Keep the original input columns?")
  var retainInputColumns: Boolean = Boolean.box(false)

  @JsonProperty
  @JsonSchemaTitle("Extra output column(s)")
  @JsonPropertyDescription(
    "Name of the newly added output columns that the UDF will produce, if any"
  )
  var outputColumns: List[Attribute] = List()

  override def getPhysicalOp(
                              workflowId: WorkflowIdentity,
                              executionId: ExecutionIdentity
                            ): PhysicalOp = {
    val opInfo = this.operatorInfo
    val partitionRequirement: List[Option[PartitionInfo]] = if (inputPorts != null) {
      inputPorts.map(p => Option(p.partitionRequirement))
    } else {
      opInfo.inputPorts.map(_ => None)
    }

    val propagateSchema = (inputSchemas: Map[PortIdentity, Schema]) => {
      val inputSchema = inputSchemas(operatorInfo.inputPorts.head.id)
      var outputSchema = if (retainInputColumns) inputSchema else Schema()

      // Add custom output columns if defined
      if (outputColumns != null) {
        if (retainInputColumns) {
          // Check for duplicate column names
          for (column <- outputColumns) {
            if (inputSchema.containsAttribute(column.getName)) {
              throw new RuntimeException(s"Column name ${column.getName} already exists!")
            }
          }
        }
        // Add output columns to the schema
        outputSchema = outputSchema.add(outputColumns)
      }

      Map(operatorInfo.outputPorts.head.id -> outputSchema)
    }

    PhysicalOp
      .manyToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithCode(code, "scala")
      )
      .withDerivePartition(_ => UnknownPartition())
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPartitionRequirement(partitionRequirement)
      .withIsOneToManyOp(true)
      .withParallelizable(false)
      .withPropagateSchema(SchemaPropagationFunc(propagateSchema))
  }

  override def operatorInfo: OperatorInfo = {
    val inputPortInfo = if (inputPorts != null) {
      inputPorts.zipWithIndex.map {
        case (portDesc: PortDescription, idx) =>
          InputPort(
            PortIdentity(idx),
            displayName = portDesc.displayName,
            allowMultiLinks = portDesc.allowMultiInputs,
            dependencies = portDesc.dependencies.map(idx => PortIdentity(idx))
          )
      }
    } else {
      List(InputPort(PortIdentity(), allowMultiLinks = true))
    }
    val outputPortInfo = if (outputPorts != null) {
      outputPorts.zipWithIndex.map {
        case (portDesc, idx) => OutputPort(PortIdentity(idx), displayName = portDesc.displayName)
      }
    } else {
      List(OutputPort())
    }

    OperatorInfo(
      "Scala UDF",
      "User-defined function operator in Scala script",
      OperatorGroupConstants.SCALA_GROUP,
      inputPortInfo,
      outputPortInfo,
      dynamicInputPorts = true,
      dynamicOutputPorts = true,
      supportReconfiguration = true,
      allowPortCustomization = true
    )
  }

  override def runtimeReconfiguration(
                                       workflowId: WorkflowIdentity,
                                       executionId: ExecutionIdentity,
                                       oldLogicalOp: LogicalOp,
                                       newLogicalOp: LogicalOp
                                     ): Try[(PhysicalOp, Option[StateTransferFunc])] = {
    Success(newLogicalOp.getPhysicalOp(workflowId, executionId), None)
  }
}
