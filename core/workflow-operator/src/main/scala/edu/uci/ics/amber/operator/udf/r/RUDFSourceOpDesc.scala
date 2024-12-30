package edu.uci.ics.amber.operator.udf.r

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.executor.OpExecInitInfo
import edu.uci.ics.amber.core.tuple.{Attribute, Schema}
import edu.uci.ics.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.source.SourceOperatorDescriptor
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.{OutputPort, PortIdentity}

class RUDFSourceOpDesc extends SourceOperatorDescriptor {

  @JsonProperty(
    required = true,
    defaultValue = "# If using Table API:\n" +
      "# function() { \n" +
      "#   return (data.frame(Column_Here = \"Value_Here\")) \n" +
      "# }\n" +
      "\n" +
      "# If using Tuple API:\n" +
      "# library(coro)\n" +
      "# coro::generator(function() {\n" +
      "#   yield (list(text= \"hello world!\"))\n" +
      "# })"
  )
  @JsonSchemaTitle("R Source UDF Script")
  @JsonPropertyDescription("Input your code here")
  var code: String = _

  @JsonProperty(required = true, defaultValue = "1")
  @JsonSchemaTitle("Worker count")
  @JsonPropertyDescription("Specify how many parallel workers to launch")
  var workers: Int = 1

  @JsonProperty(required = true, defaultValue = "false")
  @JsonSchemaTitle("Use Tuple API?")
  @JsonPropertyDescription("Check this box to use Tuple API, leave unchecked to use Table API")
  var useTupleAPI: Boolean = false

  @JsonProperty()
  @JsonSchemaTitle("Columns")
  @JsonPropertyDescription("The columns of the source")
  var columns: List[Attribute] = List.empty

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    val rOperatorType = if (useTupleAPI) "r-tuple" else "r-table"
    val exec = OpExecInitInfo(code, rOperatorType)
    require(workers >= 1, "Need at least 1 worker.")

    val func = SchemaPropagationFunc { _: Map[PortIdentity, Schema] =>
      val outputSchema = sourceSchema()
      Map(operatorInfo.outputPorts.head.id -> outputSchema)
    }

    val physicalOp = PhysicalOp
      .sourcePhysicalOp(workflowId, executionId, operatorIdentifier, exec)
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withIsOneToManyOp(true)
      .withPropagateSchema(func)
      .withLocationPreference(None)

    if (workers > 1) {
      physicalOp
        .withParallelizable(true)
        .withSuggestedWorkerNum(workers)
    } else {
      physicalOp.withParallelizable(false)
    }
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      "1-out R UDF",
      "User-defined function operator in R script",
      OperatorGroupConstants.R_GROUP,
      List.empty, // No input ports for a source operator
      List(OutputPort())
    )
  }

  override def sourceSchema(): Schema = {
    val outputSchemaBuilder = Schema.builder()
    if (columns.nonEmpty && columns != null) {
      outputSchemaBuilder.add(columns)
    }
    outputSchemaBuilder.build()
  }
}
