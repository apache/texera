package edu.uci.ics.amber.operator.udf.python.source

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.executor.OpExecWithCode
import edu.uci.ics.amber.core.tuple.{Attribute, Schema}
import edu.uci.ics.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.source.SourceOperatorDescriptor
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.OutputPort

class PythonUDFSourceOpDescV2 extends SourceOperatorDescriptor {

  @JsonProperty(
    required = true,
    defaultValue = "# from pytexera import *\n" +
      "# class GenerateOperator(UDFSourceOperator):\n" +
      "# \n" +
      "#     @overrides\n" +
      "#     \n" +
      "#     def produce(self) -> Iterator[Union[TupleLike, TableLike, None]]:\n" +
      "#         yield\n"
  )
  @JsonSchemaTitle("Python script")
  @JsonPropertyDescription("Input your code here")
  var code: String = _

  @JsonProperty(required = true, defaultValue = "1")
  @JsonSchemaTitle("Worker count")
  @JsonPropertyDescription("Specify how many parallel workers to launch")
  var workers: Int = 1

  @JsonProperty()
  @JsonSchemaTitle("Columns")
  @JsonPropertyDescription("The columns of the source")
  var columns: List[Attribute] = List.empty

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    require(workers >= 1, "Need at least 1 worker.")
    val physicalOp = PhysicalOp
      .sourcePhysicalOp(workflowId, executionId, operatorIdentifier, OpExecWithCode(code, "python"))
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withIsOneToManyOp(true)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> sourceSchema()))
      )
      .withLocationPreference(Option.empty)

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
      "1-out Python UDF",
      "User-defined function operator in Python script",
      OperatorGroupConstants.PYTHON_GROUP,
      List.empty, // No input ports for a source operator
      List(OutputPort()),
      supportReconfiguration = true
    )
  }

  override def sourceSchema(): Schema = {
    if (columns != null && columns.nonEmpty) {
      Schema().add(columns)
    } else {
      Schema()
    }
  }
}
