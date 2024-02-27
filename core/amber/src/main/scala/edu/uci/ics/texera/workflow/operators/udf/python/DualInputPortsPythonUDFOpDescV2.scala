package edu.uci.ics.texera.workflow.operators.udf.python

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.common.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.LogicalOp
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, Schema}
import edu.uci.ics.texera.workflow.common.workflow.UnknownPartition
import edu.uci.ics.amber.engine.common.workflow.InputPort
import edu.uci.ics.amber.engine.common.workflow.OutputPort
import edu.uci.ics.amber.engine.common.workflow.PortIdentity

class DualInputPortsPythonUDFOpDescV2 extends LogicalOp {
  @JsonProperty(
    required = true,
    defaultValue =
      "# Choose from the following templates:\n" +
        "# \n" +
        "# from pytexera import *\n" +
        "# \n" +
        "# class ProcessTupleOperator(UDFOperatorV2):\n" +
        "#     \n" +
        "#     @overrides\n" +
        "#     def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:\n" +
        "#         yield tuple_\n" +
        "# \n" +
        "# class ProcessBatchOperator(UDFBatchOperator):\n" +
        "#     BATCH_SIZE = 10 # must be a positive integer\n" +
        "# \n" +
        "#     @overrides\n" +
        "#     def process_batch(self, batch: Batch, port: int) -> Iterator[Optional[BatchLike]]:\n" +
        "#         yield batch\n" +
        "# \n" +
        "# class ProcessTableOperator(UDFTableOperator):\n" +
        "# \n" +
        "#     @overrides\n" +
        "#     def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:\n" +
        "#         yield table\n"
  )
  @JsonSchemaTitle("Python script")
  @JsonPropertyDescription("Input your code here")
  var code: String = ""

  @JsonProperty(required = true)
  @JsonSchemaTitle("Worker count")
  @JsonPropertyDescription("Specify how many parallel workers to lunch")
  var workers: Int = Int.box(1)

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
    Preconditions.checkArgument(workers >= 1, "Need at least 1 worker.", Array())
    if (workers > 1) {
      PhysicalOp
        .oneToOnePhysicalOp(workflowId, executionId, operatorIdentifier, OpExecInitInfo(code))
        .withDerivePartition(_ => UnknownPartition())
        .withParallelizable(true)
        .withInputPorts(operatorInfo.inputPorts, inputPortToSchemaMapping)
        .withOutputPorts(operatorInfo.outputPorts, outputPortToSchemaMapping)
        .withBlockingInputs(List(operatorInfo.inputPorts.head.id))
        .withSuggestedWorkerNum(workers)
    } else {
      PhysicalOp
        .manyToOnePhysicalOp(workflowId, executionId, operatorIdentifier, OpExecInitInfo(code))
        .withDerivePartition(_ => UnknownPartition())
        .withParallelizable(false)
        .withInputPorts(operatorInfo.inputPorts, inputPortToSchemaMapping)
        .withOutputPorts(operatorInfo.outputPorts, outputPortToSchemaMapping)
        .withBlockingInputs(List(operatorInfo.inputPorts.head.id))
    }
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "2-in Python UDF",
      "User-defined function operator in Python script",
      OperatorGroupConstants.UDF_GROUP,
      inputPorts = List(
        InputPort(PortIdentity(), displayName = "model", allowMultiLinks = true),
        InputPort(
          PortIdentity(1),
          displayName = "tuples",
          allowMultiLinks = true,
          dependencies = List(PortIdentity(0))
        )
      ),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 2)
    val inputSchema = schemas(1)
    val outputSchemaBuilder = Schema.builder()
    // keep the same schema from input
    if (retainInputColumns) outputSchemaBuilder.add(inputSchema)
    // for any pythonUDFType, it can add custom output columns (attributes).
    if (outputColumns != null) {
      if (retainInputColumns) { // check if columns are duplicated

        for (column <- outputColumns) {
          if (inputSchema.containsAttribute(column.getName))
            throw new RuntimeException("Column name " + column.getName + " already exists!")
        }
      }
      outputSchemaBuilder.add(outputColumns).build()
    }
    outputSchemaBuilder.build()
  }
}
