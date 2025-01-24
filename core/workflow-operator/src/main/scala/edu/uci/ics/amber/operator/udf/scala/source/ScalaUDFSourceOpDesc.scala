package edu.uci.ics.amber.operator.udf.scala.source

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.executor.OpExecWithCode
import edu.uci.ics.amber.core.tuple.{Attribute, Schema}
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow._
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.source.SourceOperatorDescriptor
import edu.uci.ics.amber.operator.util.OperatorDescriptorUtils

import scala.jdk.javaapi.CollectionConverters.asScala

class ScalaUDFSourceOpDesc extends SourceOperatorDescriptor {

  // Code property to hold the user-defined Scala UDF script
  @JsonProperty(required = true, defaultValue =
    "import edu.uci.ics.amber.engine.common.workflow.PortIdentity\n"+
    "import edu.uci.ics.amber.engine.common.SourceOperatorExecutor\n" +
      "import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike\n" +
      "\n" +
    "// Choose from the following templates:\n" +
      "// \n" +
      "// class ScalaUDFOpExec extends SourceOperatorExecutor {\n" +
      "//   override def produceTuple(): Iterator[TupleLike] = {\n" +
      "//     // Your logic here\n" +
      "//     Iterator.empty\n" +
      "//   }\n" +
      "// }")
  @JsonSchemaTitle("Scala script")
  @JsonPropertyDescription("input your code here")
  var code: String = _

  // Output columns (attributes) for the source
  @JsonProperty()
  @JsonSchemaTitle("Columns")
  @JsonPropertyDescription("The columns of the source")
  var columns: List[Attribute] = List()

  // Define the physical operator
  override def getPhysicalOp(workflowId: WorkflowIdentity, executionId: ExecutionIdentity): PhysicalOp = {
    // Initialize the operator execution with the Scala code
    val exec = OpExecWithCode(code, "scala")

    // Initialize the physical operator as a source operator
    var physicalOp = PhysicalOp.sourcePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        exec
      ).withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withIsOneToManyOp(true)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> sourceSchema()))
      )

    // Set parallelism based on the worker count
    physicalOp = physicalOp.withParallelizable(false)

    physicalOp
  }

  // Define the operator metadata
  override def operatorInfo: OperatorInfo = {
    new OperatorInfo(
      "Scala UDF Source",
      "User-defined function operator in Scala script",
      OperatorGroupConstants.SCALA_GROUP,
      List.empty[InputPort],
      List(new OutputPort(new PortIdentity(0, false), "", false)),
      false, false, true, false
    )
  }

  // Define the schema for the source output
  override def sourceSchema(): Schema = {
    if (columns != null && columns.nonEmpty) {
      Schema().add(columns)
    } else {
      Schema()
    }
  }
}
