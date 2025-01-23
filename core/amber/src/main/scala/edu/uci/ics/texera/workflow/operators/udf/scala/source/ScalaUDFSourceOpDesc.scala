package edu.uci.ics.texera.workflow.operators.udf.scala.source

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.architecture.deploysemantics.{PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.common.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort, PortIdentity}
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, Schema}
import edu.uci.ics.texera.workflow.operators.util.OperatorDescriptorUtils

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

  // Worker count to determine parallelism
  @JsonProperty(required = true, defaultValue = "1")
  @JsonSchemaTitle("Worker count")
  @JsonPropertyDescription("Specify how many parallel workers to launch")
  var workers: Int = 1

  // Output columns (attributes) for the source
  @JsonProperty()
  @JsonSchemaTitle("Columns")
  @JsonPropertyDescription("The columns of the source")
  var columns: java.util.List[Attribute] = new java.util.ArrayList()

  // Define the physical operator
  override def getPhysicalOp(workflowId: WorkflowIdentity, executionId: ExecutionIdentity): PhysicalOp = {
    // Initialize the operator execution with the Scala code
    val exec = OpExecInitInfo(code, "scala")

    // Define schema propagation logic
    val schemaFunc = SchemaPropagationFunc((inputSchemas: Map[PortIdentity, Schema]) => {
      val outputSchema = sourceSchema()
      val javaMap = new java.util.HashMap[PortIdentity, Schema]()
      javaMap.put(operatorInfo.outputPorts.head.id, outputSchema)
      OperatorDescriptorUtils.toImmutableMap(javaMap)
    })

    // Initialize the physical operator as a source operator
    var physicalOp = PhysicalOp.sourcePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        exec
      ).withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withIsOneToManyOp(true)
      .withPropagateSchema(schemaFunc)

    // Set parallelism based on the worker count
    if (workers > 1) {
      physicalOp = physicalOp.withParallelizable(true).withSuggestedWorkerNum(workers)
    } else {
      physicalOp = physicalOp.withParallelizable(false)
    }

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
    val outputSchemaBuilder = Schema.builder()
    if (columns != null && !columns.isEmpty) {
      outputSchemaBuilder.add(asScala(columns))
    }
    outputSchemaBuilder.build()
  }
}
