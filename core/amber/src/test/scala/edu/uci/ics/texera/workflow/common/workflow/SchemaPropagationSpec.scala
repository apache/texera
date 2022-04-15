package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.operators.sink.SinkOpDesc
import org.apache.arrow.util.Preconditions
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable

class SchemaPropagationSpec extends AnyFlatSpec with BeforeAndAfter {

  private abstract class TempTestSourceOpDesc extends SourceOperatorDescriptor {
    override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = ???
    override def operatorInfo: OperatorInfo =
      OperatorInfo("", "", "", List(InputPort()), List(OutputPort()))
  }
  private class TempTestSinkOpDesc extends SinkOpDesc {
    override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = ???
    override def operatorInfo: OperatorInfo =
      OperatorInfo("", "", "", List(InputPort()), List(OutputPort()))
    override def getOutputSchema(schemas: Array[Schema]): Schema = {
      Preconditions.checkArgument(schemas.length == 1)
      schemas(0)
    }
  }

  it should "propagate workflow schema with multiple input and output ports" in {
    // build the following workflow DAG:
    // trainingData ---\                 /----> mlVizSink
    // testingData  ----> mlTrainingOp--<
    // inferenceData ---------------------> mlInferenceOp --> inferenceSink

    val dataSchema = Schema.newBuilder().add("dataCol", AttributeType.INTEGER).build()
    val trainingScan = new TempTestSourceOpDesc() {
      override def sourceSchema(): Schema = dataSchema
    }
    trainingScan.operatorID = "trainingScan"

    val testingScan = new TempTestSourceOpDesc() {
      override def sourceSchema(): Schema = dataSchema
    }
    testingScan.operatorID = "testingScan"

    val inferenceScan = new TempTestSourceOpDesc() {
      override def sourceSchema(): Schema = dataSchema
    }
    inferenceScan.operatorID = "inferenceScan"

    val mlModelSchema = Schema.newBuilder().add("model", AttributeType.STRING).build()
    val mlVizSchema = Schema.newBuilder().add("visualization", AttributeType.STRING).build()

    val mlTrainingOp = new OperatorDescriptor() {
      override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = ???

      override def operatorInfo: OperatorInfo =
        OperatorInfo(
          "",
          "",
          "",
          List(InputPort("training"), InputPort("testing")),
          List(OutputPort("visualization"), OutputPort("model"))
        )

      override def getOutputSchema(schemas: Array[Schema]): Schema = ???

      override def getOutputSchemas(schemas: Array[Schema]): Array[Schema] = {
        Preconditions.checkArgument(schemas.length == 2)
        Preconditions.checkArgument(schemas.distinct.length == 1)
        Array(mlVizSchema, mlModelSchema)
      }
    }
    mlTrainingOp.operatorID = "mlTrainingOp"

    val mlInferOp = new OperatorDescriptor() {
      override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = ???

      override def operatorInfo: OperatorInfo =
        OperatorInfo(
          "",
          "",
          "",
          List(InputPort("model"), InputPort("data")),
          List(OutputPort("data"))
        )

      override def getOutputSchema(schemas: Array[Schema]): Schema = ???

      override def getOutputSchemas(schemas: Array[Schema]): Array[Schema] = {
        Preconditions.checkArgument(schemas.length == 2)
        Array(schemas(1))
      }
    }
    mlInferOp.operatorID = "mlInferOp"

    val mlVizSink = new TempTestSinkOpDesc
    mlVizSink.operatorID = "mlVizSink"

    val inferenceSink = new TempTestSinkOpDesc
    inferenceSink.operatorID = "inferenceSink"

    val operators = new mutable.MutableList[OperatorDescriptor]()
    operators += (trainingScan, testingScan, inferenceScan, mlTrainingOp, mlInferOp, mlVizSink, inferenceSink)

    val links = new mutable.MutableList[OperatorLink]()
    links += OperatorLink(
      OperatorPort(trainingScan.operatorID),
      OperatorPort(mlTrainingOp.operatorID, 0)
    )
    links += OperatorLink(
      OperatorPort(testingScan.operatorID),
      OperatorPort(mlTrainingOp.operatorID, 1)
    )
    links += OperatorLink(
      OperatorPort(inferenceScan.operatorID),
      OperatorPort(mlInferOp.operatorID, 1)
    )
    links += OperatorLink(
      OperatorPort(mlTrainingOp.operatorID, 0),
      OperatorPort(mlVizSink.operatorID)
    )
    links += OperatorLink(
      OperatorPort(mlTrainingOp.operatorID, 1),
      OperatorPort(mlInferOp.operatorID, 0)
    )
    links += OperatorLink(
      OperatorPort(mlInferOp.operatorID, 0),
      OperatorPort(inferenceSink.operatorID, 0)
    )

    val workflowInfo = new WorkflowInfo(operators, links, new mutable.MutableList[BreakpointInfo]())
    val workflowCompiler = new WorkflowCompiler(workflowInfo, new WorkflowContext())

    val schemaResult = workflowCompiler.propagateWorkflowSchema()

    assert(schemaResult(mlTrainingOp).head.get.equals(dataSchema))
    assert(schemaResult(mlTrainingOp)(1).get.equals(dataSchema))

    assert(schemaResult(mlInferOp).head.get.equals(mlModelSchema))
    assert(schemaResult(mlInferOp)(1).get.equals(dataSchema))

    assert(schemaResult(mlVizSink).head.get.equals(mlVizSchema))
    assert(schemaResult(inferenceSink).head.get.equals(dataSchema))

  }

}
