package edu.uci.ics.amber.operator.sink.managed

import com.google.common.base.Preconditions
import edu.uci.ics.amber.core.executor.{ExecFactory, OpExecInitInfo}
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.sink.IncrementalOutputMode.SET_SNAPSHOT
import edu.uci.ics.amber.operator.sink.{IncrementalOutputMode, ProgressiveUtils, SinkOpDesc}
import edu.uci.ics.amber.operator.util.OperatorDescriptorUtils
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import edu.uci.ics.amber.virtualidentity.{ExecutionIdentity, OperatorIdentity, WorkflowIdentity}
import edu.uci.ics.amber.workflow.{InputPort, OutputPort, PortIdentity}

import java.util
import java.util.Collections.singletonList
import scala.jdk.javaapi.CollectionConverters.asScala

class ProgressiveSinkOpDesc extends SinkOpDesc {
  // use SET_SNAPSHOT as the default output mode
  // this will be set internally by the workflow compiler
  var outputMode = SET_SNAPSHOT
  // whether this sink corresponds to a visualization result, default is no
  private var chartType: Option[String] = None
  // corresponding upstream operator ID and output port, will be set by workflow compiler
  private var upstreamId: Option[OperatorIdentity] = None
   private var upstreamPort: Option[Integer] = None
  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    System.out.println("workflow ID:" + workflowId)
    PhysicalOp
      .localPhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecInitInfo((_, _) =>
          ExecFactory.newExecFromJavaClassName(
            "edu.uci.ics.amber.operator.sink.managed.ProgressiveSinkOpExec",
            objectMapper.writeValueAsString(this),
            0,
            1,
            Some(workflowId)
          )
        )
      )
      .withInputPorts(this.operatorInfo.inputPorts)
      .withOutputPorts(this.operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc((inputSchemas: Map[PortIdentity, Schema]) => {

          // Initialize a Java HashMap
          val javaMap = new util.HashMap[PortIdentity, Schema]
          val inputSchema = inputSchemas.values.head
          // SET_SNAPSHOT:
          var outputSchema: Schema = null
          if (this.outputMode == SET_SNAPSHOT)
            if (inputSchema.containsAttribute(ProgressiveUtils.insertRetractFlagAttr.getName)) {
              // input is insert/retract delta: the flag column is removed in output
              outputSchema = Schema
                .builder()
                .add(inputSchema)
                .remove(ProgressiveUtils.insertRetractFlagAttr.getName)
                .build()
            } else {
              // input is insert-only delta: output schema is the same as input schema
              outputSchema = inputSchema
            }
          else {
            // SET_DELTA: output schema is always the same as input schema
            outputSchema = inputSchema
          }
          javaMap.put(operatorInfo.outputPorts.head.id, outputSchema)
          // Convert the Java Map to a Scala immutable Map
          OperatorDescriptorUtils.toImmutableMap(javaMap)

        })
      )
  }
  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "View Results",
      "View the results",
      OperatorGroupConstants.UTILITY_GROUP,
      asScala(
        singletonList(
          new InputPort(
            new PortIdentity(0, false),
            "",
            false,
            asScala(new util.ArrayList[PortIdentity]).toSeq
          )
        )
      ).toList,
      asScala(singletonList(new OutputPort(new PortIdentity(0, false), "", false))).toList
    )
  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    val inputSchema = schemas(0)
// SET_SNAPSHOT:
    if (this.outputMode == SET_SNAPSHOT)
      if (inputSchema.containsAttribute(ProgressiveUtils.insertRetractFlagAttr.getName)) {
// input is insert/retract delta: the flag column is removed in output
        Schema
          .builder()
          .add(inputSchema)
          .remove(ProgressiveUtils.insertRetractFlagAttr.getName)
          .build()
      } else {
// input is insert-only delta: output schema is the same as input schema
        inputSchema
      }
    else {
// SET_DELTA: output schema is always the same as input schema
      inputSchema
    }
  }
  def getOutputMode: IncrementalOutputMode = outputMode
  def setOutputMode(outputMode: IncrementalOutputMode): Unit = {
    this.outputMode = outputMode
  }
  def getChartType: Option[String] = this.chartType
  def setChartType(chartType: String): Unit = {
    this.chartType = Option(chartType)
  }
  def getUpstreamId: Option[OperatorIdentity] = upstreamId
  def setUpstreamId(upstreamId: OperatorIdentity): Unit = {
    this.upstreamId = Option(upstreamId)
  }
  def getUpstreamPort: Option[Integer] = upstreamPort
  def setUpstreamPort(upstreamPort: Integer): Unit = {
    this.upstreamPort = Option(upstreamPort)
  }
}
