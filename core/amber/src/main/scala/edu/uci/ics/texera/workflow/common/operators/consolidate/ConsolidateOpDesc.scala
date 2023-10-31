package edu.uci.ics.texera.workflow.common.operators.consolidate

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.texera.workflow.common.ProgressiveUtils
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}

import scala.collection.JavaConverters.asScalaBuffer
import scala.collection.immutable.List

class ConsolidateOpDesc extends OperatorDescriptor {
  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      "Consolidate",
      "Consolidate retractable inputs, collect all of them and output append-only data",
      OperatorGroupConstants.UTILITY_GROUP,
      List(InputPort("")),
      List(OutputPort("")),
      supportRetractableInput = true
    )
  }

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    val newAttrs = asScalaBuffer(schemas(0).getAttributes)
      .filter(attr => attr != ProgressiveUtils.insertRetractFlagAttr)
    Schema.newBuilder().add(newAttrs.toArray: _*).build()
  }

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    OpExecConfig.manyToOneLayer(operatorIdentifier, _ => new ConsolidateOpExec(operatorSchemaInfo))
  }
}
