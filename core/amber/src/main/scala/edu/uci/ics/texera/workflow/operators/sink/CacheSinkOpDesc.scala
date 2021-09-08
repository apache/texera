package edu.uci.ics.texera.workflow.operators.sink

import com.google.common.base.Preconditions
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.ProgressiveUtils
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}

import java.util.Collections.singletonList
import scala.collection.JavaConverters.asScalaBuffer
import scala.collection.immutable.List

class CacheSinkOpDesc(val uuid: String, opResultStorage: OpResultStorage) extends SimpleSinkOpDesc {

  var schema: Schema = _

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    schema = operatorSchemaInfo.outputSchema
    new CacheSinkOpExecConfig(operatorIdentifier, operatorSchemaInfo, uuid, opResultStorage)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Cache Sink Operator",
      "Cache the output to dest",
      OperatorGroupConstants.RESULT_GROUP,
      asScalaBuffer(singletonList(InputPort(""))).toList,
      List.empty
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    //FIXME: Copied from edu.uci.ics.texera.workflow.operators.sink.SimpleSinkOpDesc.getOutputSchema
    Preconditions.checkArgument(schemas.length == 1)
    val inputSchema = schemas(0)
    if (inputSchema.containsAttribute(ProgressiveUtils.insertRetractFlagAttr.getName)) {
      Schema.newBuilder
        .add(inputSchema)
        .remove(ProgressiveUtils.insertRetractFlagAttr.getName)
        .build
    } else {
      inputSchema
    }
  }
}
