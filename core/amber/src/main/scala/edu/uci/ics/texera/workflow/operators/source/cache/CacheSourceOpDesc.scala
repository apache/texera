package edu.uci.ics.texera.workflow.operators.source.cache

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.{
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}

import java.util.Collections.singletonList
import scala.collection.JavaConverters.asScalaBuffer

class CacheSourceOpDesc(val targetSinkStorageId: String, opResultStorage: OpResultStorage)
    extends SourceOperatorDescriptor {
  assert(null != targetSinkStorageId)
  assert(null != opResultStorage)

  var schema: Schema = opResultStorage.get(targetSinkStorageId).getSchema

  override def sourceSchema(): Schema = schema

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo) = {
    OpExecConfig.localLayer(
      operatorIdentifier,
      _ => new CacheSourceOpExec(opResultStorage.get(targetSinkStorageId))
    )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Cache Source Operator",
      "Retrieve the cached output to src",
      OperatorGroupConstants.UTILITY_GROUP,
      List.empty,
      asScalaBuffer(singletonList(OutputPort(""))).toList
    )
}
