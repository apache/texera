package edu.uci.ics.texera.workflow.operators.source.cache

import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.{
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}

import java.util.Collections.singletonList
import scala.collection.JavaConverters.asScalaBuffer
import scala.collection.immutable.List
import scala.collection.mutable

class CacheSourceOpDesc(src: mutable.MutableList[Tuple]) extends SourceOperatorDescriptor {
  assert(null != src)

  var schema: Schema = _

  override def sourceSchema(): Schema = schema

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    new CacheSourceOpExecConfig(operatorIdentifier, src)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Cache Source Operator",
      "Retrieve the cached output to src",
      OperatorGroupConstants.RESULT_GROUP,
      List.empty,
      asScalaBuffer(singletonList(OutputPort(""))).toList
    )
}
