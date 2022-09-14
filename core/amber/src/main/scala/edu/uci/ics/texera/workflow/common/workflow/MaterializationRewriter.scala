package edu.uci.ics.texera.workflow.common.workflow

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.virtualidentity.{LinkIdentity, OperatorIdentity}
import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentityUtil.toOperatorIdentity
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.Utils.objectMapper
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.common.workflow.WorkflowRewriter.copyOperator
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc

import java.util.UUID
import edu.uci.ics.texera.workflow.operators.sink.managed.{
  ProgressiveSinkOpDesc,
  ProgressiveSinkOpExec
}

import scala.collection.mutable

class MaterializationRewriter(
    val context: WorkflowContext,
    val opResultStorage: OpResultStorage
) extends LazyLogging {

  def addMaterializationToLink(
      physicalPlan: PhysicalPlan,
      logicalPlan: LogicalPlan,
      linkId: LinkIdentity
  ): PhysicalPlan = {

    var newPlan = physicalPlan

    val fromOpId = linkId.from
    val toOpId = linkId.to

    val materializationWriter = new ProgressiveSinkOpDesc()
    materializationWriter.setContext(context)

    val fromOpIdInputSchema: Array[Schema] =
      if (!logicalPlan.operators(fromOpId.operator).isInstanceOf[SourceOperatorDescriptor])
        logicalPlan.inputSchemaMap(logicalPlan.operators(fromOpId.operator)).map(s => s.get).toArray
      else Array()
    val matWriterInputSchemas = logicalPlan
      .operators(fromOpId.operator)
      .getOutputSchemas(
        fromOpIdInputSchema
      )
    val matWriterOutputSchemas = materializationWriter.getOutputSchemas(matWriterInputSchemas)
    materializationWriter.setStorage(
      opResultStorage.create(materializationWriter.operatorID, matWriterOutputSchemas(0))
    )
    val matWriterOpExecConfig =
      materializationWriter.operatorExecutor(
        OperatorSchemaInfo(matWriterInputSchemas, matWriterOutputSchemas)
      )

    val materializationReader = new CacheSourceOpDesc(
      materializationWriter.operatorID,
      opResultStorage: OpResultStorage
    )
    materializationReader.setContext(context)
    materializationReader.schema = materializationWriter.getStorage.getSchema
    val matReaderOutputSchema = materializationReader.getOutputSchemas(Array())
    val matReaderOpExecConfig =
      materializationReader.operatorExecutor(
        OperatorSchemaInfo(Array(), matReaderOutputSchema)
      )

    newPlan = newPlan
      .addOperator(matWriterOpExecConfig.id, matWriterOpExecConfig)
      .addOperator(matReaderOpExecConfig.id, matReaderOpExecConfig)
      .addEdge(fromOpId, 0, matWriterOpExecConfig.id, 0)
      .addEdge(matReaderOpExecConfig.id, 0, toOpId, 0)

    newPlan
  }

}
