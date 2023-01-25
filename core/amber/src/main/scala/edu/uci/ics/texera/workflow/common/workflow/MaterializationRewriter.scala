package edu.uci.ics.texera.workflow.common.workflow

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc

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
      if (!logicalPlan.operatorMap(fromOpId.operator).isInstanceOf[SourceOperatorDescriptor])
        logicalPlan
          .inputSchemaMap(logicalPlan.operatorMap(fromOpId.operator).operatorIdentifier)
          .map(s => s.get)
          .toArray
      else Array()
    val matWriterInputSchemas = logicalPlan
      .operatorMap(fromOpId.operator)
      .getOutputSchemas(
        fromOpIdInputSchema
      )
    val matWriterOutputSchemas = materializationWriter.getOutputSchemas(matWriterInputSchemas)
    materializationWriter.setStorage(
      opResultStorage.create(materializationWriter.operatorID, matWriterOutputSchemas(0))
    )
    val matWriterOpExecConfig =
      materializationWriter.newOperatorExecutor(
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
      materializationReader.newOperatorExecutor(
        OperatorSchemaInfo(Array(), matReaderOutputSchema)
      )

    newPlan = newPlan
      .addOperator(matWriterOpExecConfig)
      .addOperator(matReaderOpExecConfig)
      .addEdge(fromOpId, matWriterOpExecConfig.id)
      .addEdge(matReaderOpExecConfig.id, toOpId)

    newPlan
  }

}
