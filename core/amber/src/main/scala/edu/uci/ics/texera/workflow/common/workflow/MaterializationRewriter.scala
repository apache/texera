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
    val fromOp = physicalPlan.operatorMap(fromOpId)
    val fromOutputPortIdx = fromOp.outputToOrdinalMapping(linkId)
    val fromOutputPortName = fromOp.outputPorts(fromOutputPortIdx).displayName
    val toOpId = linkId.to
    val toOp = physicalPlan.operatorMap(toOpId)
    val toInputPortIdx = physicalPlan.operatorMap(toOpId).inputToOrdinalMapping(linkId)
    val toInputPortName = toOp.inputPorts(toInputPortIdx).displayName

    val materializationWriter = new ProgressiveSinkOpDesc()
    materializationWriter.setContext(context)

    val fromOpIdInputSchema: Array[Schema] =
      if (!logicalPlan.operatorMap(fromOpId.operator).isInstanceOf[SourceOperatorDescriptor])
        logicalPlan
          .inputSchemaMap(logicalPlan.operatorMap(fromOpId.operator).operatorIdentifier)
          .map(s => s.get)
          .toArray
      else Array()
    val matWriterInputSchema = logicalPlan
      .operatorMap(fromOpId.operator)
      .getOutputSchemas(
        fromOpIdInputSchema
      )(fromOutputPortIdx)
    val matWriterOutputSchema =
      materializationWriter.getOutputSchemas(Array(matWriterInputSchema))(0)
    materializationWriter.setStorage(
      opResultStorage.create(
        key = materializationWriter.operatorID,
        schema = matWriterOutputSchema,
        mode = OpResultStorage.defaultStorageMode
      )
    )
    val matWriterOpExecConfig =
      materializationWriter.operatorExecutor(
        OperatorSchemaInfo(Array(matWriterInputSchema), Array(matWriterOutputSchema))
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
      .addOperator(matWriterOpExecConfig)
      .addOperator(matReaderOpExecConfig)
      .addEdge(
        fromOpId,
        fromOutputPortIdx,
        matWriterOpExecConfig.id,
        0
      )
      .addEdge(
        matReaderOpExecConfig.id,
        0,
        toOpId,
        toInputPortIdx
      )
      .removeEdge(linkId)

    newPlan
  }

}
