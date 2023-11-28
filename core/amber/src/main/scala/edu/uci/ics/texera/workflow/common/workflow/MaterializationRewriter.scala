package edu.uci.ics.texera.workflow.common.workflow

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.virtualidentity.{LayerIdentity, LinkIdentity}
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc

import scala.collection.mutable

class MaterializationRewriter(
    val context: WorkflowContext,
    val opResultStorage: OpResultStorage
) extends LazyLogging {

  def addMaterializationToLink(
      physicalPlan: PhysicalPlan,
      logicalPlan: LogicalPlan,
      linkId: LinkIdentity,
      writerReaderPairs: mutable.HashMap[LayerIdentity, LayerIdentity]
  ): PhysicalPlan = {

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

    // create 2 links for materialization
    val readerToDestLink = LinkIdentity(matReaderOpExecConfig.id, 0, toOpId, toInputPortIdx)
    val sourceToWriterLink = LinkIdentity(fromOpId, fromOutputPortIdx, matWriterOpExecConfig.id, 0)
    // add the pair to the map for later adding edges between 2 regions.
    writerReaderPairs(matWriterOpExecConfig.id) = matReaderOpExecConfig.id

    physicalPlan
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
      .setOperator(
        toOp.copy(
          // update the input mapping by replacing the original link with the new link from materialization.
          inputToOrdinalMapping =
            toOp.inputToOrdinalMapping - linkId + (readerToDestLink -> toInputPortIdx),
          // the dest operator's input port is not blocking anymore.
          blockingInputs = toOp.blockingInputs.filter(e => e != toInputPortIdx)
        )
      )
      .setOperator(
        fromOp.copy(
          // update the output mapping by replacing the original link with the new link to materialization.
          outputToOrdinalMapping =
            fromOp.outputToOrdinalMapping - linkId + (sourceToWriterLink -> fromOutputPortIdx)
        )
      )
  }

}
