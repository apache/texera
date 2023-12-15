package edu.uci.ics.texera.workflow.common.workflow

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalLink
import edu.uci.ics.amber.engine.common.virtualidentity.{OperatorIdentity, PhysicalOpIdentity}
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
      physicalLink: PhysicalLink,
      writerReaderPairs: mutable.HashMap[PhysicalOpIdentity, PhysicalOpIdentity]
  ): PhysicalPlan = {
    // get the actual Op from the physical plan. the operators on the link and that on the physical plan
    // are different due to partial rewrite
    val fromOp = physicalPlan.getOperator(physicalLink.id.from)
    val fromOutputPortIdx = fromOp.getPortIdxForOutputLinkId(physicalLink.id)

    // get the actual Op from the physical plan. the operators on the link and that on the physical plan
    // are different due to partial rewrite
    val toOp = physicalPlan.getOperator(physicalLink.id.to)
    val toInputPortIdx = toOp.getPortIdxForInputLinkId(physicalLink.id)

    val materializationWriter = new ProgressiveSinkOpDesc()
    materializationWriter.setContext(context)

    val fromOpIdInputSchema: Array[Schema] =
      if (
        !logicalPlan
          .getOperator(OperatorIdentity(fromOp.id.logicalOpId.id))
          .isInstanceOf[SourceOperatorDescriptor]
      )
        logicalPlan
          .inputSchemaMap(
            logicalPlan.getOperator(fromOp.id.logicalOpId.id).operatorIdentifier
          )
          .map(s => s.get)
          .toArray
      else Array()
    val matWriterInputSchema = logicalPlan
      .getOperator(fromOp.id.logicalOpId.id)
      .getOutputSchemas(
        fromOpIdInputSchema
      )(fromOutputPortIdx)
    val matWriterOutputSchema =
      materializationWriter.getOutputSchemas(Array(matWriterInputSchema))(0)
    materializationWriter.setStorage(
      opResultStorage.create(
        key = materializationWriter.operatorIdentifier,
        mode = OpResultStorage.defaultStorageMode
      )
    )
    opResultStorage.get(materializationWriter.operatorIdentifier).setSchema(matWriterOutputSchema)
    val matWriterOp =
      materializationWriter.getPhysicalOp(
        context.executionId,
        OperatorSchemaInfo(Array(matWriterInputSchema), Array(matWriterOutputSchema))
      )

    val materializationReader = new CacheSourceOpDesc(
      materializationWriter.operatorIdentifier,
      opResultStorage: OpResultStorage
    )
    materializationReader.setContext(context)
    materializationReader.schema = materializationWriter.getStorage.getSchema
    val matReaderOutputSchema = materializationReader.getOutputSchemas(Array())
    val matReaderOp =
      materializationReader.getPhysicalOp(
        context.executionId,
        OperatorSchemaInfo(Array(), matReaderOutputSchema)
      )

    // create 2 links for materialization
    val readerToDestLink = PhysicalLink(matReaderOp, 0, toOp, toInputPortIdx)
    val sourceToWriterLink =
      PhysicalLink(fromOp, fromOutputPortIdx, matWriterOp, 0)

    // add the pair to the map for later adding edges between 2 regions.
    writerReaderPairs(matWriterOp.id) = matReaderOp.id

    physicalPlan
      .removeLink(physicalLink)
      .addOperator(matWriterOp)
      .addOperator(matReaderOp)
      .addLink(readerToDestLink)
      .addLink(sourceToWriterLink)
      .setOperatorUnblockPort(toOp.id, toInputPortIdx)
      .populatePartitioningOnLinks()

  }

}
