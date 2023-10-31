package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.texera.workflow.common.{ProgressiveUtils, WorkflowContext}
import edu.uci.ics.texera.workflow.common.operators.consolidate.ConsolidateOpDesc

import scala.collection.mutable.ArrayBuffer

object ProgressiveRetractionEnforcer {

  def enforceDelta(logicalPlan: LogicalPlan, context: WorkflowContext): LogicalPlan = {
    // first find the edges that we need to add the consolidate operator
    val edgesToAddConsolidateOp = new ArrayBuffer[OperatorLink]()
    logicalPlan.outputSchemaMap.foreach(kv => {
      val op = kv._1
      val outSchemas = kv._2
      logicalPlan
        .getDownstreamEdges(op.operator)
        .zip(outSchemas)
        .foreach(out => {
          val outEdge = out._1
          val outSchema = out._2
          if (outSchema.containsAttribute(ProgressiveUtils.insertRetractFlagAttr.getName)) {
            val downstreamOp = logicalPlan.getOperator(outEdge.destination.operatorID)
            if (!downstreamOp.operatorInfo.supportRetractableInput) {
              edgesToAddConsolidateOp.append(outEdge)
            }
          }
        })
    })

    var resultPlan = logicalPlan
    edgesToAddConsolidateOp.foreach(edge => {
      val newOp = new ConsolidateOpDesc()
      newOp.setContext(context)
      resultPlan = resultPlan.removeEdge(edge)
      resultPlan = resultPlan
        .addOperator(newOp)
        .addEdge(edge.origin.operatorID, newOp.operatorID, edge.origin.portOrdinal, 0)
        .addEdge(newOp.operatorID, edge.destination.operatorID, 0, edge.destination.portOrdinal)
    })

    resultPlan
  }

}
