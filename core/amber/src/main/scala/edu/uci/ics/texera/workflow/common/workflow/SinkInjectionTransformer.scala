package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity
import edu.uci.ics.texera.workflow.operators.sink.SinkOpDesc
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.VisualizationOperator

object SinkInjectionTransformer {

  def transform(opsToViewResult: List[String], oldPlan: LogicalPlan): LogicalPlan = {
    var logicalPlan = oldPlan

    // for any terminal operator without a sink, add a sink
    val nonSinkTerminalOps = logicalPlan.getTerminalOperatorIds.filter(opId =>
      !logicalPlan.getOperator(opId).isInstanceOf[SinkOpDesc]
    )
    // for any operators marked as view result without a sink, add a sink
    val viewResultOps = opsToViewResult
      .map(idString => OperatorIdentity(idString))
      .filter(opId => !logicalPlan.getDownstreamOps(opId).exists(op => op.isInstanceOf[SinkOpDesc]))

    val operatorsToAddSink = (nonSinkTerminalOps ++ viewResultOps).toSet
    operatorsToAddSink.foreach(opId => {
      val op = logicalPlan.getOperator(opId)
      op.operatorInfo.outputPorts.indices.foreach(outPort => {
        val sink = new ProgressiveSinkOpDesc()
        logicalPlan = logicalPlan
          .addOperator(sink)
          .addLink(op.operatorIdentifier, outPort, sink.operatorIdentifier, toPort = 0)
      })
    })

    // check precondition: all the terminal operators should be sinks
    assert(
      logicalPlan.getTerminalOperatorIds.forall(o =>
        logicalPlan.getOperator(o).isInstanceOf[SinkOpDesc]
      )
    )

    // for each sink:
    // set the corresponding upstream ID and port
    // set output mode based on the visualization operator before it
    logicalPlan.getTerminalOperatorIds.foreach(sinkOpId => {
      val sinkOp = logicalPlan.getOperator(sinkOpId).asInstanceOf[ProgressiveSinkOpDesc]
      val upstream = logicalPlan.getUpstreamOps(sinkOpId).headOption
      val edge = logicalPlan.links.find(l =>
        l.origin.operatorId == upstream.map(_.operatorIdentifier).orNull
          && l.destination.operatorId == sinkOpId
      )
      assert(upstream.nonEmpty)
      if (upstream.nonEmpty && edge.nonEmpty) {
        // set upstream ID and port
        sinkOp.setUpstreamId(upstream.get.operatorIdentifier)
        sinkOp.setUpstreamPort(edge.get.origin.portOrdinal)

        // set output mode for visualization operator
        (upstream.get, sinkOp) match {
          // match the combination of a visualization operator followed by a sink operator
          case (viz: VisualizationOperator, sink: ProgressiveSinkOpDesc) =>
            sink.setOutputMode(viz.outputMode())
            sink.setChartType(viz.chartType())
          case _ =>
          //skip
        }
      }
    })

    logicalPlan
  }

}
