package edu.uci.ics.texera.workflow.operators.source.scan.json

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.UseAll
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LayerIdentity, OperatorIdentity}
import edu.uci.ics.amber.engine.operators.OpExecConfig

import java.io.{BufferedReader, FileReader}

class JSONLScanSourceOpExecConfig(val tag: OperatorIdentity, val desc: JSONLScanSourceOpDesc)
    extends OpExecConfig(tag) {
    override lazy val topology: Topology = {
        val numWorkers = Constants.defaultNumWorkers
        val reader = new BufferedReader(new FileReader(desc.filePath.get))
        var totalLines = 0
        while ( {
            reader.readLine != null
        }) totalLines += 1
        reader.close()

        new Topology(
            Array(
                new WorkerLayer(
                    LayerIdentity(tag, "main"),
                    i => {
                        val startOffset: Long = totalLines / numWorkers * i
                        val endOffset: Long =
                            if (i != numWorkers - 1) totalLines / numWorkers * (i + 1) else totalLines
                        new JSONLScanSourceOpExec(desc, startOffset, endOffset)
                    },
                    numWorkers,
                    UseAll(), // it's source operator
                    RoundRobinDeployment()
                )
            ),
            Array()
        )
    }

    override def assignBreakpoint(breakpoint: GlobalBreakpoint[_]): Array[ActorVirtualIdentity] = {
        topology.layers(0).identifiers
    }
}
