package edu.uci.ics.texera.workflow.operators.source.apis
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.UseAll
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  OperatorIdentity
}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

class TwitterFullArchiveSearchSourceOpExecConfig(
    operatorIdentifier: OperatorIdentity,
    numWorkers: Int,
    schema: Schema,
    accessToken: String,
    accessTokenSecret: String,
    apiKey: String,
    apiSecretKey: String,
    searchQuery: String
) extends OpExecConfig(operatorIdentifier) {

  override lazy val topology: Topology = {
    new Topology(
      Array(
        new WorkerLayer(
          LayerIdentity(operatorIdentifier, "main"),
          _ => {
            new TwitterFullArchiveSearchSourceOpExec(
              schema,
              accessToken,
              accessTokenSecret,
              apiKey,
              apiSecretKey,
              searchQuery
            )
          },
          numWorkers,
          UseAll(), // it's source operator
          RoundRobinDeployment()
        )
      ),
      Array()
    )
  }

  override def assignBreakpoint(
      breakpoint: GlobalBreakpoint[_]
  ): Array[ActorVirtualIdentity] = {
    topology.layers(0).identifiers
  }
}
