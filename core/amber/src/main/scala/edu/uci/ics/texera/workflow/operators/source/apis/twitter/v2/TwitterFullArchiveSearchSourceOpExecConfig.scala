package edu.uci.ics.texera.workflow.operators.source.apis.twitter.v2

import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.UseAll
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.virtualidentity.{LayerIdentity, OperatorIdentity}
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.operators.source.apis.twitter.TwitterSourceOpExecConfig

class TwitterFullArchiveSearchSourceOpExecConfig(
    operatorIdentifier: OperatorIdentity,
    numWorkers: Int,
    schema: Schema,
    accessToken: String,
    accessTokenSecret: String,
    apiKey: String,
    apiSecretKey: String,
    searchQuery: String,
    fromDateTime: String,
    toDateTime: String
) extends TwitterSourceOpExecConfig(operatorIdentifier) {

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
              searchQuery,
              fromDateTime,
              toDateTime
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
}
