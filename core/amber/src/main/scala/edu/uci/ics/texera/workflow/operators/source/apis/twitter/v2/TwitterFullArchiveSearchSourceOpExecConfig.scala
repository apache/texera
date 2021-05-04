package edu.uci.ics.texera.workflow.operators.source.apis.twitter.v2

import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity
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
    toDateTime: String,
    limit: Int
) extends TwitterSourceOpExecConfig(
      operatorIdentifier,
      numWorkers,
      new TwitterFullArchiveSearchSourceOpExec(
        schema,
        accessToken,
        accessTokenSecret,
        apiKey,
        apiSecretKey,
        searchQuery,
        fromDateTime,
        toDateTime,
        limit
      )
    ) {}
