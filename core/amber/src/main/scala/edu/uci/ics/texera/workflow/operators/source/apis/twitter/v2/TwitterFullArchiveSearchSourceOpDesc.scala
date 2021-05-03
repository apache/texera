package edu.uci.ics.texera.workflow.operators.source.apis.twitter.v2

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.texera.workflow.operators.source.apis.twitter.TwitterSourceOpDesc

class TwitterFullArchiveSearchSourceOpDesc extends TwitterSourceOpDesc {

  @JsonIgnore
  override val APIName: Option[String] = Some("Full Archive Search")

  @JsonProperty(required = true)
  @JsonSchemaTitle("Search Query")
  var searchQuery: String = _

  override def operatorExecutor: OpExecConfig =
    new TwitterFullArchiveSearchSourceOpExecConfig(
      operatorIdentifier,
      1, // here using 1 since there is no easy way to split the task for multi-line csv.
      sourceSchema(),
      accessToken,
      accessTokenSecret,
      apiKey,
      apiSecretKey,
      searchQuery
    )

  override def sourceSchema(): Schema =
    Schema
      .newBuilder()
      .add(new Attribute("id", AttributeType.LONG), new Attribute("text", AttributeType.STRING))
      .build()
}
