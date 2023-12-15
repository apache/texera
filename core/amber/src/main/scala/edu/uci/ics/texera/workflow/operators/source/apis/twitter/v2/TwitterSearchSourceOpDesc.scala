package edu.uci.ics.texera.workflow.operators.source.apis.twitter.v2

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty}
import com.kjetland.jackson.jsonSchema.annotations.{
  JsonSchemaDescription,
  JsonSchemaInject,
  JsonSchemaTitle
}
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.common.virtualidentity.ExecutionIdentity
import edu.uci.ics.texera.workflow.common.metadata.annotations.UIWidget
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}
import edu.uci.ics.texera.workflow.operators.source.apis.twitter.TwitterSourceOpDesc

class TwitterSearchSourceOpDesc extends TwitterSourceOpDesc {

  @JsonIgnore
  override val APIName: Option[String] = Some("Search")

  @JsonProperty(required = true)
  @JsonSchemaTitle("Search Query")
  @JsonSchemaDescription("Up to 1024 characters (Limited by Twitter)")
  @JsonSchemaInject(json = UIWidget.UIWidgetTextArea)
  var searchQuery: String = _

  @JsonProperty(required = true, defaultValue = "100")
  @JsonSchemaTitle("Limit")
  @JsonSchemaDescription("Maximum number of tweets to retrieve")
  var limit: Int = _

  override def getPhysicalOp(
      executionId: ExecutionIdentity,
      operatorSchemaInfo: OperatorSchemaInfo
  ): PhysicalOp =
    // TODO: use multiple workers
    PhysicalOp.sourcePhysicalOp(
      executionId,
      operatorIdentifier,
      OpExecInitInfo(_ => new TwitterSearchSourceOpExec(this, operatorSchemaInfo))
    )

  override def sourceSchema(): Schema = {

    // twitter schema is hard coded for now. V2 API has changed many fields of the Tweet object.
    // we are also currently depending on redouane59/twittered client library to parse tweet fields.

    Schema
      .newBuilder()
      .add(
        new Attribute("id", AttributeType.STRING),
        new Attribute("text", AttributeType.STRING),
        new Attribute("created_at", AttributeType.TIMESTAMP),
        new Attribute("lang", AttributeType.STRING),
        new Attribute("tweet_type", AttributeType.STRING),
        new Attribute("place_id", AttributeType.STRING),
        new Attribute("place_coordinate", AttributeType.STRING),
        new Attribute("in_reply_to_status_id", AttributeType.STRING),
        new Attribute("in_reply_to_user_id", AttributeType.STRING),
        new Attribute("like_count", AttributeType.LONG),
        new Attribute("quote_count", AttributeType.LONG),
        new Attribute("reply_count", AttributeType.LONG),
        new Attribute("retweet_count", AttributeType.LONG),
        new Attribute("hashtags", AttributeType.STRING),
        new Attribute("symbols", AttributeType.STRING),
        new Attribute("urls", AttributeType.STRING),
        new Attribute("mentions", AttributeType.STRING),
        new Attribute("user_id", AttributeType.STRING),
        new Attribute("user_created_at", AttributeType.TIMESTAMP),
        new Attribute("user_name", AttributeType.STRING),
        new Attribute("user_display_name", AttributeType.STRING),
        new Attribute("user_lang", AttributeType.STRING),
        new Attribute("user_description", AttributeType.STRING),
        new Attribute("user_followers_count", AttributeType.LONG),
        new Attribute("user_following_count", AttributeType.LONG),
        new Attribute("user_tweet_count", AttributeType.LONG),
        new Attribute("user_listed_count", AttributeType.LONG),
        new Attribute("user_location", AttributeType.STRING),
        new Attribute("user_url", AttributeType.STRING),
        new Attribute("user_profile_image_url", AttributeType.STRING),
        new Attribute("user_pinned_tweet_id", AttributeType.STRING),
        new Attribute("user_protected", AttributeType.BOOLEAN),
        new Attribute("user_verified", AttributeType.BOOLEAN)
      )
      .build()
  }
}
