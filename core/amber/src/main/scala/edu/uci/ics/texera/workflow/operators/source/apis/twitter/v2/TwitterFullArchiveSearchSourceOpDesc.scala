package edu.uci.ics.texera.workflow.operators.source.apis.twitter.v2

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty}
import com.kjetland.jackson.jsonSchema.annotations.{
  JsonSchemaDescription,
  JsonSchemaInject,
  JsonSchemaTitle
}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.annotations.UIWidget
import edu.uci.ics.texera.workflow.common.operators.ManyToOneOpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}
import edu.uci.ics.texera.workflow.operators.source.apis.twitter.TwitterSourceOpDesc

class TwitterFullArchiveSearchSourceOpDesc extends TwitterSourceOpDesc {

  @JsonIgnore
  override val APIName: Option[String] = Some("Full Archive Search")

  @JsonProperty(required = true)
  @JsonSchemaTitle("Search Query")
  @JsonSchemaDescription("Up to 1024 characters")
  @JsonSchemaInject(json = UIWidget.UIWidgetTextArea)
  var searchQuery: String = _

  @JsonProperty(required = true, defaultValue = "2021-04-01T00:00:00Z")
  @JsonSchemaTitle("From Datetime")
  @JsonSchemaDescription("ISO 8601 format")
  var fromDateTime: String = _

  @JsonProperty(required = true, defaultValue = "2021-05-01T00:00:00Z")
  @JsonSchemaTitle("To Datetime")
  @JsonSchemaDescription("ISO 8601 format")
  var toDateTime: String = _

  @JsonProperty(required = true, defaultValue = "100")
  @JsonSchemaTitle("Limit")
  @JsonSchemaDescription("Maximum number of tweets to retrieve")
  var limit: Int = _

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig =
    // TODO: use multiple workers
    new ManyToOneOpExecConfig(
      operatorIdentifier,
      _ => new TwitterFullArchiveSearchSourceOpExec(this, operatorSchemaInfo)
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
