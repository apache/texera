package edu.uci.ics.texera.workflow.operators.source.apis.twitter.v2

import com.github.redouane59.twitter.dto.tweet.Tweet
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeTypeUtils, Schema}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.source.apis.twitter.TwitterSourceOpExec

import java.time.LocalDateTime
import scala.collection.{mutable, Iterator}
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
class TwitterFullArchiveSearchSourceOpExec(
    schema: Schema,
    accessToken: String,
    accessTokenSecret: String,
    apiKey: String,
    apiSecretKey: String,
    searchQuery: String,
    fromDateTime: String,
    toDateTime: String
) extends TwitterSourceOpExec(accessToken, accessTokenSecret, apiKey, apiSecretKey) {

  var nextToken: String = _
  var tweetCache: mutable.Queue[Tweet] = mutable.Queue()
  var limit = 10
  var current = 0

  override def produceTexeraTuple(): Iterator[Tuple] =
    new Iterator[Tuple]() {
      override def hasNext: Boolean = current < limit

      override def next: Tuple = {
        if (tweetCache.isEmpty) {

          val query = searchQuery
          queryForNextBatch(
            query,
            AttributeTypeUtils.parseTimestamp(fromDateTime).toLocalDateTime,
            AttributeTypeUtils.parseTimestamp(toDateTime).toLocalDateTime,
            100
          )
        }
        val tweet: Tweet = tweetCache.dequeue()
        current += 1
        val fields = AttributeTypeUtils.parseFields(
          Array[Object](tweet.getId, tweet.getText),
          schema.getAttributes.map((attribute: Attribute) => { attribute.getType }).toArray
        )
        Tuple.newBuilder
          .add(schema, fields)
          .build
      }

    }

  private def queryForNextBatch(
      query: String,
      startDateTime: LocalDateTime,
      endDateTime: LocalDateTime,
      limit: Int
  ): Unit = {

    val response = twitterClient.searchForTweetsFullArchive(
      query,
      startDateTime,
      endDateTime,
      limit,
      nextToken
    )
    nextToken = response.getNextToken
    for (tweet <- response.getTweets.asScala) {
      tweetCache.enqueue(tweet)
    }
  }
}
