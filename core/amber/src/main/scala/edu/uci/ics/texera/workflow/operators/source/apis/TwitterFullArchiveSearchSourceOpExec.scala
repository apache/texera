package edu.uci.ics.texera.workflow.operators.source.apis
import com.github.redouane59.twitter.TwitterClient
import com.github.redouane59.twitter.dto.tweet.Tweet
import com.github.redouane59.twitter.signature.TwitterCredentials
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeTypeUtils, Schema}
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.{mutable, Iterator}
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
class TwitterFullArchiveSearchSourceOpExec(
    schema: Schema,
    accessToken: String,
    accessTokenSecret: String,
    apiKey: String,
    apiSecretKey: String,
    searchQuery: String
) extends SourceOperatorExecutor {
  var twitterClient: TwitterClient = null
  var nextToken: String = null
  var tweetCache: mutable.Queue[Tweet] = mutable.Queue()
  var limit = 10
  var current = 0
  override def open(): Unit = {
    twitterClient = new TwitterClient(
      TwitterCredentials
        .builder()
        .accessToken(accessToken)
        .accessTokenSecret(accessTokenSecret)
        .apiKey(apiKey)
        .apiSecretKey(apiSecretKey)
        .build()
    )
  }
  override def close(): Unit = {}
  override def produceTexeraTuple(): Iterator[Tuple] =
    new Iterator[Tuple]() {
      override def hasNext: Boolean = current < limit

      override def next: Tuple = {
        if (tweetCache.isEmpty) {
          val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")
          val startDateTime = LocalDateTime.parse("2021-04-08 00:00", formatter)
          val endDateTime = LocalDateTime.parse("2021-04-09 00:00", formatter)
          val query = searchQuery
          queryForNextBatch(query, startDateTime, endDateTime, 100)
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

  def queryForNextBatch(
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
