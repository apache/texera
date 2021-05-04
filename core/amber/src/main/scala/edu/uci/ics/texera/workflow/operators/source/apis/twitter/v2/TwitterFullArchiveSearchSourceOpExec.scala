package edu.uci.ics.texera.workflow.operators.source.apis.twitter.v2

import com.github.redouane59.twitter.dto.tweet.Tweet
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeTypeUtils, Schema}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.source.apis.twitter.TwitterSourceOpExec

import java.time.{LocalDateTime, ZoneOffset}
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
    toDateTime: String,
    var curLimit: Int
) extends TwitterSourceOpExec(accessToken, accessTokenSecret, apiKey, apiSecretKey) {

  // nextToken is used to retrieve next page of results, if exists.
  var nextToken: String = _

  // contains tweets from the previous request.
  var tweetCache: mutable.Buffer[Tweet] = mutable.Buffer()

  var hasNextRequest: Boolean = curLimit > 0

  override def produceTexeraTuple(): Iterator[Tuple] =
    new Iterator[Tuple]() {
      override def hasNext: Boolean = (hasNextRequest || tweetCache.nonEmpty) && curLimit > 0

      override def next: Tuple = {
        // if the current cache is exhausted, query for the next response
        if (tweetCache.isEmpty && hasNextRequest) {
          queryForNextBatch(
            searchQuery,
            AttributeTypeUtils.parseTimestamp(fromDateTime).toLocalDateTime,
            AttributeTypeUtils.parseTimestamp(toDateTime).toLocalDateTime,
            curLimit.min(TWITTER_API_BATCH_SIZE)
          )
        }

        // if the request is emtpy, it indicates no more tweets, iterator should stop
        if (tweetCache.isEmpty) {
          return null
        }
        val tweet: Tweet = tweetCache.remove(0)

        curLimit -= 1

        // if limit is 0, then no more requests should be sent
        if (curLimit == 0) {
          hasNextRequest = false
        }

        val fields = AttributeTypeUtils.parseFields(
          Array[Object](
            tweet.getId,
            tweet.getText,
            tweet.getCreatedAt.toInstant(ZoneOffset.UTC).toString,
            tweet.getAuthorId,
            // tweet.getUser, // currently unsupported by the twittered library
            // TODO: add user information
            tweet.getLang,
            tweet.getTweetType.toString,
            // TODO: add actual geo related information
            Option(tweet.getGeo).map(_.getPlaceId).orNull,
            Option(tweet.getGeo).map(_.getCoordinates).orNull,
            tweet.getInReplyToStatusId,
            tweet.getInReplyToUserId,
            Integer.valueOf(tweet.getLikeCount),
            Integer.valueOf(tweet.getQuoteCount),
            Integer.valueOf(tweet.getReplyCount),
            Integer.valueOf(tweet.getRetweetCount)
          ),
          schema.getAttributes.map((attribute: Attribute) => { attribute.getType }).toArray
        )
        Tuple.newBuilder.add(schema, fields).build
      }
    }

  private def queryForNextBatch(
      query: String,
      startDateTime: LocalDateTime,
      endDateTime: LocalDateTime,
      maxResults: Int
  ): Unit = {

    val response = twitterClient.searchForTweetsFullArchive(
      query,
      startDateTime,
      endDateTime,
      maxResults.max(10), // a request needs at least 10 results
      nextToken
    )

    nextToken = response.getNextToken

    // when there is no more pages left, no need to request any more
    hasNextRequest = nextToken != null

    tweetCache = response.getTweets.asScala
  }
}
