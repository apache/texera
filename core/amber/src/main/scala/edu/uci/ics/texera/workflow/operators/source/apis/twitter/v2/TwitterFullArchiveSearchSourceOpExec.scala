package edu.uci.ics.texera.workflow.operators.source.apis.twitter.v2

import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  AttributeTypeUtils,
  OperatorSchemaInfo
}
import edu.uci.ics.texera.workflow.operators.source.apis.twitter.TwitterSourceOpExec
import io.github.redouane59.twitter.dto.endpoints.AdditionalParameters
import io.github.redouane59.twitter.dto.tweet.TweetList
import io.github.redouane59.twitter.dto.tweet.TweetV2.TweetData
import io.github.redouane59.twitter.dto.user.UserV2.UserData

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId, ZoneOffset}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable.ListBuffer
import scala.collection.{Iterator, mutable}
import scala.jdk.CollectionConverters.asScalaBufferConverter

class TwitterFullArchiveSearchSourceOpExec(
    desc: TwitterFullArchiveSearchSourceOpDesc,
    operatorSchemaInfo: OperatorSchemaInfo
) extends TwitterSourceOpExec(desc.apiKey, desc.apiSecretKey, desc.stopWhenRateLimited) {
  val outputSchemaAttributes: Array[AttributeType] = operatorSchemaInfo.outputSchema.getAttributes
    .map((attribute: Attribute) => {
      attribute.getType
    })
    .toArray
  var curLimit: Int = desc.limit
  // nextToken is used to retrieve next page of results, if exists.
  var nextToken: String = _
  // contains tweets from the previous request.
  var tweetCache: mutable.Buffer[TweetData] = mutable.Buffer()
  var userCache: Map[String, UserData] = Map()
  var hasNextRequest: Boolean = curLimit > 0
  var lastQueryTime: Long = 0

  override def produceTexeraTuple(): Iterator[Tuple] =
    new Iterator[Tuple]() {
      override def hasNext: Boolean = (hasNextRequest || tweetCache.nonEmpty) && curLimit > 0

      override def next: Tuple = {
        // if the current cache is exhausted, query for the next response
        if (tweetCache.isEmpty && hasNextRequest) {
          queryForNextBatch(
            desc.searchQuery,
            LocalDateTime.parse(desc.fromDateTime, DateTimeFormatter.ISO_DATE_TIME),
            LocalDateTime.parse(desc.toDateTime, DateTimeFormatter.ISO_DATE_TIME),
            curLimit.min(TWITTER_API_BATCH_SIZE_MAX)
          )
        }

        // if the request is emtpy, it indicates no more tweets, iterator should stop
        if (tweetCache.isEmpty) {
          return null
        }
        val tweet: TweetData = tweetCache.remove(0)

        curLimit -= 1

        // if limit is 0, then no more requests should be sent
        if (curLimit == 0) {
          hasNextRequest = false
        }

        val user = userCache.get(tweet.getAuthorId)

        val fields = AttributeTypeUtils.parseFields(
          Array[Object](
            tweet.getId,
            tweet.getText,
            // given the fact that the redouane59/twittered library is using LocalDateTime as the API parameter,
            // we have to fix it to UTC time zone to normalize the time.
            tweet.getCreatedAt
              .atZone(ZoneId.systemDefault())
              .withZoneSameInstant(ZoneId.of("UTC"))
              .toLocalDateTime
              .atOffset(ZoneOffset.UTC)
              .format(DateTimeFormatter.ISO_DATE_TIME),
            tweet.getLang,
            tweet.getTweetType.toString,
            // TODO: add actual geo related information
            Option(tweet.getGeo).map(_.getPlaceId).orNull,
            Option(tweet.getGeo).map(_.getCoordinates).orNull,
            tweet.getInReplyToStatusId,
            tweet.getInReplyToUserId,
            java.lang.Long.valueOf(tweet.getLikeCount),
            java.lang.Long.valueOf(tweet.getQuoteCount),
            java.lang.Long.valueOf(tweet.getReplyCount),
            java.lang.Long.valueOf(tweet.getRetweetCount),
            Option(tweet.getEntities)
              .map(e => Option(e.getHashtags).map(_.map(x => x.getText).mkString(",")).orNull)
              .orNull,
            Option(tweet.getEntities)
              .map(e => Option(e.getSymbols).map(_.map(x => x.getText).mkString(",")).orNull)
              .orNull,
            Option(tweet.getEntities)
              .map(e => Option(e.getUrls).map(_.map(x => x.getExpandedUrl).mkString(",")).orNull)
              .orNull,
            Option(tweet.getEntities)
              .map(e => Option(e.getUserMentions).map(_.map(x => x.getText).mkString(",")).orNull)
              .orNull,
            user.get.getId,
            user.get.getCreatedAt,
            user.get.getName,
            user.get.getDisplayedName,
            user.get.getLang,
            user.get.getDescription,
            Option(user.get.getPublicMetrics)
              .map(u => java.lang.Long.valueOf(u.getFollowersCount))
              .orNull,
            Option(user.get.getPublicMetrics)
              .map(u => java.lang.Long.valueOf(u.getFollowingCount))
              .orNull,
            Option(user.get.getPublicMetrics)
              .map(u => java.lang.Long.valueOf(u.getTweetCount))
              .orNull,
            Option(user.get.getPublicMetrics)
              .map(u => java.lang.Long.valueOf(u.getListedCount))
              .orNull,
            user.get.getLocation,
            user.get.getUrl,
            user.get.getProfileImageUrl,
            user.get.getPinnedTweetId,
            Boolean.box(user.get.isProtectedAccount),
            Boolean.box(user.get.isVerified)
          ),
          outputSchemaAttributes
        )
        Tuple.newBuilder(operatorSchemaInfo.outputSchema).addSequentially(fields).build
      }
    }

  private def queryForNextBatch(
      query: String,
      startDateTime: LocalDateTime,
      endDateTime: LocalDateTime,
      maxResults: Int
  ): Unit = {
    def enforceRateLimit(): Unit = {
      // Twitter limit 1 request per second and 300 calls in 15 minutes for V2 FullArchiveSearch
      // If request too frequently, twitter will force the client wait for 5 minutes.
      // Here we send at most 1 request per second to avoid hitting rate limit.
      val currentTime = System.currentTimeMillis()

      // using 1100 to avoid some edge cases
      if (currentTime - lastQueryTime < 1100) {
        Thread.sleep(currentTime - lastQueryTime)
      }
      lastQueryTime = System.currentTimeMillis()
    }

    val params = AdditionalParameters
      .builder()
      .startTime(startDateTime)
      .endTime(endDateTime)
      .maxResults(maxResults.max(TWITTER_API_BATCH_SIZE_MIN))
      .recursiveCall(false)
      .nextToken(nextToken)
      .build()

    // There is bug in the library twittered that it returns null although there exists
    // more pages.
    // Below is a temporary patch to make sure the query stops when there are actually
    // no more pages. The strategy is to repeat the last request multiple times to ensure
    // it returns the nextToken as null. The solution is not ideal but should do job in
    // the most cases.
    // TODO: replace with newer version library twittered when the bug is fixed.
    var response: TweetList = null
    var retry = 2
    do {
      enforceRateLimit()
      response = twitterClient.searchAllTweets(query, params)
      retry -= 1

      if (response == null || response.getMeta == null) {
        // Error in request, result in null responses
        throw new RuntimeException("error in requesting Twitter API, please check your query.")

      }
    } while (response.getMeta.getNextToken == null && retry > 0)

    nextToken = response.getMeta.getNextToken

    tweetCache =
      if (response != null && response.getData != null) response.getData.asScala else ListBuffer()

    userCache =
      if (response != null && response.getIncludes != null && response.getIncludes.getUsers != null)
        response.getIncludes.getUsers
          .map((userData: UserData) => userData.getId -> userData)
          .toMap
      else Map()

    // when there is no more pages left, no need to request any more
    hasNextRequest = nextToken != null

  }

}
