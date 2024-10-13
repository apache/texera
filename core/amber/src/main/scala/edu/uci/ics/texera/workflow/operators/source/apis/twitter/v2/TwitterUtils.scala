package edu.uci.ics.texera.workflow.operators.source.apis.twitter.v2

import edu.uci.ics.amber.engine.common.model.tuple.{Attribute, AttributeTypeUtils, Schema, Tuple}
import io.github.redouane59.twitter.dto.tweet.TweetV2.TweetData
import io.github.redouane59.twitter.dto.user.UserV2.UserData

import java.time.{ZoneId, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.jdk.CollectionConverters.IterableHasAsScala

object TwitterUtils {

  def tweetDataToTuple(tweetData: TweetData, user: Option[UserData], tweetSchema: Schema): Tuple = {
    val fields = AttributeTypeUtils.parseFields(
      Array[Any](
        tweetData.getId,
        tweetData.getText,
        // given the fact that the redouane59/twittered library is using LocalDateTime as the API parameter,
        // we have to fix it to UTC time zone to normalize the time.
        tweetData.getCreatedAt
          .atZone(ZoneId.systemDefault())
          .withZoneSameInstant(ZoneId.of("UTC"))
          .toLocalDateTime
          .atOffset(ZoneOffset.UTC)
          .format(DateTimeFormatter.ISO_DATE_TIME),
        tweetData.getLang,
        tweetData.getTweetType.toString,
        // TODO: add actual geo related information
        Option(tweetData.getGeo).map(_.getPlaceId).orNull,
        Option(tweetData.getGeo).map(_.getCoordinates).orNull,
        tweetData.getInReplyToStatusId,
        tweetData.getInReplyToUserId,
        java.lang.Long.valueOf(tweetData.getLikeCount),
        java.lang.Long.valueOf(tweetData.getQuoteCount),
        java.lang.Long.valueOf(tweetData.getReplyCount),
        java.lang.Long.valueOf(tweetData.getRetweetCount),
        Option(tweetData.getEntities)
          .map(e => Option(e.getHashtags).map(_.asScala.map(x => x.getText).mkString(",")).orNull)
          .orNull,
        Option(tweetData.getEntities)
          .map(e => Option(e.getSymbols).map(_.asScala.map(x => x.getText).mkString(",")).orNull)
          .orNull,
        Option(tweetData.getEntities)
          .map(e =>
            Option(e.getUrls).map(_.asScala.map(x => x.getExpandedUrl).mkString(",")).orNull
          )
          .orNull,
        Option(tweetData.getEntities)
          .map(e =>
            Option(e.getUserMentions).map(_.asScala.map(x => x.getText).mkString(",")).orNull
          )
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
      tweetSchema.getAttributes.map((attribute: Attribute) => { attribute.getType }).toArray
    )
    Tuple.builder(tweetSchema).addSequentially(fields).build()
  }
}
