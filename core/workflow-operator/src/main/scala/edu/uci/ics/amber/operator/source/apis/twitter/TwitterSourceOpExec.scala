package edu.uci.ics.amber.operator.source.apis.twitter

import edu.uci.ics.amber.core.executor.SourceOperatorExecutor
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import io.github.redouane59.twitter.TwitterClient
import io.github.redouane59.twitter.signature.TwitterCredentials

abstract class TwitterSourceOpExec(
    descString: String
) extends SourceOperatorExecutor {
  private val desc: TwitterSourceOpDesc =
    objectMapper.readValue(descString, classOf[TwitterSourceOpDesc])
  // batch size for each API request defined by Twitter
  //  500 is the maximum tweets for each request
  val TWITTER_API_BATCH_SIZE_MAX = 500

  //  10 is the minimal tweets for each request
  // val TWITTER_API_BATCH_SIZE_MIN = 10

  //  however, when using batch size < 100, could cause using different
  //  twitter endpoints which has different rate limit.
  //  (related to redouane59/twitteredV2.5)
  //  thus, in practice, we use 100 as the min batch size.
  val TWITTER_API_BATCH_SIZE_MIN = 100

  var twitterClient: TwitterClient = _

  override def open(): Unit = {
    twitterClient = new TwitterClient(
      TwitterCredentials
        .builder()
        .apiKey(desc.apiKey)
        .apiSecretKey(desc.apiSecretKey)
        .build()
    )
    twitterClient.setAutomaticRetry(!desc.stopWhenRateLimited)
  }

  override def close(): Unit = {}
}
