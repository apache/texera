package edu.uci.ics.texera.workflow.operators.source.apis.twitter
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import io.github.redouane59.twitter.TwitterClient
import io.github.redouane59.twitter.signature.TwitterCredentials

abstract class TwitterSourceOpExec(
    apiKey: String,
    apiSecretKey: String,
    stopWhenRateLimited: Boolean
) extends SourceOperatorExecutor {
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
        .apiKey(apiKey)
        .apiSecretKey(apiSecretKey)
        .build()
    )
    twitterClient.setAutomaticRetry(!stopWhenRateLimited)
  }

  override def close(): Unit = {}
}
