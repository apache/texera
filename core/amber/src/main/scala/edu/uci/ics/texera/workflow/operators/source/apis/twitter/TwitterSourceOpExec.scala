package edu.uci.ics.texera.workflow.operators.source.apis.twitter
import com.github.redouane59.twitter.TwitterClient
import com.github.redouane59.twitter.signature.TwitterCredentials
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor

abstract class TwitterSourceOpExec(
    accessToken: String,
    accessTokenSecret: String,
    apiKey: String,
    apiSecretKey: String
) extends SourceOperatorExecutor {
  // batch size for each API request, 500 is the maximum tweets for each request defined by Twitter
  val TWITTER_API_BATCH_SIZE = 500

  var twitterClient: TwitterClient = _

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
}
