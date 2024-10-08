package edu.uci.ics.texera.workflow.operators.source.apis.reddit

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyDescription
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.common.model.tuple.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.engine.common.workflow.OutputPort
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo
import edu.uci.ics.texera.workflow.common.operators.source.PythonSourceOperatorDescriptor

class RedditSearchSourceOpDesc extends PythonSourceOperatorDescriptor {
  @JsonProperty(required = true)
  @JsonSchemaTitle("Client Id")
  @JsonPropertyDescription("Client id that uses to access Reddit API")
  var clientId: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Client Secret")
  @JsonPropertyDescription("Client secret that uses to access Reddit API")
  var clientSecret: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Query")
  @JsonPropertyDescription("Search query")
  var query: String = _

  @JsonProperty(required = true, defaultValue = "100")
  @JsonSchemaTitle("Limit")
  @JsonPropertyDescription("Up to 1000")
  var limit: Integer = 100

  @JsonProperty(required = true, defaultValue = "none")
  @JsonSchemaTitle("Sorting")
  @JsonPropertyDescription("The sorting method, hot, new, etc.")
  var sorting: RedditSourceOperatorFunction = _

  override def generatePythonCode(): String = {
    val clientIdReal = this.clientId.replace("\n", "").trim
    val clientSecretReal = this.clientSecret.replace("\n", "").trim
    val queryReal = this.query.replace("\n", "").trim

    s"""from pytexera import *
        |import praw
        |from datetime import datetime
        |
        |class ProcessTupleOperator(UDFSourceOperator):
        |    client_id = '$clientIdReal'
        |    client_secret = '$clientSecretReal'
        |    limit = $limit
        |    query = '$queryReal'
        |    sorting = '${sorting.getName}'
        |
        |    @overrides
        |    def produce(self) -> Iterator[Union[TupleLike, TableLike, None]]:
        |        redditInstance = praw.Reddit(
        |            client_id=self.client_id,
        |            client_secret=self.client_secret,
        |            user_agent='chrome:reddit 0.0.0 (by /u/)'
        |        )
        |
        |        if len(self.client_id) == 0:
        |            raise ValueError('Client Id cannot be None.')
        |        
        |        if len(self.client_secret) == 0:
        |            raise ValueError('Client Secret cannot be None.')
        |        
        |        if len(self.query) == 0:
        |            raise ValueError('Query cannot be None.')
        |        
        |        if self.limit <= 0 or self.limit > 1000:
        |            raise ValueError('Limit should be larger than 0 and no more than 1000.')
        |        if self.sorting == 'none':
        |            submissions = redditInstance.subreddit('all').search(query=self.query, limit=self.limit)
        |        else:
        |            submissions = redditInstance.subreddit('all').search(query=self.query, limit=self.limit, sort=self.sorting)
        |        for submission in submissions:
        |            author = submission.author
        |            subreddit = str(submission.subreddit.display_name)
        |            edited = None
        |            if type(submission.edited) != type(True):
        |                edited = datetime.fromtimestamp(submission.edited)
        |            tuple_submission = Tuple({
        |                'id': submission.id,
        |                'name': submission.name,
        |                'title': submission.title,
        |                'created_utc': datetime.fromtimestamp(submission.created_utc),
        |                'edited': edited,
        |                'is_self': submission.is_self,
        |                'selftext': submission.selftext,
        |                'over_18': submission.over_18,
        |                'is_original_content': submission.is_original_content,
        |                'locked': submission.locked,
        |                'score': submission.score,
        |                'upvote_ratio': submission.upvote_ratio,
        |                'num_comments': submission.num_comments,
        |                'permalink': submission.permalink,
        |                'url': submission.url,
        |                'author_name': author.name,
        |                'subreddit': subreddit
        |            })
        |            yield tuple_submission""".stripMargin
  }
  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Reddit Search",
      "Search for recent posts with python-wrapped Reddit API, PRAW",
      OperatorGroupConstants.API_GROUP,
      inputPorts = List.empty,
      outputPorts = List(OutputPort())
    )
  override def asSource() = true
  override def sourceSchema(): Schema =
    Schema
      .builder()
      .add(
        new Attribute("id", AttributeType.STRING),
        new Attribute("name", AttributeType.STRING),
        new Attribute("title", AttributeType.STRING),
        new Attribute("created_utc", AttributeType.TIMESTAMP),
        new Attribute("edited", AttributeType.TIMESTAMP),
        new Attribute("is_self", AttributeType.BOOLEAN),
        new Attribute("selftext", AttributeType.STRING),
        new Attribute("over_18", AttributeType.BOOLEAN),
        new Attribute("is_original_content", AttributeType.BOOLEAN),
        new Attribute("locked", AttributeType.BOOLEAN),
        new Attribute("score", AttributeType.INTEGER),
        new Attribute("upvote_ratio", AttributeType.DOUBLE),
        new Attribute("num_comments", AttributeType.INTEGER),
        new Attribute("permalink", AttributeType.STRING),
        new Attribute("url", AttributeType.STRING),
        new Attribute("author_name", AttributeType.STRING),
        new Attribute("subreddit", AttributeType.STRING)
      )
      .build()
}
