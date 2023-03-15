package edu.uci.ics.texera.workflow.operators.source.apis.reddit;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.metadata.OutputPort;
import edu.uci.ics.texera.workflow.common.operators.source.PythonSourceOperatorDescriptor;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;

import static java.util.Collections.singletonList;
import static scala.collection.JavaConverters.asScalaBuffer;


public class RedditSearchSourceOpDesc extends PythonSourceOperatorDescriptor {

    @JsonProperty(required = true)
    @JsonSchemaTitle("Client Id")
    @JsonPropertyDescription("Client id that uses to access Reddit API")
    public String clientId;

    @JsonProperty(required = true)
    @JsonSchemaTitle("Client Secret")
    @JsonPropertyDescription("Client secret that uses to access Reddit API")
    public String clientSecret;

    @JsonProperty(required = true)
    @JsonSchemaTitle("Query")
    @JsonPropertyDescription("Search query")
    public String query;

    @JsonProperty(required = true, defaultValue = "100")
    @JsonSchemaTitle("Limit")
    @JsonPropertyDescription("Up to 1000")
    public Integer limit;

    @JsonProperty(required = true, defaultValue = "none")
    @JsonSchemaTitle("Sorting")
    @JsonPropertyDescription("The sorting method, hot, new, etc.")
    public RedditSourceOperatorFunction sorting;

    @Override
    public String generatePythonCode(OperatorSchemaInfo operatorSchemaInfo) {

        String codeTemplate = "from pytexera import *\n" +
                "import praw\n" +
                "from datetime import datetime\n" +
                "\n" +
                "class ProcessTupleOperator(UDFOperator):\n" +
                "    client_id = _CLIENT_ID_\n" +
                "    client_secret = _CLIENT_SECRET_\n" +
                "    limit = _LIMIT_\n" +
                "    query = _QUERY_\n" +
                "    sorting = _SORTING_\n" +
                "\n" +
                "    @overrides\n" +
                "    def process_tuple(self, tuple_: Union[Tuple, InputExhausted], input_: int) -> Iterator[Optional[TupleLike]]:\n" +
                "        redditInstance = praw.Reddit(\n" +
                "            client_id=self.client_id,\n" +
                "            client_secret=self.client_secret,\n" +
                "            user_agent=\"chrome:reddit 0.0.0 (by /u/)\"\n" +
                "        )\n" +
                "\n" +
                "        if len(self.client_id) == 0:\n" +
                "            raise ValueError(\"Client Id cannot be None.\")\n" +
                "        \n" +
                "        if len(self.client_secret) == 0:\n" +
                "            raise ValueError(\"Client Secret cannot be None.\")\n" +
                "        \n" +
                "        if len(self.query) == 0:\n" +
                "            raise ValueError(\"Query cannot be None.\")\n" +
                "        \n" +
                "        if self.limit <= 0 or self.limit > 1000:\n" +
                "            raise ValueError(\"Limit should be larger than 0 and no more than 1000.\")\n" +
                "        if self.sorting == \"none\":\n" +
                "            submissions = redditInstance.subreddit(\"all\").search(query=self.query, limit=self.limit)\n" +
                "        else:\n" +
                "            submissions = redditInstance.subreddit(\"all\").search(query=self.query, limit=self.limit, sort=self.sorting)\n" +
                "        for submission in submissions:\n" +
                "            author = submission.author\n" +
                "            subreddit = str(submission.subreddit.display_name)\n" +
                "            edited = None\n" +
                "            if type(submission.edited) != type(True):\n" +
                "                edited = datetime.fromtimestamp(submission.edited)\n" +
                "            tuple_submission = Tuple({\n" +
                "                \"id\": submission.id,\n" +
                "                \"name\": submission.name,\n" +
                "                \"title\": submission.title,\n" +
                "                \"created_utc\": datetime.fromtimestamp(submission.created_utc),\n" +
                "                \"edited\": edited,\n" +
                "                \"is_self\": submission.is_self,\n" +
                "                \"selftext\": submission.selftext,\n" +
                "                \"over_18\": submission.over_18,\n" +
                "                \"is_original_content\": submission.is_original_content,\n" +
                "                \"locked\": submission.locked,\n" +
                "                \"score\": submission.score,\n" +
                "                \"upvote_ratio\": submission.upvote_ratio,\n" +
                "                \"num_comments\": submission.num_comments,\n" +
                "                \"permalink\": submission.permalink,\n" +
                "                \"url\": submission.url,\n" +
                "                \"author_name\": author.name,\n" +
                "                \"subreddit\": subreddit\n" +
                "            })\n" +
                "            yield tuple_submission";
        String clientIdReal = this.clientId.replace("\n", "").trim();
        String clientSecretReal = this.clientSecret.replace("\n", "").trim();
        String queryReal = this.query.replace("\n", "").trim();

        return codeTemplate
                .replace("_CLIENT_ID_", "\"" + clientIdReal + "\"")
                .replace("_CLIENT_SECRET_", "\"" + clientSecretReal + "\"")
                .replace("_LIMIT_", "" + this.limit)
                .replace("_QUERY_", "\"" + queryReal + "\"")
                .replace("_SORTING_", "\"" + this.sorting.getName() + "\"");
    }


    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo(
                "Reddit Search",
                "Search for recent posts with python-wrapped Reddit API, PRAW",
                OperatorGroupConstants.SOURCE_GROUP(),
                scala.collection.immutable.List.empty(),
                asScalaBuffer(singletonList(new OutputPort(""))).toList(),
                false,
                false,
                false
        );
    }

    @Override
    public int numWorkers() {
        return 1;
    }

    @Override
    public boolean asSource() {
        return true;
    }

    public Schema sourceSchema() {
        return Schema
                .newBuilder()
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
                .build();
    }
}
