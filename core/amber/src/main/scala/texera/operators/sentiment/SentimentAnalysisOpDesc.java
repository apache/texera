package texera.operators.sentiment;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.Preconditions;
import scala.collection.Seq;
import texera.common.metadata.OperatorGroupConstants;
import texera.common.metadata.TexeraOperatorInfo;
import texera.common.operators.map.TexeraMapOpDesc;
import texera.common.operators.map.TexeraMapOpExecConfig;
import texera.common.tuple.schema.AttributeType;
import texera.common.tuple.schema.Schema;

public class SentimentAnalysisOpDesc extends TexeraMapOpDesc {

    @JsonProperty("attribute")
    @JsonPropertyDescription("column to perform sentiment analysis on")
    public String attribute;

    @JsonProperty("result attribute")
    @JsonPropertyDescription("column name of the sentiment analysis result")
    public String resultAttribute;

    @Override
    public TexeraMapOpExecConfig texeraOperatorExecutor() {
        if (attribute == null) {
            throw new RuntimeException("sentiment analysis: attribute is null");
        }
        return new TexeraMapOpExecConfig(operatorIdentifier(), () -> new SentimentAnalysisOpExec(this));
    }

    @Override
    public TexeraOperatorInfo texeraOperatorInfo() {
        return new TexeraOperatorInfo(
                "Sentiment Analysis",
                "analysis the sentiment of a text using machine learning",
                OperatorGroupConstants.ANALYTICS_GROUP(),
                1, 1
        );
    }

    @Override
    public Schema transformSchema(Seq<Schema> schemas) {
        Preconditions.checkArgument(schemas.length() == 1);
        if (resultAttribute == null) {
            return null;
        }
        return Schema.newBuilder().add(schemas.apply(0)).add(resultAttribute, AttributeType.STRING).build();
    }
}
