package edu.uci.ics.texera.workflow.operators.sentiment;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.Preconditions;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.operators.OneToOneOpExecConfig;
import edu.uci.ics.texera.workflow.common.operators.map.MapOpDesc;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;

public class SentimentAnalysisOpDesc extends MapOpDesc {

    @JsonProperty(value = "attribute", required = true)
    @JsonPropertyDescription("column to perform sentiment analysis on")
    public String attribute;

    @JsonProperty(value = "result attribute", required = true, defaultValue = "sentiment")
    @JsonPropertyDescription("column name of the sentiment analysis result")
    public String resultAttribute;

    @Override
    public OneToOneOpExecConfig operatorExecutor() {
        if (attribute == null) {
            throw new RuntimeException("sentiment analysis: attribute is null");
        }
        return new OneToOneOpExecConfig(operatorIdentifier(), worker -> new SentimentAnalysisOpExec(this));
    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo(
                "Sentiment Analysis",
                "analysis the sentiment of a text using machine learning",
                OperatorGroupConstants.ANALYTICS_GROUP(),
                1, 1
        );
    }

    @Override
    public Schema getOutputSchema(Schema[] schemas) {
        Preconditions.checkArgument(schemas.length == 1);
        if (resultAttribute == null || resultAttribute.trim().isEmpty()) {
            return null;
        }
        return Schema.newBuilder().add(schemas[0]).add(resultAttribute, AttributeType.INTEGER).build();
    }
}
