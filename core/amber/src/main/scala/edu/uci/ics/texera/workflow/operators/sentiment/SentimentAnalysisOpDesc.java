package edu.uci.ics.texera.workflow.operators.sentiment;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.Preconditions;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaDefault;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.operators.map.MapOpDesc;
import edu.uci.ics.texera.workflow.common.operators.map.MapOpExecConfig;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import scala.collection.Seq;

public class SentimentAnalysisOpDesc extends MapOpDesc {

    @JsonProperty("attribute")
    @JsonPropertyDescription("column to perform sentiment analysis on")
    public String attribute;

    @JsonProperty("result attribute")
    @JsonPropertyDescription("column name of the sentiment analysis result")
    @JsonSchemaDefault("sentiment")
    public String resultAttribute;

    @Override
    public MapOpExecConfig operatorExecutor() {
        if (attribute == null) {
            throw new RuntimeException("sentiment analysis: attribute is null");
        }
        return new MapOpExecConfig(operatorIdentifier(), () -> new SentimentAnalysisOpExec(this));
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
    public Schema getOutputSchema(Seq<Schema> schemas) {
        Preconditions.checkArgument(schemas.length() == 1);
        if (resultAttribute == null) {
            return null;
        }
        return Schema.newBuilder().add(schemas.apply(0)).add(resultAttribute, AttributeType.STRING).build();
    }
}
