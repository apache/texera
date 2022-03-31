package edu.uci.ics.texera.workflow.operators.sentiment;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.Preconditions;
import edu.uci.ics.amber.engine.common.Constants;
import edu.uci.ics.texera.workflow.common.metadata.InputPort;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.metadata.OutputPort;
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName;
import edu.uci.ics.texera.workflow.common.operators.OneToOneOpExecConfig;
import edu.uci.ics.texera.workflow.common.operators.map.MapOpDesc;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo;

import static java.util.Collections.singletonList;
import static scala.collection.JavaConverters.asScalaBuffer;

public class SentimentAnalysisOpDesc extends MapOpDesc {

    @JsonProperty(value = "attribute", required = true)
    @JsonPropertyDescription("column to perform sentiment analysis on")
    @AutofillAttributeName
    public String attribute;

    @JsonProperty(value = "result attribute", required = true, defaultValue = "sentiment")
    @JsonPropertyDescription("column name of the sentiment analysis result")
    public String resultAttribute;

    @Override
    public OneToOneOpExecConfig operatorExecutor(OperatorSchemaInfo operatorSchemaInfo) {
        if (attribute == null) {
            throw new RuntimeException("sentiment analysis: attribute is null");
        }
        return new OneToOneOpExecConfig(operatorIdentifier(), worker -> new SentimentAnalysisOpExec(this, operatorSchemaInfo), Constants.currentWorkerNum());
    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo(
                "Sentiment Analysis",
                "analysis the sentiment of a text using machine learning",
                OperatorGroupConstants.ANALYTICS_GROUP(),
                asScalaBuffer(singletonList(new InputPort("", false))).toList(),
                asScalaBuffer(singletonList(new OutputPort(""))).toList());
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
