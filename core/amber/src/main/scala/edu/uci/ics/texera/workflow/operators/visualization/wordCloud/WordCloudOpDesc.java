package edu.uci.ics.texera.workflow.operators.visualization.wordCloud;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.uci.ics.amber.engine.common.Constants;
import edu.uci.ics.amber.engine.operators.OpExecConfig;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import edu.uci.ics.texera.workflow.operators.visualization.VisualizationConstants;
import edu.uci.ics.texera.workflow.operators.visualization.VisualizationOperator;

/**
 * WordCloud is a visualization operator that can be used by the caller to generate data for wordcloud.js in frontend.
 * WordCloud returns tuples with word (String) and its font size (Integer) for frontend.
 * @author Mingji Han, Xiaozhen Liu
 *
 */

public class WordCloudOpDesc extends VisualizationOperator {
    @JsonProperty(value = "text column", required = true)
    public String textColumn;

    @JsonProperty(value = "lucene analyzer name", required = true, defaultValue = "standard")
    public String luceneAnalyzerName;

    @Override
    public String chartType() {
        return VisualizationConstants.WORD_CLOUD;
    }

    @Override
    public OpExecConfig operatorExecutor() {
        return new WordCloudOpExecConfig(this.operatorIdentifier(), Constants.defaultNumWorkers(), textColumn, luceneAnalyzerName);
    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo("Word Cloud",
                "Generate word cloud for result texts",
                OperatorGroupConstants.VISUALIZATION_GROUP(),
                1, 1);
    }

    @Override
    public Schema getOutputSchema(Schema[] schemas) {
        return Schema.newBuilder().add(
                new Attribute("word", AttributeType.STRING),
                new Attribute("size", AttributeType.INTEGER)
        ).build();
    }
}
