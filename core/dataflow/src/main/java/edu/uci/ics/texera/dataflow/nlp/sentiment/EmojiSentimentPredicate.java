package edu.uci.ics.texera.dataflow.nlp.sentiment;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

/**
 * Created by Vinay on 22-05-2017.
 */
public class EmojiSentimentPredicate extends PredicateBase {
    private final String inputAttributeName;
    private final String resultAttributeName;

    public EmojiSentimentPredicate(
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAME, required = true)
            String inputAttributeName,
            @JsonProperty(value = PropertyNameConstants.RESULT_ATTRIBUTE_NAME, required = true)
            String resultAttributeName
    ) {
        this.inputAttributeName = inputAttributeName;
        this.resultAttributeName = resultAttributeName;
    }

    @JsonProperty(PropertyNameConstants.ATTRIBUTE_NAME)
    public String getInputAttributeName() {
        return this.inputAttributeName;
    }

    @JsonProperty(PropertyNameConstants.RESULT_ATTRIBUTE_NAME)
    public String getResultAttributeName() {
        return this.resultAttributeName;
    }

    @Override
    public EmojiSentimentOperator newOperator() {
        return new EmojiSentimentOperator(this);
    }
}
