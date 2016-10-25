package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.plangen.operatorbuilder.KeywordMatcherBuilder;

import java.util.HashMap;

/**
 * This class defines the properties/data members specific to the KeywordMatcher operator
 * and extends the OperatorBean class which defines the data members general to all operators
 * Created by kishorenarendran on 10/17/16.
 */
@JsonTypeName("KeywordMatcher")
public class KeywordMatcherBean extends OperatorBean {
    @JsonProperty("keyword")
    private String keyword;
    @JsonProperty("matching_type")
    private KeywordMatchingType matchingType;

    public KeywordMatcherBean() {
    }

    public KeywordMatcherBean(String operatorID, String operatorType, String attributes, String limit,
                              String offset, String keyword, KeywordMatchingType matchingType) {
        super(operatorID, operatorType, attributes, limit, offset);
        this.keyword = keyword;
        this.matchingType = matchingType;
    }

    @JsonProperty("keyword")
    public String getKeyword() {
        return keyword;
    }

    @JsonProperty("keyword")
    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    @JsonProperty("matching_type")
    public KeywordMatchingType getMatchingType() {
        return matchingType;
    }

    @JsonProperty("matching_type")
    public void setMatchingType(KeywordMatchingType matchingType) {
        this.matchingType = matchingType;
    }

    public HashMap<String, String> getOperatorProperties() {
        HashMap<String, String> operatorProperties = super.getOperatorProperties();
        operatorProperties.put(KeywordMatcherBuilder.KEYWORD, this.getKeyword());
        operatorProperties.put(KeywordMatcherBuilder.MATCHING_TYPE, this.getMatchingType().name());
        return operatorProperties;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) return false;
        if (other == this) return true;
        if (!(other instanceof OperatorBean)) return false;
        KeywordMatcherBean keywordMatcherBean = (KeywordMatcherBean) other;
        return super.equals(other) &&
                this.getKeyword().equals(keywordMatcherBean.getKeyword()) &&
                this.getMatchingType().name().equals(keywordMatcherBean.getMatchingType().name());
    }
}
