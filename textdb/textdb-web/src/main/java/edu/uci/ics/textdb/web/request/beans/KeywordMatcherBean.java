package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.plangen.operatorbuilder.KeywordMatcherBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.HashMap;

/**
 * This class defines the properties/data members specific to the KeywordMatcher operator
 * and extends the OperatorBean class which defines the data members general to all operators
 * Created by kishorenarendran on 11/09/16.
 */
@JsonTypeName("KeywordMatcher")
public class KeywordMatcherBean extends OperatorBean {
    @JsonProperty("keyword")
    private String keyword;
    @JsonProperty("matching_type")
    private String matchingType;

    public KeywordMatcherBean() {
    }

    public KeywordMatcherBean(String operatorID, String operatorType, String attributes, String limit,
                              String offset, String keyword, String matchingType) {
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
    public String getMatchingType() {
        return matchingType;
    }

    @JsonProperty("matching_type")
    public void setMatchingType(String matchingType) {
        this.matchingType = matchingType;
    }

    public HashMap<String, String> getOperatorProperties() {
        HashMap<String, String> operatorProperties = super.getOperatorProperties();
        if(this.getKeyword() == null || this.getMatchingType() == null || operatorProperties == null)
            return null;
        operatorProperties.put(KeywordMatcherBuilder.KEYWORD, this.getKeyword());
        operatorProperties.put(KeywordMatcherBuilder.MATCHING_TYPE, this.getMatchingType());
        return operatorProperties;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) return false;
        if (other == this) return true;
        if (!(other instanceof OperatorBean)) return false;
        KeywordMatcherBean keywordMatcherBean = (KeywordMatcherBean) other;
        return new EqualsBuilder()
                .append(keyword, keywordMatcherBean.getKeyword())
                .append(matchingType, keywordMatcherBean.getMatchingType())
                .isEquals() &&
                super.equals(keywordMatcherBean);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31)
                .append(keyword)
                .append(matchingType)
                .toHashCode();
    }
}