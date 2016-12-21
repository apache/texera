package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.plangen.operatorbuilder.DictionaryMatcherBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.HashMap;

/**
 * This class defines the properties/data members specific to the DictionaryMatcher operator
 * and extends the OperatorBean class which defines the data members general to all operators
 * Created by kishorenarendran on 10/17/16.
 */
@JsonTypeName("DictionaryMatcher")
public class DictionaryMatcherBean extends OperatorBean {
    @JsonProperty("dictionary")
    private String dictionary;
    @JsonProperty("matching_type")
    private String matchingType;

    public DictionaryMatcherBean() {
    }

    public DictionaryMatcherBean(String operatorID, String operatorType, String attributes, String limit, String offset,
                                 String dictionary, String matchingType) {
        super(operatorID, operatorType, attributes, limit, offset);
        this.dictionary = dictionary;
        this.matchingType = matchingType;
    }

    @JsonProperty("dictionary")
    public String getDictionary() {
        return dictionary;
    }

    @JsonProperty("dictionary")
    public void setDictionary(String dictionary) {
        this.dictionary = dictionary;
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
        if(this.getDictionary() == null || this.getMatchingType() == null || operatorProperties == null)
            return null;
        operatorProperties.put(DictionaryMatcherBuilder.DICTIONARY, this.getDictionary());
        operatorProperties.put(DictionaryMatcherBuilder.MATCHING_TYPE, this.getMatchingType());
        return operatorProperties;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) return false;
        if (other == this) return true;
        if (!(other instanceof DictionaryMatcherBean)) return false;
        DictionaryMatcherBean dictionaryMatcherBean = (DictionaryMatcherBean) other;

        return new EqualsBuilder()
                .appendSuper(super.equals(dictionaryMatcherBean))
                .append(dictionary, dictionaryMatcherBean.getDictionary())
                .append(matchingType, dictionaryMatcherBean.getMatchingType())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31)
                .append(super.hashCode())
                .append(dictionary)
                .append(matchingType)
                .toHashCode();
    }
}