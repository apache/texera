package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.plangen.operatorbuilder.DictionaryMatcherBuilder;

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
    private DataConstants.KeywordMatchingType matchingType;

    public DictionaryMatcherBean() {
    }

    public DictionaryMatcherBean(String operatorID, String operatorType, String attributes, String limit, String offset,
                                 String dictionary, DataConstants.KeywordMatchingType matchingType) {
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
    public DataConstants.KeywordMatchingType getMatchingType() {
        return matchingType;
    }

    @JsonProperty("matching_type")
    public void setMatchingType(DataConstants.KeywordMatchingType matchingType) {
        this.matchingType = matchingType;
    }

    public HashMap<String, String> getOperatorProperties() {
        HashMap<String, String> operatorProperties = super.getOperatorProperties();
        operatorProperties.put(DictionaryMatcherBuilder.DICTIONARY, this.getDictionary());
        operatorProperties.put(DictionaryMatcherBuilder.MATCHING_TYPE, this.getMatchingType().name());
        return operatorProperties;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) return false;
        if (other == this) return true;
        if (!(other instanceof OperatorBean)) return false;
        DictionaryMatcherBean dictionaryMatcherBean = (DictionaryMatcherBean) other;
        return super.equals(other) &&
                this.getDictionary().equals(dictionaryMatcherBean.getDictionary()) &&
                this.getMatchingType().name().equals(dictionaryMatcherBean.getMatchingType().name());
    }
}
