package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import edu.uci.ics.textdb.plangen.operatorbuilder.FuzzyTokenMatcherBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.HashMap;

/**
 * This class defines the properties/data members specific to the FuzzyTokenMatcher operator
 * and extends the OperatorBean class which defines the data members general to all operators
 * Created by kishorenarendran on 11/09/16.
 */
@JsonTypeName("FuzzyTokenMatcher")
public class FuzzyTokenMatcherBean extends OperatorBean {
    @JsonProperty("query")
    private String query;
    @JsonProperty("threshold_ratio")
    private String thresholdRatio;

    public FuzzyTokenMatcherBean() {
    }

    public FuzzyTokenMatcherBean(String operatorID, String operatorType, String attributes, String limit, String offset,
                                 String query, String thresholdRatio) {
        super(operatorID, operatorType, attributes, limit, offset);
        this.query = query;
        this.thresholdRatio = thresholdRatio;
    }

    @JsonProperty("query")
    public String getQuery() {
        return query;
    }

    @JsonProperty("query")
    public void setQuery(String query) {
        this.query = query;
    }

    @JsonProperty("threshold_ratio")
    public String getThresholdRatio() {
        return thresholdRatio;
    }

    @JsonProperty("threshold_ratio")
    public void setThresholdRatio(String thresholdRatio) {
        this.thresholdRatio = thresholdRatio;
    }

    @JsonIgnore
    public HashMap<String, String> getOperatorProperties() {
        HashMap<String, String> operatorProperties = super.getOperatorProperties();
        if(this.getQuery() == null || this.getThresholdRatio() == null || operatorProperties == null)
            return null;
        operatorProperties.put(FuzzyTokenMatcherBuilder.FUZZY_STRING, this.getQuery());
        operatorProperties.put(FuzzyTokenMatcherBuilder.THRESHOLD_RATIO, this.getThresholdRatio());
        return operatorProperties;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) return false;
        if (other == this) return true;
        if (!(other instanceof FuzzyTokenMatcherBean)) return false;
        FuzzyTokenMatcherBean fuzzyTokenMatcherBean = (FuzzyTokenMatcherBean) other;
        return new EqualsBuilder()
                .appendSuper(super.equals(fuzzyTokenMatcherBean))
                .append(query, fuzzyTokenMatcherBean.getQuery())
                .append(thresholdRatio, fuzzyTokenMatcherBean.getThresholdRatio())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31)
                .append(super.hashCode())
                .append(query)
                .append(thresholdRatio)
                .toHashCode();
    }
}