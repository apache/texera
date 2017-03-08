package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import edu.uci.ics.textdb.plangen.operatorbuilder.FuzzyTokenMatcherBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.OperatorBuilderUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.HashMap;

/**
 * This class defines the properties/data members specific to the FuzzyTokenSource operator
 * and extends the OperatorBean class which defines the data members general to all operators
 * Created by kishorenarendran on 11/09/16.
 */
@JsonTypeName("FuzzyTokenSource")
public class FuzzyTokenSourceBean extends OperatorBean {
    @JsonProperty("query")
    private String query;
    @JsonProperty("threshold_ratio")
    private String thresholdRatio;
    @JsonProperty("data_source")
    private String dataSource;

    public FuzzyTokenSourceBean() {
    }

    public FuzzyTokenSourceBean(String operatorID, String operatorType, String attributes, String limit, String offset,
                                String query, String thresholdRatio, String dataSource) {
        super(operatorID, operatorType, attributes, limit, offset);
        this.query = query;
        this.thresholdRatio = thresholdRatio;
        this.dataSource = dataSource;
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

    @JsonProperty("data_source")
    public String getDataSource() {
        return dataSource;
    }

    @JsonProperty("data_source")
    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    @JsonIgnore
    public HashMap<String, String> getOperatorProperties() {
        HashMap<String, String> operatorProperties = super.getOperatorProperties();
        if(this.getQuery() == null || this.getThresholdRatio() == null || this.getDataSource() == null ||
                operatorProperties == null)
            return null;
        operatorProperties.put(FuzzyTokenMatcherBuilder.FUZZY_STRING, this.getQuery());
        operatorProperties.put(FuzzyTokenMatcherBuilder.THRESHOLD_RATIO, this.getThresholdRatio());
        operatorProperties.put(OperatorBuilderUtils.DATA_SOURCE, this.getDataSource());
        return operatorProperties;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) return false;
        if (other == this) return true;
        if (!(other instanceof FuzzyTokenSourceBean)) return false;
        FuzzyTokenSourceBean fuzzyTokenSourceBean = (FuzzyTokenSourceBean) other;
        return new EqualsBuilder()
                .appendSuper(super.equals(fuzzyTokenSourceBean))
                .append(query, fuzzyTokenSourceBean.getQuery())
                .append(thresholdRatio, fuzzyTokenSourceBean.getThresholdRatio())
                .append(dataSource, fuzzyTokenSourceBean.getDataSource())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31)
                .append(super.hashCode())
                .append(query)
                .append(thresholdRatio)
                .append(dataSource)
                .toHashCode();
    }
}