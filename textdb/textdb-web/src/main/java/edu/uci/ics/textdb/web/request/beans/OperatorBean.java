package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import edu.uci.ics.textdb.plangen.operatorbuilder.OperatorBuilderUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.HashMap;

/**
 * This class is the abstract class that defines the data members common to all operators. It is
 * extended by individual operator beans in order to define the data members specific to each
 * operator
 * Created by kishorenarendran on 10/21/16.
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include= JsonTypeInfo.As.PROPERTY, property="operator_type", visible = true)
@JsonSubTypes({
        @JsonSubTypes.Type(value=DictionaryMatcherBean.class, name="DictionaryMatcher"),
        @JsonSubTypes.Type(value=DictionarySourceBean.class, name="DictionarySource"),
        @JsonSubTypes.Type(value=FileSinkBean.class, name="FileSink"),
        @JsonSubTypes.Type(value=FuzzyTokenMatcherBean.class, name="FuzzyTokenMatcher"),
        @JsonSubTypes.Type(value=FuzzyTokenSourceBean.class, name="FuzzyTokenSource"),
        @JsonSubTypes.Type(value=IndexSinkBean.class, name="IndexSink"),
        @JsonSubTypes.Type(value=JoinBean.class, name="Join"),
        @JsonSubTypes.Type(value=KeywordMatcherBean.class, name="KeywordMatcher"),
        @JsonSubTypes.Type(value=KeywordSourceBean.class, name="KeywordSource"),
        @JsonSubTypes.Type(value=NlpExtractorBean.class, name="NlpExtractor"),
        @JsonSubTypes.Type(value=ProjectionBean.class, name="Projection"),
        @JsonSubTypes.Type(value=RegexMatcherBean.class, name="RegexMatcher"),
        @JsonSubTypes.Type(value=RegexSourceBean.class, name="RegexSource"),
        @JsonSubTypes.Type(value=TupleStreamSinkBean.class, name="TupleStreamSink")
})

@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class OperatorBean {
    @JsonProperty("operator_id")
    private String operatorID;
    @JsonProperty("operator_type")
    private String operatorType;
    @JsonProperty("attributes")
    private String attributes;
    @JsonProperty("limit")
    private String limit;
    @JsonProperty("offset")
    private String offset;

    public OperatorBean() {
    }

    public OperatorBean(String operatorID, String operatorType, String attributes, String limit, String offset) {
        this.operatorID = operatorID;
        this.operatorType = operatorType;
        this.attributes = attributes;
        this.limit = limit;
        this.offset = offset;
    }

    @JsonProperty("operator_id")
    public String getOperatorID() {
        return operatorID;
    }

    @JsonProperty("operator_id")
    public void setOperatorID(String operatorID) {
        this.operatorID = operatorID;
    }

    @JsonProperty("operator_type")
    public String getOperatorType() {
        return operatorType;
    }

    @JsonProperty("operator_type")
    public void setOperatorType(String operatorType) {
        this.operatorType = operatorType;
    }

    @JsonProperty("attributes")
    public String getAttributes() {
        return attributes;
    }

    @JsonProperty("attributes")
    public void setAttributes(String attributes) {
        this.attributes = attributes;
    }

    @JsonProperty("limit")
    public String getLimit() {
        return limit;
    }

    @JsonProperty("limit")
    public void setLimit(String limit) {
        this.limit = limit;
    }

    @JsonProperty("offset")
    public String getOffset() {
        return offset;
    }

    @JsonProperty("offset")
    public void setOffset(String offset) {
        this.offset = offset;
    }

    public HashMap<String, String> getOperatorProperties() {
        HashMap<String, String> basicOperatorProperties = new HashMap<String, String>();
        if(this.getLimit() != null)
            basicOperatorProperties.put(OperatorBuilderUtils.LIMIT, this.getLimit());
        if(this.getOffset() != null)
            basicOperatorProperties.put(OperatorBuilderUtils.OFFSET, this.getOffset());
        if(this.getAttributes() == null && !this.getOperatorType().equals("Join"))
            return null;
        basicOperatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, this.getAttributes());
        return basicOperatorProperties;
    }

    @Override
    public boolean equals(Object other){
        if (other == null) return false;
        if (other == this) return true;
        if (!(other instanceof OperatorBean))return false;
        OperatorBean operatorBean = (OperatorBean)other;
        return new EqualsBuilder()
                .append(operatorID, operatorBean.getOperatorID())
                .append(operatorType, operatorBean.getOperatorType())
                .append(attributes, operatorBean.getAttributes())
                .append(limit, operatorBean.getLimit())
                .append(offset, operatorBean.getOffset())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31)
                .append(operatorID)
                .append(operatorType)
                .append(attributes)
                .append(limit)
                .append(offset)
                .toHashCode();
    }
}
