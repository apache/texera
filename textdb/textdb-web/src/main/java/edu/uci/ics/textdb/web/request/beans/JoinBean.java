package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import edu.uci.ics.textdb.dataflow.join.Join;
import edu.uci.ics.textdb.plangen.operatorbuilder.JoinBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.HashMap;

/**
 * This class defines the properties/data members specific to the Join operator
 * and extends the OperatorBean class which defines the data members general to all operators
 * Created by kishorenarendran on 11/09/16.
 */
@JsonTypeName("Join")
public class JoinBean extends OperatorBean {
    @JsonProperty("inner_attribute")
    private String innerAttribute;
    @JsonProperty("outer_attribute")
    private String outerAttribute;
    @JsonProperty("predicate_type")
    private String predicateType;
    @JsonProperty("threshold")
    private String threshold;

    // A bean variable for the predicate type of the join has been omitted for now and will be included in the future

    public JoinBean() {
    }

    public JoinBean(String operatorID, String operatorType, String limit, String offset,
                    String innerAttribute, String outerAttribute, String predicateType, String threshold) {
        super(operatorID, operatorType, null, limit, offset);
        this.innerAttribute = innerAttribute;
        this.outerAttribute = outerAttribute;
        this.predicateType = predicateType;
        this.threshold = threshold;
    }

    @JsonProperty("inner_attribute")
    public String getInnerAttribute() {
        return innerAttribute;
    }

    @JsonProperty("inner_attribute")
    public void setInnerAttribute(String innerAttribute) {
        this.innerAttribute = innerAttribute;
    }

    @JsonProperty("outer_attribute")
    public String getOuterAttribute() {
        return outerAttribute;
    }

    @JsonProperty("outer_attribute")
    public void setOuterAttribute(String outerAttribute) {
        this.outerAttribute = outerAttribute;
    }

    @JsonProperty("predicate_type")
    public String getPredicateType() {
        return predicateType;
    }

    @JsonProperty("predicate_type")
    public void setPredicateType(String predicateType) {
        this.predicateType = predicateType;
    }

    @JsonProperty("threshold")
    public String getThreshold() {
        return threshold;
    }

    @JsonProperty("threshold")
    public void setThreshold(String threshold) {
        this.threshold = threshold;
    }

    public HashMap<String, String> getOperatorProperties() {
        HashMap<String, String> operatorProperties = super.getOperatorProperties();
        if(this.getInnerAttribute() == null || this.getOuterAttribute() == null || this.getThreshold() == null ||
                operatorProperties == null)
            return null;
        operatorProperties.put(JoinBuilder.JOIN_INNER_ATTR_NAME, this.getInnerAttribute());
        operatorProperties.put(JoinBuilder.JOIN_OUTER_ATTR_NAME, this.getOuterAttribute());
        operatorProperties.put(JoinBuilder.JOIN_PREDICATE, this.getPredicateType());
        operatorProperties.put(JoinBuilder.JOIN_THRESHOLD, this.getThreshold());
        // TODO - Check on the other properties required for the Join Operator
        return operatorProperties;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) return false;
        if (other == this) return true;
        if (!(other instanceof JoinBean)) return false;
        JoinBean joinBean = (JoinBean) other;
        return new EqualsBuilder()
                .appendSuper(super.equals(joinBean))
                .append(innerAttribute, joinBean.getInnerAttribute())
                .append(outerAttribute, joinBean.getOuterAttribute())
                .append(predicateType, joinBean.getPredicateType())
                .append(threshold, joinBean.getThreshold())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31)
                .append(super.hashCode())
                .append(innerAttribute)
                .append(outerAttribute)
                .append(predicateType)
                .append(threshold)
                .toHashCode();
    }
}
