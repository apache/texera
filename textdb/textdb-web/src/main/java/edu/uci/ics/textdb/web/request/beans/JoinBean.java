package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
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
    @JsonProperty("id_attribute")
    private String idAttribute;
    @JsonProperty("distance")
    private String distance;

    // A bean variable for the predicate type of the join has been omitted for now and will be included in the future

    public JoinBean() {
    }

    public JoinBean(String operatorID, String operatorType, String attributes, String limit, String offset,
                    String idAttribute, String distance) {
        super(operatorID, operatorType, attributes, limit, offset);
        this.idAttribute = idAttribute;
        this.distance = distance;
    }

    @JsonProperty("id_attribute")
    public String getIdAttribute() {
        return idAttribute;
    }

    @JsonProperty("id_attribute")
    public void setIdAttribute(String idAttribute) {
        this.idAttribute = idAttribute;
    }

    @JsonProperty("distance")
    public String getDistance() {
        return distance;
    }

    @JsonProperty("distance")
    public void setDistance(String distance) {
        this.distance = distance;
    }

    public HashMap<String, String> getOperatorProperties() {
        HashMap<String, String> operatorProperties = super.getOperatorProperties();
        if(this.getIdAttribute() == null || this.getDistance() == null || operatorProperties == null)
            return null;
        operatorProperties.put(JoinBuilder.JOIN_ID_ATTRIBUTE_NAME, this.getIdAttribute());
        operatorProperties.put(JoinBuilder.JOIN_DISTANCE, this.getDistance());
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
                .append(idAttribute, joinBean.getIdAttribute())
                .append(distance, joinBean.getDistance())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31)
                .append(super.hashCode())
                .append(idAttribute)
                .append(distance)
                .toHashCode();
    }
}
