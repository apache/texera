package edu.uci.ics.textdb.web.request.beans;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This class defines a link in the operator graph
 * Created by kishorenarendran on 10/21/16.
 */
public class OperatorLinkBean {
    @JsonProperty("from")
    private String fromOperatorID;
    @JsonProperty("to")
    private String toOperatorID;

    public OperatorLinkBean() {
    }

    public OperatorLinkBean(String fromOperatorID, String toOperatorID) {
        this.fromOperatorID = fromOperatorID;
        this.toOperatorID = toOperatorID;
    }

    public String getFromOperatorID() {
        return fromOperatorID;
    }

    public void setFromOperatorID(String fromOperatorID) {
        this.fromOperatorID = fromOperatorID;
    }

    public String getToOperatorID() {
        return toOperatorID;
    }

    public void setToOperatorID(String toOperatorID) {
        this.toOperatorID = toOperatorID;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) return false;
        if (other == this) return true;
        if (!(other instanceof OperatorLinkBean)) return false;
        OperatorLinkBean operatorLinkBean = (OperatorLinkBean) other;
        return new EqualsBuilder()
                .append(fromOperatorID, operatorLinkBean.getFromOperatorID())
                .append(toOperatorID, operatorLinkBean.getToOperatorID())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31)
                .append(fromOperatorID)
                .append(toOperatorID)
                .toHashCode();
    }
}
