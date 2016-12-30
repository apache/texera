package edu.uci.ics.textdb.textql.planbuilder.beans;

import java.util.HashMap;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import edu.uci.ics.textdb.textql.planbuilder.beans.PassThroughBean;
import edu.uci.ics.textdb.web.request.beans.OperatorBean;

/**
 * Represent an Operator that pipes all the tuples from the input to the output.
 * All the input tuples are projected straight to the output without any modifications.
 * This Bean shall not be used to be converted to an Operator, it is used just as a temporary
 * operator while the query plan is being build.
 * 
 * @author Flavio Bayer
 *
 */
public class PassThroughBean extends OperatorBean {
    
    /**
     * Create a { @code PassThroughBean } with the given parameters.
     * @param operatorID The ID of the operator.
     * @param operatorType The String representation of the type of the operator.
     */
    public PassThroughBean(String operatorID, String operatorType) {
        super(operatorID, "PassThrough", "", null, null);
    }
    
    /**
     * Set the list of attributes of the operator.
     * The PassThroughBean does not allow this attribute, the value should be either null or "".
     * @param attributes A string containing the names of the attributes.
     * @throws IllegalArgumentException If { @code attributes } is not null nor "".
     */
    @Override
    public void setAttributes(String attributes) {
        if(attributes!=null && !attributes.equals("")){
            throw new IllegalArgumentException("Only null or \"\" are valid for Attributes in PassThroughBean");
        }
    }
    
    /**
     * Set the limit parameter of the operator.
     * The PassThroughBean does not allow this attribute, the value should be null.
     * @param limit A string containing the limit value.
     * @throws IllegalArgumentException If { @code limit } is not null.
     */
    @Override
    public void setLimit(String limit) {
        if(limit!=null){
            throw new IllegalArgumentException("Only null is valid for Limit in PassThroughBean");
        }
    }
    
    /**
     * Set the offset parameter of the operator.
     * The PassThroughBean does not allow this attribute, the value should be null.
     * @param offset A string containing the offset value.
     * @throws IllegalArgumentException If { @code offset } is not null.
     */
    @Override
    public void setOffset(String offset) {
        if(offset!=null){
            throw new IllegalArgumentException("Only null is valid for Offset in PassThroughBean");
        }
    }
    
    /**
     * Build a map containing the properties of the this operator.
     * The PassThrough operator has no extra attributes.
     * @return The generated map containing the properties of the operator
     */
    @Override
    public HashMap<String, String> getOperatorProperties() {
        return super.getOperatorProperties();
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) return false;
        if (other == this) return true;
        if (!(other instanceof PassThroughBean)) return false;
        PassThroughBean passThroughBean = (PassThroughBean) other;
        return super.equals(passThroughBean);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31)
                    .append(super.hashCode())
                    .toHashCode();
    }
}