package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.HashMap;

/**
 * This class defines the properties/data members specific to the Projection operator
 * and extends the OperatorBean class which defines the data members general to all operators
 * Created by kishorenarendran on 11/09/16.
 */
@JsonTypeName("Projection")
public class ProjectionBean extends OperatorBean {
    // Properties regarding the projection operator will go here
	
    public ProjectionBean() {
    }
	
    public ProjectionBean(String operatorID, String operatorType, String attributes, String limit, String offset) {
        super(operatorID, operatorType, attributes, limit, offset);
    }
    
    @Override
    @JsonIgnore
    public HashMap<String, String> getOperatorProperties() {
        HashMap<String, String> operatorProperties = super.getOperatorProperties();
        if(operatorProperties == null)
            return null;
        return operatorProperties;
    }
    @Override
    public boolean equals(Object other) {
        if (other == null) return false;
        if (other == this) return true;
        if (!(other instanceof ProjectionBean)) return false;
        ProjectionBean projectionBean = (ProjectionBean) other;
        return super.equals(projectionBean);
    }
}