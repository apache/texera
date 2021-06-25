package edu.uci.ics.texera.web.response.planstore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.texera.dataflow.plangen.LogicalPlan;
import edu.uci.ics.texera.dataflow.planstore.PlanStoreConstants;

/**
 * This class is a bean to serialize a stored Query Plan and its associated information from the
 * plan store to JSON
 * 
 * Created by kishorenarendran on 2/22/17.
 * 
 * @author Kishore
 * @author Zuozhi
 */
public class QueryPlanBean {

    private final String planName;
    private final String planDescription;
    private final LogicalPlan logicalPlan;
    
    @JsonCreator
    public QueryPlanBean(
            @JsonProperty(value = PlanStoreConstants.NAME, required = true)
            String planName,
            @JsonProperty(value = PlanStoreConstants.DESCRIPTION, required = false)
            String planDescription,
            @JsonProperty(value = PlanStoreConstants.LOGICAL_PLAN_JSON, required = true)
            LogicalPlan logicalPlan
            ) {
        this.planName = planName.trim();
        if (planDescription == null) {
            this.planDescription = "empty description";
        } else {
            this.planDescription = planDescription.trim();
        }
        this.logicalPlan = logicalPlan;
    }
    
    @JsonProperty(value = PlanStoreConstants.NAME)
    public String getName() {
        return planName;
    }

    @JsonProperty(value = PlanStoreConstants.DESCRIPTION)
    public String getDescription() {
        return planDescription;
    }

    @JsonProperty(value = PlanStoreConstants.LOGICAL_PLAN_JSON)
    public LogicalPlan getQueryPlan() {
        return logicalPlan;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((logicalPlan == null) ? 0 : logicalPlan.hashCode());
        result = prime * result + ((planDescription == null) ? 0 : planDescription.hashCode());
        result = prime * result + ((planName == null) ? 0 : planName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        QueryPlanBean other = (QueryPlanBean) obj;
        if (logicalPlan == null) {
            if (other.logicalPlan != null)
                return false;
        } else if (!logicalPlan.equals(other.logicalPlan))
            return false;
        if (planDescription == null) {
            if (other.planDescription != null)
                return false;
        } else if (!planDescription.equals(other.planDescription))
            return false;
        if (planName == null) {
            if (other.planName != null)
                return false;
        } else if (!planName.equals(other.planName))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "LogicalPlanDescriptor [planName=" + planName + ", planDescription=" + planDescription + ", logicalPlan="
                + logicalPlan + "]";
    }

}
