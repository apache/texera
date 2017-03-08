package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.uci.ics.textdb.web.request.QueryPlanRequest;

/**
 * This class is a bean to serialize a stored Query Plan and its associated information from the
 * plan store to JSON
 * Created by kishorenarendran on 2/22/17.
 */
public class QueryPlanBean {
    @JsonProperty("name")
    private String name;
    @JsonProperty("description")
    private String description;
    @JsonProperty("query_plan")
    private QueryPlanRequest queryPlan;

    public QueryPlanBean() {
    }

    public QueryPlanBean(String name, String description, QueryPlanRequest queryPlan) {
        this.name = name;
        this.description = description;
        this.queryPlan = queryPlan;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("description")
    public String getDescription() {
        return description;
    }

    @JsonProperty("description")
    public void setDescription(String description) {
        this.description = description;
    }

    @JsonProperty("query_plan")
    public QueryPlanRequest getQueryPlan() {
        return queryPlan;
    }

    @JsonProperty("query_plan")
    public void setQueryPlan(QueryPlanRequest queryPlan) {
        this.queryPlan = queryPlan;
    }
}