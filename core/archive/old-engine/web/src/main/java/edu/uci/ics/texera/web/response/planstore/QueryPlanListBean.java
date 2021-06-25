package edu.uci.ics.texera.web.response.planstore;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class QueryPlanListBean {
    
    private final List<QueryPlanBean> queryPlanList;
    
    @JsonCreator
    public QueryPlanListBean(
            @JsonProperty(value = "queryPlans", required = true)
            List<QueryPlanBean> queryPlanList) {
        this.queryPlanList = queryPlanList;
    }
    
    @JsonProperty(value = "queryPlans")
    public List<QueryPlanBean> getQueryPlanList() {
        return this.queryPlanList;
    }

}
