package edu.uci.ics.textdb.web.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.uci.ics.textdb.web.request.beans.QueryPlanBean;

import java.util.ArrayList;

/**
 * @author Kishore Narendran on 2/16/17.
 */
public class QueryPlanResponse {

    @JsonProperty("query_plans")
    private ArrayList<QueryPlanBean> queryPlans;

    public QueryPlanResponse() {
    }

    public QueryPlanResponse(ArrayList<QueryPlanBean> queryPlans) {
        this.queryPlans = queryPlans;
    }

    @JsonProperty("query_plans")
    public ArrayList<QueryPlanBean> getQueryPlans() {
        return queryPlans;
    }

    @JsonProperty("query_plans")
    public void setQueryPlans(ArrayList<QueryPlanBean> queryPlans) {
        this.queryPlans = queryPlans;
    }
}