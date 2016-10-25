package edu.uci.ics.textdb.web.request;

import edu.uci.ics.textdb.web.request.beans.LinkBean;
import edu.uci.ics.textdb.web.request.beans.OperatorBean;

import java.util.ArrayList;

/**
 * This class describes the JSON request when a query plan is submitted
 * Created by kishorenarendran on 10/12/16.
 */
public class QueryPlanRequest {
    private ArrayList<OperatorBean> operators;
    private ArrayList<LinkBean> links;

    public QueryPlanRequest() {
    }

    public QueryPlanRequest(ArrayList<OperatorBean> operators, ArrayList<LinkBean> links) {
        this.operators = operators;
        this.links = links;
    }

    public ArrayList<OperatorBean> getOperators() {
        return operators;
    }

    public void setOperators(ArrayList<OperatorBean> operators) {
        this.operators = operators;
    }

    public ArrayList<LinkBean> getLinks() {
        return links;
    }

    public void setLinks(ArrayList<LinkBean> links) {
        this.links = links;
    }
}
