package edu.uci.ics.textdb.web.request;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.plangen.LogicalPlan;
import edu.uci.ics.textdb.web.request.beans.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * This class describes the JSON edu.uci.ics.textdb.web.request when a logical query plan is submitted.
 * It also contains the HashMap that is a representation of all the operatorBeans'
 * properties for a given logical query plan.
 * Created by kishorenarendran on 10/21/16.
 */
public class QueryPlanRequest {
    @JsonProperty("operators")
    private ArrayList<OperatorBean> operatorBeans;
    @JsonProperty("links")
    private ArrayList<OperatorLinkBean> operatorLinkBeans;
    @JsonIgnore
    private HashMap<String, HashMap<String, String>> operatorProperties;
    @JsonIgnore
    private LogicalPlan logicalPlan;

    public static final String GET_PROPERTIES_FUNCTION_NAME = "getOperatorProperties";
    public static final HashMap<String, Class<? extends OperatorBean>> OPERATOR_BEAN_MAP = 
            new HashMap<String, Class<? extends OperatorBean>>() {{
        put("DictionaryMatcher", DictionaryMatcherBean.class);
        put("DictionarySource", DictionarySourceBean.class);
        put("FileSink", FileSinkBean.class);
        put("FuzzyTokenMatcher", FuzzyTokenMatcherBean.class);
        put("FuzzyTokenSource", FuzzyTokenSourceBean.class);
        put("IndexSink", IndexSinkBean.class);
        put("Join", JoinBean.class);
        put("KeywordMatcher", KeywordMatcherBean.class);
        put("KeywordSource", KeywordSourceBean.class);
        put("NlpExtractor", NlpExtractorBean.class);
        put("Projection", ProjectionBean.class);
        put("RegexMatcher", RegexMatcherBean.class);
        put("RegexSource", RegexSourceBean.class);
        put("TupleStreamSink", TupleStreamSinkBean.class);
    }};

    public QueryPlanRequest() {
    }

    public QueryPlanRequest(ArrayList<OperatorBean> operators, ArrayList<OperatorLinkBean> operatorLinkBeans) {
        this.operatorBeans = operators;
        this.operatorLinkBeans = operatorLinkBeans;
    }

    @JsonProperty("operators")
    public ArrayList<OperatorBean> getOperatorBeans() {
        return operatorBeans;
    }

    @JsonProperty("operators")
    public void setOperatorBeans(ArrayList<OperatorBean> operatorBeans) {
        this.operatorBeans = operatorBeans;
    }

    @JsonProperty("links")
    public ArrayList<OperatorLinkBean> getOperatorLinkBeans() {
        return operatorLinkBeans;
    }

    @JsonProperty("links")
    public void setOperatorLinkBeans(ArrayList<OperatorLinkBean> operatorLinkBeans) {
        this.operatorLinkBeans = operatorLinkBeans;
    }

    @JsonIgnore
    public HashMap<String, HashMap<String, String>> getOperatorProperties() {
        return operatorProperties;
    }

    @JsonIgnore
    public LogicalPlan getLogicalPlan() {
        return logicalPlan;
    }

    /**
     * This method parses and aggregates the properties of all the operatorBeans in the operatorBeans data members. It returns
     * a false flag when there are any exceptions thrown when trying to create the operator properties HashMap
     * @return - A boolean to denote the status of aggregating the different operatorBeans' properties
     */
    public boolean aggregateOperatorProperties() {
        this.operatorProperties = new HashMap<>();
        for(Iterator<OperatorBean> iter = operatorBeans.iterator(); iter.hasNext(); ) {
            OperatorBean operatorBean = iter.next();
            Class<? extends OperatorBean> operatorBeanClassName =  OPERATOR_BEAN_MAP.get(operatorBean.getOperatorType());
            try {
                Method method = operatorBeanClassName.getMethod(GET_PROPERTIES_FUNCTION_NAME);
                HashMap<String, String> currentOperatorProperty = (HashMap<String, String>) method.invoke(operatorBean);
                if(currentOperatorProperty == null) {
                    this.operatorProperties = null;
                    return false;
                }
                this.operatorProperties.put(operatorBean.getOperatorID(), currentOperatorProperty);
            }
            catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                // If any exception arises a NULL HashMap is raised
                e.printStackTrace();
                this.operatorProperties = null;
                return false;
            }
        }
        return true;
    }

    /**
     * This method uses the OperatorBeans, OperatorProperties and the LinkBeans, to create an instance of the Logical
     * Plan class, any exceptions and failures thrown when adding an operator or a link is considered a failure and a
     * false value is returned, if not a true values is returned
     * @return - A boolean to denote the status of LogicalPlan creation
     */
    public boolean createLogicalPlan() {
        // Checking if the operatorProperties have been aggregated
        if(this.operatorProperties == null) {
            return false;
        }

        logicalPlan = new LogicalPlan();

        // Adding operatorBeans to the logical plan
        for(Iterator<OperatorBean> iterator = operatorBeans.iterator(); iterator.hasNext(); ) {
            try {
                OperatorBean operatorBean = iterator.next();
                logicalPlan.addOperator(operatorBean.getOperatorID(), operatorBean.getOperatorType(), operatorProperties.get(operatorBean.getOperatorID()));
            }
            catch(PlanGenException e) {
                // If a PlanGenException occurs the logical plan object is assigned to NULL, and a false value is returned
                e.printStackTrace();
                this.logicalPlan = null;
                return false;
            }
        }

        // Adding operatorLinkBeans to the logical plan
        for(Iterator<OperatorLinkBean> iterator = operatorLinkBeans.iterator(); iterator.hasNext(); ) {
            try {
                OperatorLinkBean operatorLinkBean = iterator.next();
                logicalPlan.addLink(operatorLinkBean.getFromOperatorID(), operatorLinkBean.getToOperatorID());
            }
            catch(PlanGenException e) {
                // If a PlanGenException occurs the logical plan object is assigned to NULL, and a false value is returned
                this.logicalPlan = null;
                return false;

            }
        }

        // Returning success on complete successful creation of a logical plan
        return true;
    }
}
