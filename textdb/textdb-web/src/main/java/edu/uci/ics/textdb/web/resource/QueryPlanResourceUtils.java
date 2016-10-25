package edu.uci.ics.textdb.web.resource;

import edu.uci.ics.textdb.web.request.beans.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Contains some constants and utility functions for the QueryPlanResource in order to
 * create the requisite HashMap of properties for the different operators.
 * Created by kishorenarendran on 10/18/16.
 */
public class QueryPlanResourceUtils {
    /**
     * Mapping from Operator type to Operator bean class
     */
    public static final HashMap<String, Class> OPERATOR_BEAN_MAP = new HashMap<String, Class>() {{
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
    }};

    public static final String GET_PROPERTIES_FUNCTION_NAME = "getOperatorProperties";

    /**
     * This function aggregates all the operators' properties that have been sent by the query plan request
     * @param operatorBeans - Contains a list of input operator bean objects
     * @return - HashMap of properties for each operatorID
     */
    public static HashMap<String, HashMap<String, String>> aggregateOperatorProperties(List<OperatorBean> operatorBeans) {
        HashMap<String, HashMap<String, String>> operatorProperties = new HashMap<>();
        for(Iterator<OperatorBean> iter = operatorBeans.iterator(); iter.hasNext(); ) {
            OperatorBean operatorBean = iter.next();
            Class operatorBeanClassName =  OPERATOR_BEAN_MAP.get(operatorBean.getOperatorType());
            try {
                Method method = operatorBeanClassName.getMethod(GET_PROPERTIES_FUNCTION_NAME);
                HashMap<String, String> currentOperatorProperty = (HashMap<String, String>) method.invoke(operatorBean);
                operatorProperties.put(operatorBean.getOperatorID(), currentOperatorProperty);
            }
            catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                // If any exception arises a NULL HashMap is raised
                return null;
            }
        }
        return operatorProperties;
    }
}
