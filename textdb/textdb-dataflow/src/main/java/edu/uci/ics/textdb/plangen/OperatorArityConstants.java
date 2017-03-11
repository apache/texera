package edu.uci.ics.textdb.plangen;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.textdb.api.exception.PlanGenException;

/**
 * OperatorArityConstants class includes the input and output arity constraints of each operator.
 * 
 * @author Zuozhi Wang
 *
 */
public class OperatorArityConstants {
    
    /*
     * Input arity map for operators which have fixed input arity.
     */
    public static Map<String, Integer> fixedInputArityMap = new HashMap<String, Integer>(){{
        put("KeywordSource".toLowerCase(), 0);
        put("DictionarySource".toLowerCase(), 0);
        
        put("KeywordMatcher".toLowerCase(), 1);
        put("DictionaryMatcher".toLowerCase(), 1);
        put("RegexMatcher".toLowerCase(), 1);
        put("NlpExtractor".toLowerCase(), 1);
        put("FuzzyTokenMatcher".toLowerCase(), 1);
        
        put("FileSink".toLowerCase(), 1);
        put("IndexSink".toLowerCase(), 1);
        put("TupleStreamSink".toLowerCase(), 1);
      
        put("Projection".toLowerCase(), 1);
        put("Join".toLowerCase(), 2);
    }};
    
    /*
     * Output arity map for operators which have fixed output arity.
     */
    public static Map<String, Integer> fixedOutputArityMap = new HashMap<String, Integer>(){{
        put("IndexSink".toLowerCase(), 0);
        put("FileSink".toLowerCase(), 0);
        put("TupleStreamSink".toLowerCase(), 0);
        
        put("KeywordMatcher".toLowerCase(), 1);
        put("DictionaryMatcher".toLowerCase(), 1);
        put("RegexMatcher".toLowerCase(), 1);
        put("NlpExtractor".toLowerCase(), 1);
        put("FuzzyTokenMatcher".toLowerCase(), 1);
        
        put("KeywordSource".toLowerCase(), 1);
        put("DictionarySource".toLowerCase(), 1);
        
        put("Projection".toLowerCase(), 1);
        put("Join".toLowerCase(), 1);  
    }};
    
    
    /**
     * Gets the input arity of an operator type.
     * 
     * @param operatorType
     * @return
     * @throws PlanGenException, if the oeprator's input arity is not specified.
     */
    public static int getFixedInputArity(String operatorType) throws PlanGenException {
        PlanGenUtils.planGenAssert(fixedInputArityMap.containsKey(operatorType.toLowerCase()), 
                String.format("input arity of %s is not specified.", operatorType));
        return fixedInputArityMap.get(operatorType.toLowerCase());
    }
    
    /**
     * Gets the output arity of an operator type.
     * 
     * @param operatorType
     * @return
     * @throws PlanGenException, if the oeprator's output arity is not specified.
     */
    public static int getFixedOutputArity(String operatorType) throws PlanGenException {
        PlanGenUtils.planGenAssert(fixedOutputArityMap.containsKey(operatorType.toLowerCase()), 
                String.format("input arity of %s is not specified.", operatorType));
        return fixedOutputArityMap.get(operatorType.toLowerCase());
    }
    
}
