package edu.uci.ics.textdb.plangen;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.textdb.common.exception.PlanGenException;

public class OperatorArityConstants {
    
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
      
        put("Join".toLowerCase(), 2);
    }};
    
    public static Map<String, Integer> fixedOutputArityMap = new HashMap<String, Integer>(){{
        put("IndexSink".toLowerCase(), 0);
        put("FileSink".toLowerCase(), 0);
        
        put("KeywordMatcher".toLowerCase(), 1);
        put("DictionaryMatcher".toLowerCase(), 1);
        put("RegexMatcher".toLowerCase(), 1);
        put("NlpExtractor".toLowerCase(), 1);
        put("FuzzyTokenMatcher".toLowerCase(), 1);
        
        put("KeywordSource".toLowerCase(), 1);
        put("DictionarySource".toLowerCase(), 1);
        
        put("Join".toLowerCase(), 1);  
    }};
    
    public static int getFixedInputArity(String operatorType) throws PlanGenException {
        PlanGenUtils.planGenAssert(fixedInputArityMap.containsKey(operatorType.toLowerCase()), 
                String.format("input arity of %s is not specified.", operatorType));
        return fixedInputArityMap.get(operatorType.toLowerCase());
    }
    
    public static int getFixedOutputArity(String operatorType) throws PlanGenException {
        PlanGenUtils.planGenAssert(fixedOutputArityMap.containsKey(operatorType.toLowerCase()), 
                String.format("input arity of %s is not specified.", operatorType));
        return fixedOutputArityMap.get(operatorType.toLowerCase());
    }
    
}
