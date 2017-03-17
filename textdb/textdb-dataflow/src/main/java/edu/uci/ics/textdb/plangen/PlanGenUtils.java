package edu.uci.ics.textdb.plangen;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.exception.PlanGenException;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.plangen.operatorbuilder.DictionaryMatcherBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.DictionarySourceBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.FileSinkBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.FuzzyTokenMatcherBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.JoinBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.KeywordMatcherBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.KeywordSourceBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.NlpExtractorBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.ProjectionBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.RegexMatcherBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.TupleStreamSinkBuilder;

/**
 * This class provides a set of helper functions that are commonly used in plan generation.
 * @author Zuozhi Wang
 *
 */
public class PlanGenUtils {
    
    @FunctionalInterface
    public interface OperatorBuilder {
        public IOperator buildOperator(Map<String, String> operatorProperties) throws PlanGenException;
    }
    
    public static Map<String, OperatorBuilder> operatorBuilderMap = new HashMap<>();
    static {
        operatorBuilderMap.put("KeywordMatcher".toLowerCase(), KeywordMatcherBuilder::buildKeywordMatcher);
        operatorBuilderMap.put("DictionaryMatcher".toLowerCase(), DictionaryMatcherBuilder::buildOperator);
        operatorBuilderMap.put("RegexMatcher".toLowerCase(), RegexMatcherBuilder::buildRegexMatcher);
        operatorBuilderMap.put("NlpExtractor".toLowerCase(), NlpExtractorBuilder::buildOperator);
        operatorBuilderMap.put("FuzzyTokenMatcher".toLowerCase(), FuzzyTokenMatcherBuilder::buildOperator);
        operatorBuilderMap.put("KeywordSource".toLowerCase(), KeywordSourceBuilder::buildSourceOperator);
        operatorBuilderMap.put("DictionarySource".toLowerCase(), DictionarySourceBuilder::buildSourceOperator);
        operatorBuilderMap.put("FileSink".toLowerCase(), FileSinkBuilder::buildSink);
        operatorBuilderMap.put("TupleStreamSink".toLowerCase(), TupleStreamSinkBuilder::buildTupleStreamSink);
        operatorBuilderMap.put("Join".toLowerCase(), JoinBuilder::buildOperator);
        operatorBuilderMap.put("Projection".toLowerCase(), ProjectionBuilder::buildOperator);
    }
    
    public static IOperator buildOperator(String operatorType, Map<String, String> operatorProperties) throws PlanGenException {
        OperatorBuilder operatorBuilder = operatorBuilderMap.get(operatorType.toLowerCase());
        planGenAssert(operatorBuilder != null, 
                String.format("operatorType %s is invalid. It must be one of %s.", operatorType, operatorBuilderMap.keySet()));
               
        return operatorBuilder.buildOperator(operatorProperties);
    }
    
    
    public static void planGenAssert(boolean assertBoolean, String errorMessage) throws PlanGenException {
        if (! assertBoolean) {
            throw new PlanGenException(errorMessage);
        }
    }
    
    /**
     * This function checks if a string is a valid operator (case insensitive).
     * 
     * @param operatorStr
     * @return true if the string is an operator
     */
    public static boolean isValidOperator(String operatorStr) {
        return operatorBuilderMap.keySet().stream().anyMatch(str -> str.toLowerCase().equals(operatorStr.toLowerCase()));
    }

    /**
     * This function checks if a string is a valid attribute type (case insensitive).
     * 
     * @param attributeType
     * @return true if the string is an attribute type
     */
    public static boolean isValidAttributeType(String attributeType) {
        return Stream.of(AttributeType.values()).anyMatch(type -> type.toString().toLowerCase().equals(attributeType.toLowerCase()));
    }
    
    /**
     * This function converts a attributeTypeString to AttributeType (case insensitive).
     * It returns null if string is not a valid type.
     * 
     * @param attributeTypeStr
     * @return AttributeType, null if attributeTypeStr is not a valid type.
     */
    public static AttributeType convertAttributeType(String attributeTypeStr) {
        return Stream.of(AttributeType.values())
                .filter(typeStr -> typeStr.toString().toLowerCase().equals(attributeTypeStr.toLowerCase()))
                .findAny().orElse(null);
    }
    

}
