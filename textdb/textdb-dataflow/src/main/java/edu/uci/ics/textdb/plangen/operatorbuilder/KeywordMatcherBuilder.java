package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import edu.uci.ics.textdb.api.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.PlanGenException;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcher;
import edu.uci.ics.textdb.dataflow.utils.DataflowUtils;
import edu.uci.ics.textdb.plangen.PlanGenUtils;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;

/**
 * KeywordMatcherBuilder provides a static function that builds a KeywordMatcher.
 * 
 * Besides some commonly used properties (properties for attribute list, limit, offset), 
 * KeywordMatcherBuilder currently needs the following properties:
 * 
 *   keyword (required)
 *   matchingType (required)
 * 
 * @author Zuozhi Wang
 *
 */
public class KeywordMatcherBuilder {

    public static final String KEYWORD = "keyword";
    public static final String MATCHING_TYPE = "matchingType";

    /**
     * Builds a KeywordMatcher according to operatorProperties.
     */
    public static KeywordMatcher buildKeywordMatcher(Map<String, String> operatorProperties) throws PlanGenException {
        String keyword = OperatorBuilderUtils.getRequiredProperty(KEYWORD, operatorProperties);
        String matchingTypeStr = OperatorBuilderUtils.getRequiredProperty(MATCHING_TYPE, operatorProperties);

        // check if keyword is empty
        PlanGenUtils.planGenAssert(!keyword.trim().isEmpty(), "keyword is empty");

        // generate attribute names
        List<String> attributeNames = OperatorBuilderUtils.constructAttributeNames(operatorProperties);

        // generate matching type
        KeywordMatchingType matchingType = KeywordMatcherBuilder.getKeywordMatchingType(matchingTypeStr);
        PlanGenUtils.planGenAssert(matchingType != null, 
                "matching type: "+ matchingTypeStr +" is not valid, "
                + "must be one of " + KeywordMatcherBuilder.keywordMatchingTypeMap.keySet());

        // build KeywordMatcher
        KeywordPredicate keywordPredicate;
        keywordPredicate = new KeywordPredicate(keyword, attributeNames,
                LuceneAnalyzerConstants.getStandardAnalyzer(), matchingType);     
        
        KeywordMatcher keywordMatcher = new KeywordMatcher(keywordPredicate);

        // set limit and offset
        Integer limitInt = OperatorBuilderUtils.findLimit(operatorProperties);
        if (limitInt != null) {
            keywordMatcher.setLimit(limitInt);
        }
        Integer offsetInt = OperatorBuilderUtils.findOffset(operatorProperties);
        if (offsetInt != null) {
            keywordMatcher.setOffset(offsetInt);
        }

        return keywordMatcher;
    }
    
    public static Map<String, KeywordMatchingType> keywordMatchingTypeMap = new HashMap<>();
    static {
        // put all keywordMatchingType
        keywordMatchingTypeMap.putAll(
                Stream.of(KeywordMatchingType.values()).collect(Collectors.toMap((x -> x.toString().toLowerCase()), (x -> x))));
        // add a few abbreviations
        keywordMatchingTypeMap.put("substring", KeywordMatchingType.SUBSTRING_SCANBASED);
        keywordMatchingTypeMap.put("conjunction", KeywordMatchingType.CONJUNCTION_INDEXBASED);
        keywordMatchingTypeMap.put("phrase", KeywordMatchingType.PHRASE_INDEXBASED);
    }
    
    public static KeywordMatchingType getKeywordMatchingType(String matchingTypeStr) {
        return keywordMatchingTypeMap.get(matchingTypeStr.toLowerCase());      
    }

}
