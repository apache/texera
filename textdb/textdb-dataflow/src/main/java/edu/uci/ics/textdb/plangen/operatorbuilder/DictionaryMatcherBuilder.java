package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.util.List;
import java.util.Map;

import edu.uci.ics.textdb.api.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.PlanGenException;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.dataflow.common.Dictionary;
import edu.uci.ics.textdb.dataflow.common.DictionaryPredicate;
import edu.uci.ics.textdb.dataflow.dictionarymatcher.DictionaryMatcher;
import edu.uci.ics.textdb.plangen.PlanGenUtils;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;

/**
 * DictionaryMatcherBuilder provides a static function that builds a DictionaryMatcher.
 * 
 * Besides some commonly used properties (properties for attribute list, limit, offset), 
 * DictionaryMatcherBuilder currently needs the following properties:
 * 
 *   dictionary (required)
 *   matchingType (required)
 * 
 * @author Zuozhi Wang
 *
 */
public class DictionaryMatcherBuilder {

    public static final String DICTIONARY = "dictionary";
    public static final String MATCHING_TYPE = KeywordMatcherBuilder.MATCHING_TYPE;

    /**
     * Builds a DictionaryMatcher according to operatorProperties.
     */
    public static DictionaryMatcher buildOperator(Map<String, String> operatorProperties) throws PlanGenException {
        String dictionaryStr = OperatorBuilderUtils.getRequiredProperty(DICTIONARY, operatorProperties);
        String matchingTypeStr = OperatorBuilderUtils.getRequiredProperty(MATCHING_TYPE, operatorProperties);

        // check if dictionary is empty       
        PlanGenUtils.planGenAssert(!dictionaryStr.trim().isEmpty(), "dictionary is empty");
        List<String> dictionaryList = OperatorBuilderUtils.splitStringByComma(dictionaryStr);
        Dictionary dictionary = new Dictionary(dictionaryList);

        // generate attribute names
        List<String> attributeNames = OperatorBuilderUtils.constructAttributeNames(operatorProperties);

        // generate matching type        
        KeywordMatchingType matchingType = KeywordMatcherBuilder.getKeywordMatchingType(matchingTypeStr);
        PlanGenUtils.planGenAssert(matchingType != null, 
                "matching type: "+ matchingTypeStr +" is not valid, "
                + "must be one of " + KeywordMatcherBuilder.keywordMatchingTypeMap.keySet());

        // build DictionaryMatcher
        DictionaryPredicate predicate = new DictionaryPredicate(dictionary, attributeNames,
                LuceneAnalyzerConstants.getStandardAnalyzer(), matchingType);
        DictionaryMatcher operator = new DictionaryMatcher(predicate);

        // set limit and offset
        Integer limitInt = OperatorBuilderUtils.findLimit(operatorProperties);
        if (limitInt != null) {
            operator.setLimit(limitInt);
        }
        Integer offsetInt = OperatorBuilderUtils.findOffset(operatorProperties);
        if (offsetInt != null) {
            operator.setOffset(offsetInt);
        }

        return operator;
    }

}
