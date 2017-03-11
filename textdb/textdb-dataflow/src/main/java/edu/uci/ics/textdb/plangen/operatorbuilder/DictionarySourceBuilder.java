package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.util.List;
import java.util.Map;

import edu.uci.ics.textdb.api.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.api.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.common.Dictionary;
import edu.uci.ics.textdb.dataflow.common.DictionaryPredicate;
import edu.uci.ics.textdb.dataflow.dictionarymatcher.DictionaryMatcherSourceOperator;
import edu.uci.ics.textdb.plangen.PlanGenUtils;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;

/**
 * DictionarySourceBuilder provides a static function that builds a DictionaryMatcherSourceOperator.
 * 
 * DictionarySourceBuilder currently needs the following properties:
 * 
 *   dictionary (required)
 *   matchingType (required)
 *   
 *   properties required for constructing attributeList, see OperatorBuilderUtils.constructAttributeList
 *   properties required for constructing dataStore, see OperatorBuilderUtils.constructDataStore
 * 
 * 
 * @author Zuozhi Wang
 *
 */
public class DictionarySourceBuilder {
    
    public static String DICTIONARY = "dictionary";
    public static final String MATCHING_TYPE = KeywordMatcherBuilder.MATCHING_TYPE;

    
    public static DictionaryMatcherSourceOperator buildSourceOperator(Map<String, String> operatorProperties) throws PlanGenException {
        String dictionaryStr = OperatorBuilderUtils.getRequiredProperty(DICTIONARY, operatorProperties);
        String matchingTypeStr = OperatorBuilderUtils.getRequiredProperty(MATCHING_TYPE, operatorProperties);
        String tableNameStr = OperatorBuilderUtils.getRequiredProperty(OperatorBuilderUtils.DATA_SOURCE, operatorProperties);

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
        
        DictionaryPredicate predicate = new DictionaryPredicate(
                dictionary, attributeNames, LuceneAnalyzerConstants.getStandardAnalyzer(), matchingType);
                
        DictionaryMatcherSourceOperator sourceOperator = new DictionaryMatcherSourceOperator(predicate, tableNameStr);
                
        return sourceOperator;
    }

}
