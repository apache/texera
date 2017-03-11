package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.util.List;
import java.util.Map;

import edu.uci.ics.textdb.api.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.PlanGenException;
import edu.uci.ics.textdb.api.exception.StorageException;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcherSourceOperator;
import edu.uci.ics.textdb.plangen.PlanGenUtils;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;

/**
 * KeywordSourceBuilder provides a static function that builds a KeywordMatcherSourceOperator.
 * 
 * KeywordSourceBuilder currently needs the following properties:
 * 
 *   keyword (required)
 *   matchingType (required)
 *   
 *   properties required for constructing attributeList, see OperatorBuilderUtils.constructAttributeList
 *   properties required for constructing dataStore, see OperatorBuilderUtils.constructDataStore
 * 
 * 
 * @author Zuozhi Wang
 *
 */
public class KeywordSourceBuilder {
    
    public static KeywordMatcherSourceOperator buildSourceOperator(Map<String, String> operatorProperties) 
            throws PlanGenException {
        String keyword = OperatorBuilderUtils.getRequiredProperty(
                KeywordMatcherBuilder.KEYWORD, operatorProperties);
        String matchingTypeStr = OperatorBuilderUtils.getRequiredProperty(
                KeywordMatcherBuilder.MATCHING_TYPE, operatorProperties);
        String tableNameStr = OperatorBuilderUtils.getRequiredProperty(
                OperatorBuilderUtils.DATA_SOURCE, operatorProperties);

        // check if the keyword is empty
        PlanGenUtils.planGenAssert(!keyword.trim().isEmpty(), "the keyword is empty");

        // generate the attribute names
        List<String> attributeNames = OperatorBuilderUtils.constructAttributeNames(operatorProperties);

        // generate the keyword matching type
        KeywordMatchingType matchingType = KeywordMatcherBuilder.getKeywordMatchingType(matchingTypeStr);
        PlanGenUtils.planGenAssert(matchingType != null, 
                "the matching type: "+ matchingTypeStr +" is not valid. "
                + "It must be one of " + KeywordMatcherBuilder.keywordMatchingTypeMap.keySet());
        
        KeywordPredicate keywordPredicate;
        keywordPredicate = new KeywordPredicate(keyword, attributeNames,
                LuceneAnalyzerConstants.getStandardAnalyzer(), matchingType);  
        
        KeywordMatcherSourceOperator sourceOperator;
        try {
            sourceOperator = new KeywordMatcherSourceOperator(keywordPredicate, tableNameStr);
        } catch (DataFlowException | StorageException e) {
            throw new PlanGenException(e.getMessage(), e);
        }
        
        // set limit and offset
        Integer limitInt = OperatorBuilderUtils.findLimit(operatorProperties);
        if (limitInt != null) {
            sourceOperator.setLimit(limitInt);
        }
        Integer offsetInt = OperatorBuilderUtils.findOffset(operatorProperties);
        if (offsetInt != null) {
            sourceOperator.setOffset(offsetInt);
        }
   
        return sourceOperator;
    }

}
