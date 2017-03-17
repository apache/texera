package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import edu.uci.ics.textdb.api.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.nlpextrator.NlpExtractor;
import edu.uci.ics.textdb.dataflow.nlpextrator.NlpPredicate;
import edu.uci.ics.textdb.plangen.PlanGenUtils;

/**
 * DictionaryMatcherBuilder provides a static function that builds a DictionaryMatcher.
 * 
 * Besides some commonly used properties (properties for attribute list, limit, offset), 
 * DictionaryMatcherBuilder currently needs the following properties:
 * 
 *   nlpType (required)
 * 
 * @author Zuozhi Wang
 *
 */
public class NlpExtractorBuilder {
    
    public static final String NLP_TYPE = "nlpType";
    
    /**
     * Builds a NlpExtractor according to operatorProperties.
     */
    public static NlpExtractor buildOperator(Map<String, String> operatorProperties) throws PlanGenException {
        String nlpTypeStr = OperatorBuilderUtils.getRequiredProperty(NLP_TYPE, operatorProperties);

        // check if nlpType is valid
        PlanGenUtils.planGenAssert(isValidNlpType(nlpTypeStr), "invalid NlpType");

        // generate attribute list
        List<String> attributeNames = OperatorBuilderUtils.constructAttributeNames(operatorProperties);

        // build NlpExtractor
        NlpPredicate predicate = new NlpPredicate(convertToNlpType(nlpTypeStr), attributeNames);
        NlpExtractor operator = new NlpExtractor(predicate);

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
    
    private static boolean isValidNlpType(String nlpTypeStr) {
        return Stream.of(NlpPredicate.NlpTokenType.values()).map(NlpPredicate.NlpTokenType::name)
                .anyMatch(name -> name.toLowerCase().equals(nlpTypeStr.toLowerCase()));    
    }
    
    private static NlpPredicate.NlpTokenType convertToNlpType(String nlpTypeStr) {
        return Stream.of(NlpPredicate.NlpTokenType.values())
                .filter(nlpType -> nlpType.toString().toLowerCase().equals(nlpTypeStr.toLowerCase()))
                .findAny().orElse(null);
    }
    
}
