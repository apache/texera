package edu.uci.ics.textdb.jsonplangen;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcher;

/**
 * KeywordMatcherBuilder is an OperatorBuilder that builds a KeywordMatcher.
 * 
 * Besides commonly used properties that are defined in OperatorBuilder, 
 * KeywordMatcherBuilder currently needs the following properties:
 * 
 *   keyword (required)
 *   matchingType (required)
 * 
 * @author Zuozhi Wang
 *
 */
public class KeywordMatcherBuilder extends OperatorBuilder {

    public static final String KEYWORD = "keyword";
    public static final String MATCHING_TYPE = "matchingType";

    public KeywordMatcherBuilder(String operatorID, Map<String, String> operatorProperties) {
        super(operatorID, operatorProperties);
    }

    /**
     * Builds a KeywordMatcher according to
     */
    @Override
    public KeywordMatcher build() throws PlanGenException, DataFlowException {
        String keyword = getRequiredProperty(KEYWORD);
        String matchingTypeStr = getRequiredProperty(MATCHING_TYPE);

        // check if keyword is empty
        JsonPlanGenUtils.planGenAssert(!keyword.trim().isEmpty(), "keyword is empty");

        // generate attribute list
        List<Attribute> attributeList = constructAttributeList();

        // generate matching type
        JsonPlanGenUtils.planGenAssert(isValidKeywordMatchingType(matchingTypeStr), "matching type is not valid");
        KeywordMatchingType matchingType = KeywordMatchingType.valueOf(matchingTypeStr.toUpperCase());

        // build KeywordMatcher
        KeywordPredicate keywordPredicate = new KeywordPredicate(keyword, attributeList,
                DataConstants.getStandardAnalyzer(), matchingType);
        KeywordMatcher keywordMatcher = new KeywordMatcher(keywordPredicate);

        // set limit and offset
        Integer limitInt = findLimit();
        if (limitInt != null) {
            keywordMatcher.setLimit(limitInt);
        }
        Integer offsetInt = findOffset();
        if (offsetInt != null) {
            keywordMatcher.setOffset(offsetInt);
        }

        return keywordMatcher;
    }

    private boolean isValidKeywordMatchingType(String matchingTypeStr) {
        return Stream.of(KeywordMatchingType.values()).map(KeywordMatchingType::name)
                .anyMatch(name -> name.toUpperCase().equals(matchingTypeStr.toUpperCase()));
    }

}
