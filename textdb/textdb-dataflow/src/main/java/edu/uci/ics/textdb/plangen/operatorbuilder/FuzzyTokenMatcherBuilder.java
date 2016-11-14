package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.common.FuzzyTokenPredicate;
import edu.uci.ics.textdb.dataflow.fuzzytokenmatcher.FuzzyTokenMatcher;
import edu.uci.ics.textdb.plangen.PlanGenUtils;

/**
 * FuzzyTokenMatcherBuilder provides a static function that builds a FuzzyTokenMatcher.
 * 
 * Besides some commonly used properties (properties for attribute list, limit, offset), 
 * FuzzyTokenMatcherBuilder currently needs the following properties:
 * 
 *   query (required)
 *   threshold (required)
 * 
 * @author Zuozhi Wang
 *
 */
public class FuzzyTokenMatcherBuilder {
    public static final String FUZZY_STRING = "fuzzyString";
    public static final String THRESHOLD_RATIO = "thresholdRatio";
    
    /**
     * Builds a FuzzyTokenMatcher according to operatorProperties.
     */
    public static FuzzyTokenMatcher buildOperator(Map<String, String> operatorProperties) throws PlanGenException {
        String query = OperatorBuilderUtils.getRequiredProperty(FUZZY_STRING, operatorProperties);
        String thresholdStr = OperatorBuilderUtils.getRequiredProperty(THRESHOLD_RATIO, operatorProperties);

        // check if query is empty
        PlanGenUtils.planGenAssert(!query.trim().isEmpty(), "query is empty");

        // generate attribute list
        List<Attribute> attributeList = OperatorBuilderUtils.constructAttributeList(operatorProperties);
        
        // generate threshold ratio double
        Double thresholdRatioDouble = generateThresholdDouble(thresholdStr);

        // build FuzzyTokenMatcher
        FuzzyTokenPredicate fuzzyTokenPredicate;
        try {
            fuzzyTokenPredicate = new FuzzyTokenPredicate(query, attributeList,
                    LuceneAnalyzerConstants.getStandardAnalyzer(), thresholdRatioDouble);
        } catch (DataFlowException e) {
            throw new PlanGenException(e.getMessage(), e);
        }
        FuzzyTokenMatcher fuzzyTokenMatcher = new FuzzyTokenMatcher(fuzzyTokenPredicate);

        // set limit and offset
        Integer limitInt = OperatorBuilderUtils.findLimit(operatorProperties);
        if (limitInt != null) {
            fuzzyTokenMatcher.setLimit(limitInt);
        }
        Integer offsetInt = OperatorBuilderUtils.findOffset(operatorProperties);
        if (offsetInt != null) {
            fuzzyTokenMatcher.setOffset(offsetInt);
        }

        return fuzzyTokenMatcher;
    }
    
    private static Double generateThresholdDouble(String thresholdStr) throws PlanGenException {
        Double thresholdDouble = Double.parseDouble(thresholdStr);
        if (thresholdDouble < 0.0 || thresholdDouble > 1.0) {
            throw new PlanGenException("fuzzy token threshold ratio must be between 0.0 and 1.0 (inclusive)");
        }
        return thresholdDouble;
    }
}
