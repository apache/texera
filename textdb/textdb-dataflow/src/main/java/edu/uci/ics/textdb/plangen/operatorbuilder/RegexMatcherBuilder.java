package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.util.List;
import java.util.Map;

import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.common.RegexPredicate;
import edu.uci.ics.textdb.dataflow.regexmatch.RegexMatcher;
import edu.uci.ics.textdb.plangen.PlanGenUtils;

/**
 * RegexMatcherBuilder provides a static function that builds a RegexMatcher.
 * 
 * Besides some commonly used properties (properties for attribute list, limit, offset), 
 * RegexMatcherBuilder currently needs the following properties:
 * 
 *   regex (required)
 * 
 * @author Zuozhi Wang
 *
 */
public class RegexMatcherBuilder {
    
    public static final String REGEX = "regex";
    
    /**
     * Builds a RegexMatcher according to operatorProperties.
     */
    public static RegexMatcher buildRegexMatcher(Map<String, String> operatorProperties) throws PlanGenException {
        String regex = OperatorBuilderUtils.getRequiredProperty(REGEX, operatorProperties);

        // check if regex is empty
        PlanGenUtils.planGenAssert(!regex.trim().isEmpty(), "regex is empty");

        // generate attribute names
        List<String> attributeNames = OperatorBuilderUtils.constructAttributeNames(operatorProperties);

        // build RegexMatcher
        RegexPredicate regexPredicate;
        try {
            regexPredicate = new RegexPredicate(regex, attributeNames,
                    LuceneAnalyzerConstants.getNGramAnalyzer(3));
        } catch (DataFlowException e) {
            throw new PlanGenException(e.getMessage(), e);
        }
        RegexMatcher regexMatcher = new RegexMatcher(regexPredicate);

        // set limit and offset
        Integer limitInt = OperatorBuilderUtils.findLimit(operatorProperties);
        if (limitInt != null) {
            regexMatcher.setLimit(limitInt);
        }
        Integer offsetInt = OperatorBuilderUtils.findOffset(operatorProperties);
        if (offsetInt != null) {
            regexMatcher.setOffset(offsetInt);
        }

        return regexMatcher;
    }

}
