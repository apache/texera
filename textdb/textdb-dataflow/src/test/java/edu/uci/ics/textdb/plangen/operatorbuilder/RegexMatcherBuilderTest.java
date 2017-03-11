package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.regexmatch.RegexMatcher;
import junit.framework.Assert;

/**
 * Tests cases for RegexMatcherBuilder
 * @author Zuozhi Wang
 *
 */
public class RegexMatcherBuilderTest {

    /*
     * test RegexMatcherBuilder with the following properties:
     *   regex: \b(woman)|(man)|(patient)\b
     *   attribute list: {content}
     *   limit: default (Integer.MAX_VALUE)
     *   offset: default (0)
     * 
     */
    @Test
    public void testRegexMatcherBuilder1() throws PlanGenException, DataFlowException, IOException {
        String regex = "\\b(woman)|(man)|(patient)\\b";
        
        HashMap<String, String> regexProperties = new HashMap<>();
        regexProperties.put(RegexMatcherBuilder.REGEX, regex);
        regexProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "content");
        
        RegexMatcher regexMatcher = RegexMatcherBuilder.buildRegexMatcher(regexProperties);
        
        Assert.assertEquals(regex, regexMatcher.getPredicate().getRegex());
        List<String> regexAttrNames = Arrays.asList("content");
        Assert.assertEquals(regexAttrNames, regexMatcher.getPredicate().getAttributeNames());
        Assert.assertEquals(Integer.MAX_VALUE, regexMatcher.getLimit());
        Assert.assertEquals(0, regexMatcher.getOffset());    
    }
    
    /*
     * test RegexMatcherBuilder with the following properties:
     *   regex: (19|20)\d\d[- /.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01])
     *   attribute list: {date}, {amount}, {description}
     *   limit: 100
     *   offset: 11
     * 
     */
    @Test
    public void testRegexMatcherBuilder2() throws PlanGenException, DataFlowException, IOException {
        String regex = "(19|20)\\d\\d[- /.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01])";
        
        HashMap<String, String> regexProperties = new HashMap<>();
        regexProperties.put(RegexMatcherBuilder.REGEX, regex);
        regexProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "date, amount, description");
        regexProperties.put(OperatorBuilderUtils.LIMIT, "100");
        regexProperties.put(OperatorBuilderUtils.OFFSET, "11");
        
        RegexMatcher regexMatcher = RegexMatcherBuilder.buildRegexMatcher(regexProperties);
        
        Assert.assertEquals(regex, regexMatcher.getPredicate().getRegex());
        List<String> regexAttrNames = Arrays.asList("date", "amount", "description");
        Assert.assertEquals(regexAttrNames, regexMatcher.getPredicate().getAttributeNames());
        Assert.assertEquals(100, regexMatcher.getLimit());
        Assert.assertEquals(11, regexMatcher.getOffset());    
    }
    
    /*
     * test invalid RegexMatcherBuilder with missing regex properties
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidRegexMatcherBuilder1() throws PlanGenException, DataFlowException, IOException {
        HashMap<String, String> regexProperties = new HashMap<>();
        regexProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "date, amount, description");
        regexProperties.put(OperatorBuilderUtils.LIMIT, "100");
        regexProperties.put(OperatorBuilderUtils.OFFSET, "11");
        
        RegexMatcherBuilder.buildRegexMatcher(regexProperties); 
    }
    
    /*
     * test invalid RegexMatcherBuilder with empty regex
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidRegexMatcherBuilder2() throws PlanGenException, DataFlowException, IOException {
        HashMap<String, String> regexProperties = new HashMap<>();
        regexProperties.put(RegexMatcherBuilder.REGEX, "");
        regexProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "date, amount, description");
        regexProperties.put(OperatorBuilderUtils.LIMIT, "100");
        regexProperties.put(OperatorBuilderUtils.OFFSET, "11");
        
        RegexMatcherBuilder.buildRegexMatcher(regexProperties); 
    }
}
