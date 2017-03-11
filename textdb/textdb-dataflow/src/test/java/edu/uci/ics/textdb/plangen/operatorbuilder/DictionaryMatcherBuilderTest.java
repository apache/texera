package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import edu.uci.ics.textdb.api.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.dictionarymatcher.DictionaryMatcher;
import junit.framework.Assert;

public class DictionaryMatcherBuilderTest {
    
    /*
     * test DictionaryMatcherBuilder with the following properties:
     *   keyword: Irvine
     *   attribute names: {city}, {location}, {content}
     *   matching type: PHRASE_INDEXBASED
     *   limit: 10
     *   offset: 2
     * 
     */
    @Test
    public void testDictionaryMatcherBuilder() throws PlanGenException, DataFlowException {
        String dictionaryStr = "Irvine, Anaheim, Costa Mesa, Santa Ana";
        List<String> dictionaryList = Arrays.asList(
                "Irvine",
                "Anaheim",
                "Costa Mesa",
                "Santa Ana");
        
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(DictionaryMatcherBuilder.DICTIONARY, dictionaryStr);
        operatorProperties.put(DictionaryMatcherBuilder.MATCHING_TYPE, "PHRASE_INDEXBASED");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        operatorProperties.put(OperatorBuilderUtils.LIMIT, "10");
        operatorProperties.put(OperatorBuilderUtils.OFFSET, "2");
        
        DictionaryMatcher dictionaryMatcher = DictionaryMatcherBuilder.buildOperator(operatorProperties);
               
        String dictEntry = null;
        ArrayList<String> actualDict = new ArrayList<>();
        while ((dictEntry = dictionaryMatcher.getPredicate().getNextDictionaryEntry()) != null) {
            actualDict.add(dictEntry);
        }
        Assert.assertEquals(dictionaryList.toString(), actualDict.toString());
        List<String> irvineAttrNames = Arrays.asList("city", "location", "content");
        
        Assert.assertEquals(irvineAttrNames, dictionaryMatcher.getPredicate().getAttributeNames());
        Assert.assertEquals(KeywordMatchingType.PHRASE_INDEXBASED, dictionaryMatcher.getPredicate().getKeywordMatchingType());
        Assert.assertEquals(10, dictionaryMatcher.getLimit());
        Assert.assertEquals(2, dictionaryMatcher.getOffset()); 
    }
    
    /*
     * test invalid DictionaryMatcherBuilder by a missing dictionary property
     * 
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidDictionaryMatcherBuilder() throws PlanGenException, DataFlowException {
        
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(DictionaryMatcherBuilder.MATCHING_TYPE, "PHRASE_INDEXBASED");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        operatorProperties.put(OperatorBuilderUtils.LIMIT, "10");
        operatorProperties.put(OperatorBuilderUtils.OFFSET, "2");
        
        DictionaryMatcherBuilder.buildOperator(operatorProperties);
    }

}
