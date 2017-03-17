package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.fuzzytokenmatcher.FuzzyTokenMatcher;
import junit.framework.Assert;

/**
 * Tests cases for FuzzyTokenMatcherBuilder
 * @author Zuozhi Wang
 *
 */
public class FuzzyTokenMatcherBuilderTest {
    
    /*
     * test FuzzyTokenMatcherBuilder with the following properties:
     *   query: "test with fuzzy token matcher builder"
     *   thresholdRatio: 0.5
     *   attribute names: {content}
     *   limit: 100
     *   offset: 11
     * 
     */
    @Test
    public void testFuzzyTokenMatcherBuilder1() throws PlanGenException, DataFlowException, IOException {
        String query = "test with fuzzy token matcher builder";
        
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(FuzzyTokenMatcherBuilder.FUZZY_STRING, query);
        operatorProperties.put(FuzzyTokenMatcherBuilder.THRESHOLD_RATIO, "0.5");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "content");
        operatorProperties.put(OperatorBuilderUtils.LIMIT, "100");
        operatorProperties.put(OperatorBuilderUtils.OFFSET, "11");
     
        FuzzyTokenMatcher fuzzyTokenMatcher = FuzzyTokenMatcherBuilder.buildOperator(operatorProperties);
        
        Assert.assertEquals(query, fuzzyTokenMatcher.getPredicate().getQuery());
        Assert.assertEquals(0.5, fuzzyTokenMatcher.getPredicate().getThresholdRatio());
        List<String> attrNames = Arrays.asList("content");
        Assert.assertEquals(attrNames, fuzzyTokenMatcher.getPredicate().getAttributeNames());
        Assert.assertEquals(100, fuzzyTokenMatcher.getLimit());
        Assert.assertEquals(11, fuzzyTokenMatcher.getOffset());   
    }
    
    /*
     * test FuzzyTokenMatcherBuilder a corner case thresholdRatio: 1
     * 
     */
    @Test
    public void testFuzzyTokenMatcherBuilder2() throws PlanGenException, DataFlowException, IOException {
        String query = "test with fuzzy token matcher builder";
        
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(FuzzyTokenMatcherBuilder.FUZZY_STRING, query);
        operatorProperties.put(FuzzyTokenMatcherBuilder.THRESHOLD_RATIO, "1");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "content");
        operatorProperties.put(OperatorBuilderUtils.LIMIT, "100");
        operatorProperties.put(OperatorBuilderUtils.OFFSET, "11");
     
        FuzzyTokenMatcher fuzzyTokenMatcher = FuzzyTokenMatcherBuilder.buildOperator(operatorProperties);
        
        Assert.assertEquals(query, fuzzyTokenMatcher.getPredicate().getQuery());
        Assert.assertEquals(1.0, fuzzyTokenMatcher.getPredicate().getThresholdRatio());
        List<String> attrNames = Arrays.asList("content");
        Assert.assertEquals(attrNames, fuzzyTokenMatcher.getPredicate().getAttributeNames());
        Assert.assertEquals(100, fuzzyTokenMatcher.getLimit());
        Assert.assertEquals(11, fuzzyTokenMatcher.getOffset());   
    }
    
    /*
     * test FuzzyTokenMatcherBuilder a corner case thresholdRatio: 0
     * 
     */
    @Test
    public void testFuzzyTokenMatcherBuilder3() throws PlanGenException, DataFlowException, IOException {
        String query = "test with fuzzy token matcher builder";
        
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(FuzzyTokenMatcherBuilder.FUZZY_STRING, query);
        operatorProperties.put(FuzzyTokenMatcherBuilder.THRESHOLD_RATIO, "0");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "content");
        operatorProperties.put(OperatorBuilderUtils.LIMIT, "100");
        operatorProperties.put(OperatorBuilderUtils.OFFSET, "11");
     
        FuzzyTokenMatcher fuzzyTokenMatcher = FuzzyTokenMatcherBuilder.buildOperator(operatorProperties);
        
        Assert.assertEquals(query, fuzzyTokenMatcher.getPredicate().getQuery());
        Assert.assertEquals(0.0, fuzzyTokenMatcher.getPredicate().getThresholdRatio());
        List<String> attrNames = Arrays.asList("content");
        Assert.assertEquals(attrNames, fuzzyTokenMatcher.getPredicate().getAttributeNames());
        Assert.assertEquals(100, fuzzyTokenMatcher.getLimit());
        Assert.assertEquals(11, fuzzyTokenMatcher.getOffset());   
    }
    
    /*
     * test invalid FuzzyTokenMatcherBuilder by missing a query property
     * 
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidFuzzyTokenMatcherBuilder1() throws PlanGenException, DataFlowException, IOException {
        
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(FuzzyTokenMatcherBuilder.THRESHOLD_RATIO, "0.5");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "content");
        operatorProperties.put(OperatorBuilderUtils.LIMIT, "100");
        operatorProperties.put(OperatorBuilderUtils.OFFSET, "11");
       
        FuzzyTokenMatcherBuilder.buildOperator(operatorProperties); 
    }
    
    /*
     * test invalid FuzzyTokenMatcherBuilder by an empty query
     * 
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidFuzzyTokenMatcherBuilder2() throws PlanGenException, DataFlowException, IOException {
        String query = "";
        
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(FuzzyTokenMatcherBuilder.FUZZY_STRING, query);
        operatorProperties.put(FuzzyTokenMatcherBuilder.THRESHOLD_RATIO, "0.5");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "content");
        operatorProperties.put(OperatorBuilderUtils.LIMIT, "100");
        operatorProperties.put(OperatorBuilderUtils.OFFSET, "11");
       
        FuzzyTokenMatcherBuilder.buildOperator(operatorProperties); 
    }
    
    /*
     * test invalid FuzzyTokenMatcherBuilder by missing a thresholdRatio property
     * 
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidFuzzyTokenMatcherBuilder3() throws PlanGenException, DataFlowException, IOException {
        String query = "test with fuzzy token matcher builder";
        
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(FuzzyTokenMatcherBuilder.FUZZY_STRING, query);
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "content");
        operatorProperties.put(OperatorBuilderUtils.LIMIT, "100");
        operatorProperties.put(OperatorBuilderUtils.OFFSET, "11");
       
        FuzzyTokenMatcherBuilder.buildOperator(operatorProperties); 
    }

    /*
     * test invalid FuzzyTokenMatcherBuilder by an invalid thresholdRatio property
     * 
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidFuzzyTokenMatcherBuilder4() throws PlanGenException, DataFlowException, IOException {
        String query = "test with fuzzy token matcher builder";
        
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(FuzzyTokenMatcherBuilder.FUZZY_STRING, query);
        operatorProperties.put(FuzzyTokenMatcherBuilder.THRESHOLD_RATIO, "100");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "content");
        operatorProperties.put(OperatorBuilderUtils.LIMIT, "100");
        operatorProperties.put(OperatorBuilderUtils.OFFSET, "11");
       
        FuzzyTokenMatcherBuilder.buildOperator(operatorProperties); 
    }
    
    /*
     * test invalid FuzzyTokenMatcherBuilder with an invalid thresholdRatio property
     * 
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidFuzzyTokenMatcherBuilder5() throws PlanGenException, DataFlowException, IOException {
        String query = "test with fuzzy token matcher builder";
        
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(FuzzyTokenMatcherBuilder.FUZZY_STRING, query);
        operatorProperties.put(FuzzyTokenMatcherBuilder.THRESHOLD_RATIO, "-1.2");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "content");
        operatorProperties.put(OperatorBuilderUtils.LIMIT, "100");
        operatorProperties.put(OperatorBuilderUtils.OFFSET, "11");
       
        FuzzyTokenMatcherBuilder.buildOperator(operatorProperties); 
    }
    

}
