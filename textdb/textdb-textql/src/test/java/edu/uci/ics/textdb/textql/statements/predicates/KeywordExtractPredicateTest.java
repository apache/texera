package edu.uci.ics.textdb.textql.statements.predicates;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.textdb.api.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.web.request.beans.KeywordMatcherBean;
import edu.uci.ics.textdb.web.request.beans.OperatorBean;

/**
 * This class contains test cases for the KeywordExtractPredicate class.
 * The constructor, getters, setters and the generateOperatorBean methods are
 * tested.
 * 
 * @author Flavio Bayer
 *
 */
public class KeywordExtractPredicateTest {

    /**
     * Test the class constructor, getter and the setter methods.
     * Call the constructor of the KeywordExtractPredicate, test 
     * if the returned value by the getter is the same as used in 
     * the constructor and then test if the value is changed
     * when the setter method is invoked.
     */
    @Test
    public void testConstructorsGettersSetters(){
        List<String> matchingFields;
        String keywords;
        String matchingType;

        matchingFields = null;
        keywords = null;
        matchingType = null;
        assertConstructorGettersSetters(matchingFields, keywords, matchingType);
        
        matchingFields = Arrays.asList();
        keywords = "xxx";
        matchingType = KeywordMatchingType.PHRASE_INDEXBASED.toString();
        assertConstructorGettersSetters(matchingFields, keywords, matchingType);
        
        matchingFields = Arrays.asList("a", "b");
        keywords = "new york";
        matchingType = KeywordMatchingType.CONJUNCTION_INDEXBASED.toString();
        assertConstructorGettersSetters(matchingFields, keywords, matchingType);
        
        matchingFields = Arrays.asList("field1", "field0", "field2");
        keywords = "university of california irvine";
        matchingType = KeywordMatchingType.SUBSTRING_SCANBASED.toString();
        assertConstructorGettersSetters(matchingFields, keywords, matchingType);
    }

    /**
     * Assert the correctness of the Constructor, getter and setter methods.
     * Test the constructor with the given parameters, the constructor without
     * parameters, the setter with the given values, the setter with null
     * values and the getter methods.
     * @param matchingFields The matchingFields of the KeywordExtractPredicate.
     * @param keywords The keywords of the KeywordExtractPredicate.
     * @param matchingType The matchingType of the KeywordExtractPredicate.
     */
    private void assertConstructorGettersSetters(List<String> matchingFields, String keywords, String matchingType){
        KeywordExtractPredicate keywordExtractPredicate;
        
        // Check the constructor with the arguments for initialization
        keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
        Assert.assertEquals(keywordExtractPredicate.getMatchingFields(), matchingFields);
        Assert.assertEquals(keywordExtractPredicate.getKeywords(), keywords);
        Assert.assertEquals(keywordExtractPredicate.getMatchingType(), matchingType);
        
        // Check the constructor with no arguments
        keywordExtractPredicate = new KeywordExtractPredicate();
        Assert.assertEquals(keywordExtractPredicate.getMatchingFields(), null);
        Assert.assertEquals(keywordExtractPredicate.getKeywords(), null);
        Assert.assertEquals(keywordExtractPredicate.getMatchingType(), null);
        
        // Check all the setters with the given values
        keywordExtractPredicate.setMatchingFields(matchingFields);
        Assert.assertEquals(keywordExtractPredicate.getMatchingFields(), matchingFields);
        keywordExtractPredicate.setKeywords(keywords);
        Assert.assertEquals(keywordExtractPredicate.getKeywords(), keywords);
        keywordExtractPredicate.setMatchingType(matchingType);
        Assert.assertEquals(keywordExtractPredicate.getMatchingType(), matchingType);
        
        // Check all the setters with null values
        keywordExtractPredicate.setMatchingFields(null);
        Assert.assertEquals(keywordExtractPredicate.getMatchingFields(), null);
        keywordExtractPredicate.setKeywords(null);
        Assert.assertEquals(keywordExtractPredicate.getKeywords(), null);
        keywordExtractPredicate.setMatchingType(null);
        Assert.assertEquals(keywordExtractPredicate.getMatchingType(), null);
    }

    /**
     * Test the generateOperatorBean method.
     * Build a KeywordExtractPredicate, invoke the generateOperatorBean and
     * check whether a KeywordMatcherBean with the right attributes is returned.
     * An empty list is used as the list of fields to perform the match.
     */
    @Test
    public void testGenerateOperatorBean00() {
        String operatorId = "xxx";
        List<String> matchingFields = Collections.emptyList();
        String keywords = "keyword";
        String matchingType = KeywordMatchingType.CONJUNCTION_INDEXBASED.toString();
        KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
        
        OperatorBean computedProjectionBean = keywordExtractPredicate.generateOperatorBean(operatorId);
        String matchingFieldsAsString = String.join(",", matchingFields);
        OperatorBean expectedProjectionBean = new KeywordMatcherBean(operatorId, "KeywordMatcher", matchingFieldsAsString,
                            null, null, keywords, matchingType);
        
        Assert.assertEquals(expectedProjectionBean, computedProjectionBean);
    }
    
    /**
     * Test the generateOperatorBean method.
     * Build a KeywordExtractPredicate, invoke the generateOperatorBean and
     * check whether a KeywordMatcherBean with the right attributes is returned.
     * A list with one field is used as the list of fields to perform the match.
     */
    @Test
    public void testGenerateOperatorBean01() {
        String operatorId = "operator";
        List<String> matchingFields = Arrays.asList("fieldOne");
        String keywords = "keyword(s)";
        String matchingType = KeywordMatchingType.PHRASE_INDEXBASED.toString();
        KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
        
        OperatorBean computedProjectionBean = keywordExtractPredicate.generateOperatorBean(operatorId);
        String matchingFieldsAsString = String.join(",", matchingFields);
        OperatorBean expectedProjectionBean = new KeywordMatcherBean(operatorId, "KeywordMatcher", matchingFieldsAsString,
                            null, null, keywords, matchingType);
        
        Assert.assertEquals(expectedProjectionBean, computedProjectionBean);
    }
    
    /**
     * Test the generateOperatorBean method.
     * Build a KeywordExtractPredicate, invoke the generateOperatorBean and
     * check whether a KeywordMatcherBean with the right attributes is returned.
     * A list with some fields is used as the list of fields to perform the match.
     */
    @Test
    public void testGenerateOperatorBean02() {
        String operatorId = "keywordExtract00";
        List<String> matchingFields = Arrays.asList("field0", "field1");
        String keywords = "xxx";
        String matchingType = KeywordMatchingType.SUBSTRING_SCANBASED.toString();
        KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
        
        OperatorBean computedProjectionBean = keywordExtractPredicate.generateOperatorBean(operatorId);
        String matchingFieldsAsString = String.join(",", matchingFields);
        OperatorBean expectedProjectionBean = new KeywordMatcherBean(operatorId, "KeywordMatcher",
                            matchingFieldsAsString, null, null, keywords, matchingType);
        
        Assert.assertEquals(expectedProjectionBean, computedProjectionBean);
    }
    
}
