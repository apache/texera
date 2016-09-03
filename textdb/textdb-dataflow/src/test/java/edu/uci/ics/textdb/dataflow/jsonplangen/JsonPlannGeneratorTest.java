package edu.uci.ics.textdb.dataflow.jsonplangen;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcher;
import edu.uci.ics.textdb.jsonplangen.JsonPlanGenerator;
import junit.framework.Assert;

/**
 * Tests for JsonPlannerGenerator
 * 
 * @author Zuozhi Wang
 *
 */
public class JsonPlannGeneratorTest {

    
    /*
     * Test generating a KeywordMatcher according to JSON query.
     * JSON query only contains keyword matchers. Links of operators are not tested here.
     */
    @Test
    public void testGenerateKeywordMatcher() throws Exception {
        JsonPlanGenerator planGen = new JsonPlanGenerator();
        planGen.generateQueryPlan(SampleJsonQuery.sampleJsonQueryKeywordMatcher);
        Map<String, IOperator> operatorMap = planGen.getOperatorMap();
        
        // test KeywordMatcher with ID: keyword_zika
        // this KeywordMatcher should have:
        // keyword: zika
        // attribute list: {content, TEXT}
        // matching type: conjunction_indexbased
        // limit: Integer.MAX_VALUE
        // offset: 0
        Assert.assertTrue(operatorMap.containsKey("keyword_zika"));
        KeywordMatcher keywordZika = (KeywordMatcher) operatorMap.get("keyword_zika");
        
        Assert.assertEquals("zika", keywordZika.getPredicate().getQuery());
        List<Attribute> zikaAttrList = Arrays.asList(
                new Attribute("content", FieldType.TEXT));
        Assert.assertEquals(zikaAttrList.toString(), keywordZika.getPredicate().getAttributeList().toString());
        Assert.assertEquals(KeywordMatchingType.CONJUNCTION_INDEXBASED, keywordZika.getPredicate().getOperatorType());
        Assert.assertEquals(Integer.MAX_VALUE, keywordZika.getLimit());
        Assert.assertEquals(0, keywordZika.getOffset());
        
        // test KeywordMatcher with ID: keyword_irvine
        // this KeywordMatcher should have:
        // keyword: Irvine
        // attribute list: {city, STRING}, {location, STRING}, {content, TEXT}
        // matching type: substring_scanbased
        // limit: 10
        // offset: 2
        
        Assert.assertTrue(operatorMap.containsKey("keyword_irvine"));
        KeywordMatcher keywordIrvine = (KeywordMatcher) operatorMap.get("keyword_irvine");
        
        Assert.assertEquals("Irvine", keywordIrvine.getPredicate().getQuery());
        List<Attribute> irvineAttrList = Arrays.asList(
                new Attribute("city", FieldType.STRING),
                new Attribute("location", FieldType.STRING),
                new Attribute("content", FieldType.TEXT));
        Assert.assertEquals(irvineAttrList.toString(), keywordIrvine.getPredicate().getAttributeList().toString());
        Assert.assertEquals(KeywordMatchingType.SUBSTRING_SCANBASED, keywordIrvine.getPredicate().getOperatorType());
        Assert.assertEquals(10, keywordIrvine.getLimit());
        Assert.assertEquals(2, keywordIrvine.getOffset());
    }
    
    @Test(expected = PlanGenException.class)
    public void testInvalidKeywordMatcher1() throws Exception {
        JsonPlanGenerator planGen = new JsonPlanGenerator();
        planGen.generateQueryPlan(SampleJsonQuery.sampleInvalidJsonQueryKeywordMatcher);
    }

}
