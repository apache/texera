package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.util.Arrays;
import java.util.HashMap;

import edu.uci.ics.textdb.api.common.AttributeType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcherSourceOperator;
import edu.uci.ics.textdb.storage.RelationManager;
import junit.framework.Assert;

public class KeywordSourceBuilderTest {
    
    public static final String TEST_TABLE = "keyword_source_buidler_test_table";
    
    public static final Schema TEST_SCHEMA = new Schema(
            new Attribute("city", AttributeType.STRING), new Attribute("location", AttributeType.STRING),
            new Attribute("content", AttributeType.TEXT));
    
    @BeforeClass
    public static void setUp() throws StorageException {
        RelationManager.getRelationManager().createTable(
                TEST_TABLE, "../index/test_tables/"+TEST_TABLE,
                TEST_SCHEMA, LuceneAnalyzerConstants.standardAnalyzerString());
    }
    
    @AfterClass
    public static void cleanUp() throws StorageException {
        RelationManager.getRelationManager().deleteTable(TEST_TABLE);
    }
    
    @Test
    public void testKeywordSourceBuilder1() throws Exception {        
        String keyword = "Irvine";
        
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(KeywordMatcherBuilder.KEYWORD, keyword);
        operatorProperties.put(KeywordMatcherBuilder.MATCHING_TYPE, "PHRASE_INDEXBASED");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        operatorProperties.put(OperatorBuilderUtils.DATA_SOURCE, TEST_TABLE);
        
        KeywordMatcherSourceOperator sourceOperator = KeywordSourceBuilder.buildSourceOperator(operatorProperties);

        
        // compare the keyword
        Assert.assertEquals(keyword, sourceOperator.getPredicate().getQuery());
        // compare the data source (table name)
        Assert.assertEquals(TEST_TABLE, sourceOperator.getTableName());
        // compare the keyword matcher attribute list
        Assert.assertEquals(
                Arrays.asList("city", "location", "content"),
                sourceOperator.getPredicate().getAttributeNames());

    }
    
    /*
     * Test invalid KeywordMatcherSourceOperator with an empty data source
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidBuilder3() throws Exception {
        String KeywordStr = "Irvine";
   
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(KeywordMatcherBuilder.KEYWORD, KeywordStr);
        operatorProperties.put(KeywordMatcherBuilder.MATCHING_TYPE, "PHRASE_INDEXBASED");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        operatorProperties.put(OperatorBuilderUtils.DATA_SOURCE, "  ");
        
        KeywordSourceBuilder.buildSourceOperator(operatorProperties);
    }
    
    /*
     * Test invalid KeywordMatcherSourceOperator without a data source attribute
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidBuilder4() throws Exception {
        String KeywordStr = "Irvine";
   
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(KeywordMatcherBuilder.KEYWORD, KeywordStr);
        operatorProperties.put(KeywordMatcherBuilder.MATCHING_TYPE, "PHRASE_INDEXBASED");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        
        KeywordSourceBuilder.buildSourceOperator(operatorProperties);
    }

}
