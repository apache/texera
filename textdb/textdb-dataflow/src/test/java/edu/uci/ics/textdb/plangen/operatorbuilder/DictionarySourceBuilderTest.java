package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.textdb.api.exception.PlanGenException;
import edu.uci.ics.textdb.api.exception.StorageException;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.dataflow.dictionarymatcher.DictionaryMatcherSourceOperator;
import edu.uci.ics.textdb.storage.RelationManager;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;
import junit.framework.Assert;

public class DictionarySourceBuilderTest {
    
    public static final String TEST_TABLE = "dictionary_source_buidler_test_table";
    
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
    public void testDictionarySourceBuilder1() throws Exception {

        String dictionaryStr = "Irvine, Anaheim, Costa Mesa, Santa Ana";
        List<String> dictionaryList = Arrays.asList(
                "Irvine",
                "Anaheim",
                "Costa Mesa",
                "Santa Ana");
        List<String> dictAttrNames = Arrays.asList("city", "location", "content");

        
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(DictionaryMatcherBuilder.DICTIONARY, dictionaryStr);
        operatorProperties.put(DictionaryMatcherBuilder.MATCHING_TYPE, "PHRASE_INDEXBASED");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        operatorProperties.put(OperatorBuilderUtils.DATA_SOURCE, TEST_TABLE);
        
        DictionaryMatcherSourceOperator sourceOperator = DictionarySourceBuilder.buildSourceOperator(operatorProperties);
        
        String dictionaryEntry = null;
        ArrayList<String> actualDictionary = new ArrayList<>();
        while ((dictionaryEntry = sourceOperator.getPredicate().getNextDictionaryEntry()) != null) {
            actualDictionary.add(dictionaryEntry);
        }
        
        // compare dict
        Assert.assertEquals(dictionaryList, actualDictionary);
        // compare table name
        Assert.assertEquals(TEST_TABLE, sourceOperator.getTableName());
        // compare dictMatcher attribute list
        Assert.assertEquals(dictAttrNames, sourceOperator.getPredicate().getAttributeNames());

    }
    
    /*
     * Test invalid DictionaryMatcherSourceOperator with an empty data source (table name)
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidBuilder1() throws Exception {       
        String dictionaryStr = "Irvine, Anaheim, Costa Mesa, Santa Ana";
   
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(DictionaryMatcherBuilder.DICTIONARY, dictionaryStr);
        operatorProperties.put(DictionaryMatcherBuilder.MATCHING_TYPE, "PHRASE_INDEXBASED");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "STRING, STRING, TEXT");
        operatorProperties.put(OperatorBuilderUtils.DATA_SOURCE, "");
        
        DictionarySourceBuilder.buildSourceOperator(operatorProperties);
    }
    
    /*
     * Test invalid DictionaryMatcherSourceOperator without the data source attribute
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidBuilder2() throws Exception {
         
        String dictionaryStr = "Irvine, Anaheim, Costa Mesa, Santa Ana";
   
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(DictionaryMatcherBuilder.DICTIONARY, dictionaryStr);
        operatorProperties.put(DictionaryMatcherBuilder.MATCHING_TYPE, "PHRASE_INDEXBASED");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "STRING, STRING, TEXT");
        
        DictionarySourceBuilder.buildSourceOperator(operatorProperties);
    }

}
