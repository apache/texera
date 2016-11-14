package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.json.JSONObject;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcherSourceOperator;
import junit.framework.Assert;

public class KeywordSourceBuilderTest {
    
    @Test
    public void testKeywordSourceBuilder1() throws Exception {
        String directoryStr = "./index";
        JSONObject schemaJsonJSONObject = new JSONObject();
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "id, city, location, content");
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "integer, string, string, text");
                
        List<Attribute> schemaAttrs = Arrays.asList(
                new Attribute("id", FieldType.INTEGER),
                new Attribute("city", FieldType.STRING),
                new Attribute("location", FieldType.STRING),
                new Attribute("content", FieldType.TEXT));
        
        String keyword = "Irvine";
        List<Attribute> keywordAttributes = schemaAttrs.stream()
                .filter(attr -> ! attr.getFieldName().equals("id")).collect(Collectors.toList());

        
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(KeywordMatcherBuilder.KEYWORD, keyword);
        operatorProperties.put(KeywordMatcherBuilder.MATCHING_TYPE, "PHRASE_INDEXBASED");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        operatorProperties.put(OperatorBuilderUtils.DATA_DIRECTORY, directoryStr);
        operatorProperties.put(OperatorBuilderUtils.SCHEMA, schemaJsonJSONObject.toString());
        
        KeywordMatcherSourceOperator sourceOperator = KeywordSourceBuilder.buildSourceOperator(operatorProperties);

        
        // compare the keyword
        Assert.assertEquals(keyword, sourceOperator.getPredicate().getQuery());
        // compare the dataStore directory
        Assert.assertEquals(directoryStr, sourceOperator.getDataStore().getDataDirectory());
        // compare the dataStore schema
        Assert.assertEquals(
                schemaAttrs.stream().collect(Collectors.toList()).toString(), 
                sourceOperator.getDataStore().getSchema().getAttributes().stream().collect(Collectors.toList()).toString());
        // compare the keyword matcher attribute list
        Assert.assertEquals(
                Utils.getAttributeNames(keywordAttributes).toString(),
                sourceOperator.getPredicate().getAttributeNames().toString());

    }
    
    /*
     * Test invalid KeywordMatcherSourceOperator with an invalid attribute type
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidBuilder1() throws Exception {
        String directoryStr = "./index";
        JSONObject schemaJsonJSONObject = new JSONObject();
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "id, city, location, content");
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "random_type, string, type-that-doesnt-exist, text");
         
        String KeywordStr = "Irvine";
   
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(KeywordMatcherBuilder.KEYWORD, KeywordStr);
        operatorProperties.put(KeywordMatcherBuilder.MATCHING_TYPE, "PHRASE_INDEXBASED");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        operatorProperties.put(OperatorBuilderUtils.DATA_DIRECTORY, directoryStr);
        operatorProperties.put(OperatorBuilderUtils.SCHEMA, schemaJsonJSONObject.toString());
        
        KeywordSourceBuilder.buildSourceOperator(operatorProperties);
    }
    
    /*
     * Test invalid KeywordMatcherSourceOperator with inconsistent attribute names and types
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidBuilder2() throws Exception {
        String directoryStr = "./index";
        JSONObject schemaJsonJSONObject = new JSONObject();
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "id, city, location, content");
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "integer, string, text");
         
        String KeywordStr = "Irvine";
   
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(KeywordMatcherBuilder.KEYWORD, KeywordStr);
        operatorProperties.put(KeywordMatcherBuilder.MATCHING_TYPE, "PHRASE_INDEXBASED");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        operatorProperties.put(OperatorBuilderUtils.DATA_DIRECTORY, directoryStr);
        operatorProperties.put(OperatorBuilderUtils.SCHEMA, schemaJsonJSONObject.toString());
        
        KeywordSourceBuilder.buildSourceOperator(operatorProperties);
    }
    
    /*
     * Test invalid KeywordMatcherSourceOperator with an empty directory
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidBuilder3() throws Exception {
        String directoryStr = "";
        JSONObject schemaJsonJSONObject = new JSONObject();
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "id, city, location, content");
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "integer, string, text");
         
        String KeywordStr = "Irvine";
   
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(KeywordMatcherBuilder.KEYWORD, KeywordStr);
        operatorProperties.put(KeywordMatcherBuilder.MATCHING_TYPE, "PHRASE_INDEXBASED");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        operatorProperties.put(OperatorBuilderUtils.DATA_DIRECTORY, directoryStr);
        operatorProperties.put(OperatorBuilderUtils.SCHEMA, schemaJsonJSONObject.toString());
        
        KeywordSourceBuilder.buildSourceOperator(operatorProperties);
    }
    
    /*
     * Test invalid KeywordMatcherSourceOperator with an empty directory with only spaces
     */
    @Test(expected = PlanGenException.class)
    public void testInvalidBuilder4() throws Exception {
        String directoryStr = "      ";
        JSONObject schemaJsonJSONObject = new JSONObject();
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "id, city, location, content");
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "integer, string, text");
         
        String KeywordStr = "Irvine";
   
        HashMap<String, String> operatorProperties = new HashMap<>();
        operatorProperties.put(KeywordMatcherBuilder.KEYWORD, KeywordStr);
        operatorProperties.put(KeywordMatcherBuilder.MATCHING_TYPE, "PHRASE_INDEXBASED");
        operatorProperties.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        operatorProperties.put(OperatorBuilderUtils.DATA_DIRECTORY, directoryStr);
        operatorProperties.put(OperatorBuilderUtils.SCHEMA, schemaJsonJSONObject.toString());
        
        KeywordSourceBuilder.buildSourceOperator(operatorProperties);
    }

}
