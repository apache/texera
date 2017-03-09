package edu.uci.ics.textdb.textql.statements.predicates;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcher;
import edu.uci.ics.textdb.plangen.PlanGenUtils;
import edu.uci.ics.textdb.plangen.operatorbuilder.KeywordMatcherBuilder;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.textql.statements.StatementTestUtils;
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

    /**
     * Test the generateOutputSchema method.
     * This test use an empty Schema as input and no fields for extraction
     * to be performed.
     * The expected output Schema is a schema with the PAYLAOAD and
     * SPAN_LIST attribute.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test
    public void testGenerateOutputSchema00() throws TextDBException {
        List<String> matchingFields = Arrays.asList();
        String keywords = "keyword(s)";
        String matchingType = KeywordMatchingType.CONJUNCTION_INDEXBASED.toString();
        KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
        
        Schema inputSchema = new Schema();
                
        Schema computedOutputSchema = keywordExtractPredicate.generateOutputSchema(inputSchema);
        Schema expectedOutputSchema = new Schema(
                SchemaConstants.PAYLOAD_ATTRIBUTE,
                SchemaConstants.SPAN_LIST_ATTRIBUTE
            );
        
        Assert.assertEquals(expectedOutputSchema, computedOutputSchema);
    }

    /**
     * Test the generateOutputSchema method.
     * This test uses a Schema with all the values of FiledType as input
     * and no field to be matched.
     * The expected output Schema is the input schema with the added 
     * PAYLAOAD and SPAN_LIST attributes.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test
    public void testGenerateOutputSchema01() throws TextDBException {
        List<String> matchingFields = Arrays.asList();
        String keywords = "keyword(s)";
        String matchingType = KeywordMatchingType.PHRASE_INDEXBASED.toString();
        KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
        
        Schema inputSchema = StatementTestUtils.ALL_FIELD_TYPES_SCHEMA;
                
        Schema computedOutputSchema = keywordExtractPredicate.generateOutputSchema(inputSchema);
        Schema expectedOutputSchema = new Schema(Stream.concat(
                StatementTestUtils.ALL_FIELD_TYPES_SCHEMA.getAttributes().stream(),
                Stream.of(SchemaConstants.PAYLOAD_ATTRIBUTE, SchemaConstants.SPAN_LIST_ATTRIBUTE)
            ).toArray(Attribute[]::new));
        
        Assert.assertEquals(expectedOutputSchema, computedOutputSchema);
    }

    /**
     * Test the generateOutputSchema method.
     * This test uses a Schema with attributes in which FiledType is 
     * either String or Text. All the fields in the input Schema are
     * to be matched.
     * The expected output Schema is the input schema with the added
     * PAYLAOAD and SPAN_LIST attributes.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test
    public void testGenerateOutputSchema02() throws TextDBException {
        List<String> matchingFields = Arrays.asList(
                StatementTestUtils.STRING_ATTRIBUTE.getFieldName(),
                StatementTestUtils.TEXT_ATTRIBUTE.getFieldName()
            );
        String keywords = "keyword(s)";
        String matchingType = KeywordMatchingType.SUBSTRING_SCANBASED.toString();
        KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
        
        Schema inputSchema = new Schema(
                StatementTestUtils.STRING_ATTRIBUTE,
                StatementTestUtils.TEXT_ATTRIBUTE
            );
                
        Schema computedOutputSchema = keywordExtractPredicate.generateOutputSchema(inputSchema);
        Schema expectedOutputSchema = new Schema(
                StatementTestUtils.STRING_ATTRIBUTE,
                StatementTestUtils.TEXT_ATTRIBUTE,
                SchemaConstants.PAYLOAD_ATTRIBUTE,
                SchemaConstants.SPAN_LIST_ATTRIBUTE
            );
        
        Assert.assertEquals(expectedOutputSchema, computedOutputSchema);
    }
    
    /**
     * Test the generateOutputSchema method.
     * This test uses a Schema with all the values of FiledType as input
     * and attributes with type String and Text to be extracted.
     * The expected output Schema is the input schema with the added 
     * PAYLAOAD and SPAN_LIST attributes.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test
    public void testGenerateOutputSchema03() throws TextDBException {
        List<String> matchingFields = Arrays.asList(
                StatementTestUtils.STRING_ATTRIBUTE.getFieldName(),
                StatementTestUtils.TEXT_ATTRIBUTE.getFieldName()
            );
        String keywords = "keyword(s)";
        String matchingType = KeywordMatchingType.SUBSTRING_SCANBASED.toString();
        KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
        Schema inputSchema = StatementTestUtils.ALL_FIELD_TYPES_SCHEMA;
                
        Schema computedOutputSchema = keywordExtractPredicate.generateOutputSchema(inputSchema);
        Schema expectedOutputSchema = new Schema(Stream.concat(
                StatementTestUtils.ALL_FIELD_TYPES_SCHEMA.getAttributes().stream(),
                Stream.of(SchemaConstants.PAYLOAD_ATTRIBUTE, SchemaConstants.SPAN_LIST_ATTRIBUTE)
            ).toArray(Attribute[]::new));
        
        Assert.assertEquals(expectedOutputSchema, computedOutputSchema);
    }

    /**
     * Test the generateOutputSchema method.
     * This test uses a Schema with all the values of FiledType and the 
     * PAYLAOAD attribute as input. One field with type String is
     * in the list of fields to perform the extraction.
     * The expected output Schema is the input schema with the added 
     * SPAN_LIST attribute.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test
    public void testGenerateOutputSchema04() throws TextDBException {
        List<String> matchingFields = Arrays.asList(
                StatementTestUtils.STRING_ATTRIBUTE.getFieldName()
            );
        String keywords = "keyword(s)";
        String matchingType = KeywordMatchingType.SUBSTRING_SCANBASED.toString();
        KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
        
        Schema inputSchema = new Schema(Stream.concat(
                StatementTestUtils.ALL_FIELD_TYPES_SCHEMA.getAttributes().stream(),
                Stream.of(SchemaConstants.PAYLOAD_ATTRIBUTE)
            ).toArray(Attribute[]::new));
                
        Schema computedOutputSchema = keywordExtractPredicate.generateOutputSchema(inputSchema);
        Schema expectedOutputSchema = new Schema(Stream.concat(
                StatementTestUtils.ALL_FIELD_TYPES_SCHEMA.getAttributes().stream(),
                Stream.of(SchemaConstants.PAYLOAD_ATTRIBUTE, SchemaConstants.SPAN_LIST_ATTRIBUTE)
            ).toArray(Attribute[]::new));
        
        Assert.assertEquals(expectedOutputSchema, computedOutputSchema);
    }
    
    /**
     * Test the generateOutputSchema method.
     * This test uses a Schema with all the values of FiledType and the 
     * SPAN_LIST attribute as input. One field with type Text is
     * in the list of fields to perform the extraction.
     * The expected output Schema is the input schema with the added 
     * PAYLAOAD attribute.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test
    public void testGenerateOutputSchema05() throws TextDBException {
        List<String> matchingFields = Arrays.asList(
                StatementTestUtils.TEXT_ATTRIBUTE.getFieldName()
            );
        String keywords = "keyword(s)";
        String matchingType = KeywordMatchingType.SUBSTRING_SCANBASED.toString();
        KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
        
        Schema inputSchema = new Schema(Stream.concat(
                StatementTestUtils.ALL_FIELD_TYPES_SCHEMA.getAttributes().stream(),
                Stream.of(SchemaConstants.SPAN_LIST_ATTRIBUTE)
            ).toArray(Attribute[]::new));
                
        Schema computedOutputSchema = keywordExtractPredicate.generateOutputSchema(inputSchema);
        Schema expectedOutputSchema = new Schema(Stream.concat(
                StatementTestUtils.ALL_FIELD_TYPES_SCHEMA.getAttributes().stream(),
                Stream.of(SchemaConstants.SPAN_LIST_ATTRIBUTE, SchemaConstants.PAYLOAD_ATTRIBUTE)
            ).toArray(Attribute[]::new));
        
        Assert.assertEquals(expectedOutputSchema, computedOutputSchema);
    }
    
    /**
     * Test the generateOutputSchema method.
     * This test uses a Schema with all the values of FiledType, the 
     * SPAN_LIST and the PAYLOAD attributes as input. One field with
     * type String is in the list of fields to be matched.
     * The expected output Schema is a schema equal to the input schema.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test
    public void testGenerateOutputSchema06() throws TextDBException {
        List<String> matchingFields = Arrays.asList(
                StatementTestUtils.STRING_ATTRIBUTE.getFieldName()
            );
        String keywords = "keyword(s)";
        String matchingType = KeywordMatchingType.SUBSTRING_SCANBASED.toString();
        KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
        
        Schema inputSchema = new Schema(Stream.concat(
                StatementTestUtils.ALL_FIELD_TYPES_SCHEMA.getAttributes().stream(),
                Stream.of(SchemaConstants.SPAN_LIST_ATTRIBUTE, SchemaConstants.PAYLOAD_ATTRIBUTE)
            ).toArray(Attribute[]::new));
                
        Schema computedOutputSchema = keywordExtractPredicate.generateOutputSchema(inputSchema);
        Schema expectedOutputSchema = inputSchema;
        
        Assert.assertEquals(expectedOutputSchema, computedOutputSchema);
    }
    
    /**
     * Test the generateOutputSchema method.
     * This test uses a Schema with all the values of FiledType. One
     * missing field is in the list of fields to perform the matching.
     * The expected result is a TextDBException being thrown, since field
     * 'fieldInvalid' does not exist in the input schema.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test(expected = TextDBException.class)
    public void testGenerateOutputSchema07() throws TextDBException {
        List<String> matchingFields = Arrays.asList(
                StatementTestUtils.STRING_ATTRIBUTE.getFieldName(),
                "fieldInvalid",
                StatementTestUtils.TEXT_ATTRIBUTE.getFieldName()
            );
        String keywords = "keyword(s)";
        String matchingType = KeywordMatchingType.SUBSTRING_SCANBASED.toString();
        KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
        
        Schema inputSchema = StatementTestUtils.ALL_FIELD_TYPES_SCHEMA;
                
        keywordExtractPredicate.generateOutputSchema(inputSchema);
    }
    
    /**
     * Test the generateOutputSchema method.
     * This test uses a Schema with all the values of FiledType. One
     * field with incompatible is in the list of fields to perform
     * the matching.
     * The expected result is a TextDBException being thrown, since 
     * fieldType Integer is not compatible with the matching type of
     * this predicate.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test(expected = TextDBException.class)
    public void testGenerateOutputSchema08() throws TextDBException {
        List<String> matchingFields = Arrays.asList(
                StatementTestUtils.STRING_ATTRIBUTE.getFieldName(),
                StatementTestUtils.INTEGER_ATTRIBUTE.getFieldName(),
                StatementTestUtils.TEXT_ATTRIBUTE.getFieldName()
            );
        String keywords = "keyword(s)";
        String matchingType = KeywordMatchingType.SUBSTRING_SCANBASED.toString();
        KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
        Schema inputSchema = StatementTestUtils.ALL_FIELD_TYPES_SCHEMA;
                
        keywordExtractPredicate.generateOutputSchema(inputSchema);
    }
    
    /**
     * Test the generateOutputSchema method.
     * This test check if valid values for matchingType with different
     * cases can successfully generate an outputSchema.
     * This test uses a Schema with all the values of FiledType and a
     * list with no fields to be matched.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test
    public void testGenerateOutputSchema09() throws TextDBException {
        Schema inputSchema = StatementTestUtils.ALL_FIELD_TYPES_SCHEMA;
        // Create a list with valid matchingType with the original case, lower case and upper case
        List<String> allValidMatchingTypes = KeywordMatcherBuilder.keywordMatchingTypeMap.keySet().stream()
                .flatMap( matchingType -> Stream.of(matchingType, matchingType.toLowerCase(), matchingType.toUpperCase()) )
                .collect( Collectors.toList() );
        
        for (String matchingType : allValidMatchingTypes) {
            // Build the predicate
            List<String> matchingFields = Collections.emptyList();
            String keywords = "keyword(s)";
            KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
            // Get the output schema from the predicate
            Schema computedOutputSchema = keywordExtractPredicate.generateOutputSchema(inputSchema);
            Schema expectedOutputSchema = new Schema(Stream.concat(
                    StatementTestUtils.ALL_FIELD_TYPES_SCHEMA.getAttributes().stream(),
                    Stream.of(SchemaConstants.PAYLOAD_ATTRIBUTE, SchemaConstants.SPAN_LIST_ATTRIBUTE)
                ).toArray(Attribute[]::new));
            
            Assert.assertEquals(expectedOutputSchema, computedOutputSchema);
        }
    }
    
    /**
     * Test the generateOutputSchema method.
     * This test check if a invalid matchingType (different cases)
     * will cause an TextDBException to be thrown.
     * This test uses a Schema with all the values of FiledType. A
     * list with no fields to be matched is used.
     */
    @Test
    public void testGenerateOutputSchema10() {
        Schema inputSchema = StatementTestUtils.ALL_FIELD_TYPES_SCHEMA;
        // Create a list with invalid values for the matchingType attribute (perform some operations to change the string)
        Set<String> allValidMatchingTypes = KeywordMatcherBuilder.keywordMatchingTypeMap.keySet();
        List<String> invalidMatchingTypes = allValidMatchingTypes.stream()
                .map( (validMatchingType) -> validMatchingType.substring(1, validMatchingType.length()/2))
                .map( (invalidMatchingType) -> invalidMatchingType.substring(invalidMatchingType.length()/2)+invalidMatchingType )
                .flatMap( matchingType -> Stream.of(matchingType, matchingType.toLowerCase(), matchingType.toUpperCase()) )
                .filter( invalidMatchingType -> !allValidMatchingTypes.contains(invalidMatchingType))
                .collect(Collectors.toList());
        Assert.assertTrue(!invalidMatchingTypes.isEmpty());
        
        for (String matchingType : invalidMatchingTypes) {
            // Build the predicate
            List<String> matchingFields = Collections.emptyList();
            String keywords = "keyword(s)";
            KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
            // Get the output schema from the predicate
            try {
                keywordExtractPredicate.generateOutputSchema(inputSchema);
                Assert.fail("Expected generateOutputSchema method to throw a TextDBException");
            }catch(TextDBException e){
                
            }
        }
    }

    /**
     * Integration test: assert the schemas obtained by invoking the
     * generateOutputSchema method in the predicate and getOutpuSchema in 
     * the generated operator are the same.
     * A schema with attributes of all types is used as the input schema.
     */
    @Test
    public void integrationOutputSchemaTest00() throws TextDBException {
        Schema inputSchema = StatementTestUtils.ALL_FIELD_TYPES_SCHEMA;
        // Construct the predicate
        List<String> matchingFields = Arrays.asList(
                StatementTestUtils.STRING_ATTRIBUTE.getFieldName(),
                StatementTestUtils.TEXT_ATTRIBUTE.getFieldName()
            );
        String keywords = "keyword(s)";
        String matchingType = KeywordMatchingType.SUBSTRING_SCANBASED.toString();
        KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
        // Assert correct bean construction
        OperatorBean operatorBean = keywordExtractPredicate.generateOperatorBean("xxx");
        Assert.assertTrue("Generated bean is not a KeywordMatcherBean", operatorBean instanceof KeywordMatcherBean);
        // Assert correct operator construction
        IOperator operator = PlanGenUtils.buildOperator(operatorBean.getOperatorType(), operatorBean.getOperatorProperties());
        Assert.assertTrue(operator instanceof KeywordMatcher);
        KeywordMatcher keywordMatcherOperator = (KeywordMatcher)operator;
        // Build an operator with the inputSchema and set it as the input of the KeywordMatcher operator
        IOperator keywordMatcherInputOperator = StatementTestUtils.generateSampleIOperator(inputSchema, Collections.emptyList());
        keywordMatcherOperator.setInputOperator(keywordMatcherInputOperator);
        // Get schemas from statement and from the operator 
        Schema statementOutputSchema = keywordExtractPredicate.generateOutputSchema(inputSchema);
        keywordMatcherOperator.open();
        while (keywordMatcherOperator.getNextTuple() != null) {
            // Some exceptions may be thrown only when the operator is iterated
        }
        Schema operatorOutputSchema = keywordMatcherOperator.getOutputSchema();
        keywordMatcherOperator.close();
        // Assert schemas from statement and from the operator are equal
        Assert.assertEquals(statementOutputSchema, operatorOutputSchema);
    }
    
    /**
     * Integration test: assert an exception is thrown when calling the
     * generateOutputSchema method of the predicate if and only if building
     * the operator by using edu.uci.ics.textdb.plangen.operatorbuilder 
     * and running it will also cause an exception to be thrown when
     * the same parameters are used.
     * This test check if an exception is thrown when a non existing field
     * or field with incompatible type is used.
     */
    @Test
    public void integrationExceptionDueToIncompatibleFieldTypeTest00(){
        Schema inputSchema = StatementTestUtils.ALL_FIELD_TYPES_SCHEMA;
        List<String> matchingFieldsToTest = inputSchema.getAttributeNames();
        matchingFieldsToTest.add("invalidField"); // Also test with non existing fields
        // Iterate over all combinations of Attributes and  KeywordMatchingType
        for(String fieldName : matchingFieldsToTest){
            for(KeywordMatchingType matchingType : KeywordMatchingType.values()){
                Schema predicateOutputSchema=null, operatorOutputSchema=null;
                boolean statementOutputSuccess, operatorOutputSuccess;
                // Build the predicate
                List<String> matchingFields = Arrays.asList(fieldName);
                String keywords = "keyword(s)";
                KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType.toString());
                // Get the output schema from the predicate
                try {
                    predicateOutputSchema = keywordExtractPredicate.generateOutputSchema(inputSchema);
                    statementOutputSuccess = true;
                } catch (TextDBException e) {
                    statementOutputSuccess = false;
                }
                // Build the operator, run it and get the output schema
                try {
                    // Assert the bean creation
                    String operatorBeanId = "xxx";
                    OperatorBean operatorBean = keywordExtractPredicate.generateOperatorBean(operatorBeanId);
                    Assert.assertTrue(operatorBean instanceof KeywordMatcherBean);
                    // Assert the operator creation
                    IOperator operator = PlanGenUtils.buildOperator(operatorBean.getOperatorType(), operatorBean.getOperatorProperties());
                    Assert.assertTrue(operator instanceof KeywordMatcher);
                    KeywordMatcher keywordMatcherOperator = (KeywordMatcher)operator;
                    // Create an operator and set it as input of the KeywordMatcher operator
                    IOperator keywordMatcherInputOperator = StatementTestUtils.generatePopulatedSampleOperator();
                    Assert.assertEquals("Error in test code: defined input schema is different than the sample schema", 
                            keywordMatcherInputOperator.getOutputSchema(), inputSchema);
                    keywordMatcherOperator.setInputOperator(keywordMatcherInputOperator);
                    // Iterate and get schema from the operator 
                    keywordMatcherOperator.open();
                    while (keywordMatcherOperator.getNextTuple() != null) {
                        // Some exceptions may be thrown only when the operator is iterated
                    }
                    operatorOutputSchema = keywordMatcherOperator.getOutputSchema();
                    keywordMatcherOperator.close();
                    operatorOutputSuccess = true;
                }catch(TextDBException e){
                    operatorOutputSuccess = false;
                }
                // Assert the result from statement and generated operator are the equal
                Assert.assertEquals("Expected predicate and operator to both fail or succeed", statementOutputSuccess, operatorOutputSuccess);
                if(statementOutputSuccess==true && operatorOutputSuccess==true){
                    Assert.assertEquals("Output schema from predicate and operator does not match", predicateOutputSchema, operatorOutputSchema);
                }
            }
        }
    }
    
}
