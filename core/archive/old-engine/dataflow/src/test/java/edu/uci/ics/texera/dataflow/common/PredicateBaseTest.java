package edu.uci.ics.texera.dataflow.common;

import java.util.ArrayList;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;

import edu.uci.ics.texera.api.constants.DataConstants;
import edu.uci.ics.texera.api.utils.TestUtils;
import edu.uci.ics.texera.dataflow.aggregator.AggregationAttributeAndResult;
import edu.uci.ics.texera.dataflow.aggregator.AggregationType;
import edu.uci.ics.texera.dataflow.aggregator.AggregatorPredicate;
import edu.uci.ics.texera.dataflow.comparablematcher.ComparablePredicate;
import edu.uci.ics.texera.dataflow.comparablematcher.ComparisonType;
import edu.uci.ics.texera.dataflow.dictionarymatcher.Dictionary;
import edu.uci.ics.texera.dataflow.dictionarymatcher.DictionaryPredicate;
import edu.uci.ics.texera.dataflow.dictionarymatcher.DictionarySourcePredicate;
import edu.uci.ics.texera.dataflow.fuzzytokenmatcher.FuzzyTokenPredicate;
import edu.uci.ics.texera.dataflow.fuzzytokenmatcher.FuzzyTokenSourcePredicate;
import edu.uci.ics.texera.dataflow.join.JoinDistancePredicate;
import edu.uci.ics.texera.dataflow.join.SimilarityJoinPredicate;
import edu.uci.ics.texera.dataflow.common.JsonSchemaHelper;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordMatchingType;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordPredicate;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordSourcePredicate;
import edu.uci.ics.texera.dataflow.nlp.entity.NlpEntityPredicate;
import edu.uci.ics.texera.dataflow.nlp.entity.NlpEntityType;
import edu.uci.ics.texera.dataflow.nlp.sentiment.EmojiSentimentPredicate;
import edu.uci.ics.texera.dataflow.nlp.sentiment.NlpSentimentPredicate;
import edu.uci.ics.texera.dataflow.nlp.splitter.NLPOutputType;
import edu.uci.ics.texera.dataflow.nlp.splitter.NlpSplitPredicate;
import edu.uci.ics.texera.dataflow.projection.ProjectionPredicate;
import edu.uci.ics.texera.dataflow.regexmatcher.RegexPredicate;
import edu.uci.ics.texera.dataflow.regexmatcher.RegexSourcePredicate;
import edu.uci.ics.texera.dataflow.regexsplit.RegexOutputType;
import edu.uci.ics.texera.dataflow.regexsplit.RegexSplitPredicate;
import edu.uci.ics.texera.dataflow.regexsplit.RegexSplitPredicate.SplitType;
import edu.uci.ics.texera.dataflow.sampler.SamplerPredicate;
import edu.uci.ics.texera.dataflow.sampler.SamplerPredicate.SampleType;
import edu.uci.ics.texera.dataflow.sink.excel.ExcelSinkPredicate;
import edu.uci.ics.texera.dataflow.sink.mysql.MysqlSinkPredicate;
import edu.uci.ics.texera.dataflow.sink.tuple.TupleSinkPredicate;
import edu.uci.ics.texera.dataflow.source.asterix.AsterixSourcePredicate;
import edu.uci.ics.texera.dataflow.source.file.FileSourcePredicate;
import edu.uci.ics.texera.dataflow.source.scan.ScanSourcePredicate;
import edu.uci.ics.texera.dataflow.wordcount.WordCountIndexSourcePredicate;
import edu.uci.ics.texera.dataflow.wordcount.WordCountOperatorPredicate;
import junit.framework.Assert;

public class  PredicateBaseTest {
    
    /*
     * A helper test function to assert if a predicate is 
     *   serialized / deserialized correctly
     *   by converting predicate to json, then back to predicate, then back to json again,
     *   assert two json strings are the same, (test if anything changed between conversions)
     *   and the json contains "operatorType" and "id". (test if the predicate is registered in PredicateBase)
     *   
     */
    public static void testPredicate(PredicateBase predicate) throws Exception {  
        JsonNode jsonNode = TestUtils.testJsonSerialization(predicate, true);
        Assert.assertTrue(jsonNode.has(PropertyNameConstants.OPERATOR_TYPE));
        Assert.assertTrue(jsonNode.has(PropertyNameConstants.OPERATOR_ID));
        
        testJsonSchema(predicate);
    }
    
    /**
     * Validate the predicate object against the json schema of the predicate
     * 
     * @param predicate
     * @throws Exception
     */
    public static void testJsonSchema(PredicateBase predicate) throws Exception {
        // if the operator is not exported, skip the test
        if (! JsonSchemaHelper.operatorTypeMap.containsKey(predicate.getClass())) {
            return;
        }
        
        // read the json schema of the predicate class
        Path predicateJsonSchemaPath = JsonSchemaHelper.getJsonSchemaPath(predicate.getClass());
        ObjectMapper objectMapper = DataConstants.defaultObjectMapper;
        ObjectNode schemaJsonNode = (ObjectNode) objectMapper.readValue(predicateJsonSchemaPath.toFile(), ObjectNode.class);
        
        // convert the jsonNode to jsonSchema for validation
        JsonSchema predicateSchema = JsonSchemaFactory.byDefault().getJsonSchema(schemaJsonNode);
        // convert the predicate object to jsonNode
        JsonNode predicateJsonNode = objectMapper.convertValue(predicate, JsonNode.class);
        
        // validate the predicate object against the schema
        predicateSchema.validate(predicateJsonNode);
    }
    
    private static List<String> attributeNames = Arrays.asList("attr1", "attr2");

    @Test
    public void testAggregator() throws Exception {
        AggregationAttributeAndResult aggEntity = new AggregationAttributeAndResult("inputAttr", AggregationType.AVERAGE, "averageAttr");
        List<AggregationAttributeAndResult> aggEntitiesList = new ArrayList<>();
        aggEntitiesList.add(aggEntity);
        
        AggregatorPredicate aggPredicate = new AggregatorPredicate(aggEntitiesList);
        testPredicate(aggPredicate);
    }

    @Test
    public void testDictionary() throws Exception {
        DictionaryPredicate dictionaryPredicate = new DictionaryPredicate(
                new Dictionary(Arrays.asList("entry1", "entry2")),
                attributeNames,
                "standard",
                KeywordMatchingType.CONJUNCTION_INDEXBASED,
                "dictResults");
        testPredicate(dictionaryPredicate);
        
        DictionarySourcePredicate dictionarySourcePredicate = new DictionarySourcePredicate(
                new Dictionary(Arrays.asList("entry1", "entry2")),
                attributeNames,
                "standard",
                KeywordMatchingType.CONJUNCTION_INDEXBASED, 
                "tableName",
                "dictSourceResults");
        testPredicate(dictionarySourcePredicate);
    }
    
    @Test
    public void testFuzzyToken() throws Exception {
        FuzzyTokenPredicate fuzzyTokenPredicate = new FuzzyTokenPredicate(
                "token1 token2 token3",
                attributeNames,
                "standard",
                0.8,
                "spanListName");
        testPredicate(fuzzyTokenPredicate);
        
        FuzzyTokenSourcePredicate fuzzyTokenSourcePredicate = new FuzzyTokenSourcePredicate(
                "token1 token2 token3",
                attributeNames,
                "standard",
                0.8,
                "tableName",
                "spanListName");
        testPredicate(fuzzyTokenSourcePredicate);
    }
    
    @Test
    public void testJoinDistance() throws Exception {
        JoinDistancePredicate joinDistancePredicate = new JoinDistancePredicate("attr1", "attr1", 100);
        testPredicate(joinDistancePredicate);
    }
    
    @Test
    public void testSimilarityJoin() throws Exception {
        SimilarityJoinPredicate similarityJoinPredicate = new SimilarityJoinPredicate("attr1", "attr1", 0.8);
        testPredicate(similarityJoinPredicate);
    }
    
    @Test
    public void testKeyword() throws Exception {
        KeywordPredicate keywordPredicate = new KeywordPredicate(
                "keyword",
                attributeNames,
                "standard",
                KeywordMatchingType.CONJUNCTION_INDEXBASED,
                "keywordResults");
        testPredicate(keywordPredicate);
        
        KeywordSourcePredicate keywordSourcePredicate = new KeywordSourcePredicate(
                "keyword",
                attributeNames,
                "standard",
                KeywordMatchingType.CONJUNCTION_INDEXBASED,
                "tableName",
                "keywordSourceResults");
        testPredicate(keywordSourcePredicate);
    }
    
    @Test
    public void testNlpEntity() throws Exception {
        NlpEntityPredicate nlpEntityPredicate = new NlpEntityPredicate(
                NlpEntityType.LOCATION,
                attributeNames,
                "nlpEntityResults");
        testPredicate(nlpEntityPredicate);
    }
    
    @Test
    public void testNlpSentiment() throws Exception {
        NlpSentimentPredicate nlpSentimentPredicate = new NlpSentimentPredicate(
                "inputAttr",
                "resultAttr");
        testPredicate(nlpSentimentPredicate);
    }
    
    @Test
    public void testProjection() throws Exception {
        ProjectionPredicate projectionPredicate = new ProjectionPredicate(attributeNames);
        testPredicate(projectionPredicate);
    }
    
    @Test
    public void testRegexMatcher() throws Exception {
        RegexPredicate regexPredicate = new RegexPredicate(
                "regex",
                attributeNames,
                "spanListName");
        testPredicate(regexPredicate);
        
        RegexSourcePredicate regexSourcePredicate = new RegexSourcePredicate(
                "regex",
                attributeNames,
                "tableName",
                "spanListName");
        testPredicate(regexSourcePredicate);
    }
    
    @Test
    public void testRegexSplit() throws Exception {
        RegexSplitPredicate regexSplitPredicate = new RegexSplitPredicate(
                "regex",
                "attr1",
                RegexOutputType.ONE_TO_MANY,
                SplitType.STANDALONE,
                "resultAttr");
        testPredicate(regexSplitPredicate);
    }
    
    @Test
    public void testSampler() throws Exception {
        SamplerPredicate samplerPredicate = new SamplerPredicate(
                10,
                SampleType.FIRST_K_ARRIVAL);
        testPredicate(samplerPredicate);
    }
    
    @Test
    public void testFileSource() throws Exception {
        FileSourcePredicate fileSourcePredicate = new FileSourcePredicate(
                "fileName", FileSourcePredicate.FileFormat.PLAIN_TEXT, null, null);
        testPredicate(fileSourcePredicate);
    }
    
    @Test
    public void testScanSource() throws Exception {
        ScanSourcePredicate scanSourcePredicate = new ScanSourcePredicate("tableName");
        testPredicate(scanSourcePredicate);
    }
    
    @Test
    public void testTupleSink() throws Exception {
        TupleSinkPredicate tupleSinkPredicate = new TupleSinkPredicate();
        testPredicate(tupleSinkPredicate);
    }
    
    @Test

    public void testWordCountIndexSource() throws Exception {
        WordCountIndexSourcePredicate wordCountIndexSourcePredicate = new WordCountIndexSourcePredicate("tableName", "attr1");
        testPredicate(wordCountIndexSourcePredicate);
    }
    
    @Test
    public void testWordCountOperator() throws Exception {
        WordCountOperatorPredicate wordCountPredicate = new WordCountOperatorPredicate("attr1", "standard");
        testPredicate(wordCountPredicate);
    }

    @Test
    public void testExcelSink() throws Exception {
        	ExcelSinkPredicate excelSinkPredicate = new ExcelSinkPredicate(10, 10);
        	testPredicate(excelSinkPredicate);
    }
    
    @Test
    public void testAsterixSource() throws Exception {
        testPredicate(new AsterixSourcePredicate(
                "resultField", "host", 19002, "twitter", "ds_tweet", "text", "zika", "2000-01-01", "2017-05-18", 10));
    }
    
    @Test
    public void testComparable() throws Exception {
        testPredicate(new ComparablePredicate("attr", ComparisonType.EQUAL_TO, "1"));
    }
    
    @Test
    public void testEmojiSentiment() throws Exception {
        testPredicate(new EmojiSentimentPredicate("inputAttr", "outputAttr"));
    }

    @Test
    public void tesNlpSplit() throws Exception {
        testPredicate(new NlpSplitPredicate(NLPOutputType.ONE_TO_MANY, "inputAttr", "resultAttr")) ;
    }
    
    @Test
    public void testMysqlSink() throws Exception {
        testPredicate(new MysqlSinkPredicate("host", 1234, "db", "table", "user", "pass", null, null)) ;
    }

}
