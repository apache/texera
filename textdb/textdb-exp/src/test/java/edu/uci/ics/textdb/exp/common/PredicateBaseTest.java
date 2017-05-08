package edu.uci.ics.textdb.exp.common;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.textdb.exp.dictionarymatcher.Dictionary;
import edu.uci.ics.textdb.exp.dictionarymatcher.DictionaryPredicate;
import edu.uci.ics.textdb.exp.dictionarymatcher.DictionarySourcePredicate;
import edu.uci.ics.textdb.exp.fuzzytokenmatcher.FuzzyTokenPredicate;
import edu.uci.ics.textdb.exp.fuzzytokenmatcher.FuzzyTokenSourcePredicate;
import edu.uci.ics.textdb.exp.join.JoinDistancePredicate;
import edu.uci.ics.textdb.exp.join.SimilarityJoinPredicate;
import edu.uci.ics.textdb.exp.keywordmatcher.KeywordMatchingType;
import edu.uci.ics.textdb.exp.keywordmatcher.KeywordPredicate;
import edu.uci.ics.textdb.exp.keywordmatcher.KeywordSourcePredicate;
import edu.uci.ics.textdb.exp.nlp.entity.NlpEntityPredicate;
import edu.uci.ics.textdb.exp.nlp.entity.NlpEntityType;
import edu.uci.ics.textdb.exp.nlp.sentiment.NlpSentimentPredicate;
import edu.uci.ics.textdb.exp.projection.ProjectionPredicate;
import edu.uci.ics.textdb.exp.regexmatcher.RegexPredicate;
import edu.uci.ics.textdb.exp.regexmatcher.RegexSourcePredicate;
import edu.uci.ics.textdb.exp.regexsplit.RegexSplitPredicate;
import edu.uci.ics.textdb.exp.regexsplit.RegexSplitPredicate.SplitType;
import edu.uci.ics.textdb.exp.sampler.SamplerPredicate;
import edu.uci.ics.textdb.exp.sampler.SamplerPredicate.SampleType;
import edu.uci.ics.textdb.exp.sink.excel.ExcelSinkPredicate;
import edu.uci.ics.textdb.exp.sink.tuple.TupleSinkPredicate;
import edu.uci.ics.textdb.exp.source.file.FileSourcePredicate;
import edu.uci.ics.textdb.exp.source.scan.ScanSourcePredicate;
import junit.framework.Assert;

public class PredicateBaseTest {
    
    /*
     * A helper test function to assert if a predicate is 
     *   serialized / deserialized correctly
     *   by converting predicate to json, then back to predicate, then back to json again,
     *   assert two json strings are the same, (test if anything changed between conversions)
     *   and the json contains "operatorType" and "id". (test if the predicate is registered in PredicateBase)
     *   
     */
    public static void testPredicate(PredicateBase predicate) throws Exception {  
        ObjectMapper objectMapper = new ObjectMapper();
        String predicateJson = objectMapper.writeValueAsString(predicate);
        PredicateBase resultPredicate = objectMapper.readValue(predicateJson, PredicateBase.class);
        String resultPredicateJson = objectMapper.writeValueAsString(resultPredicate);
        
        JsonNode predicateJsonNode = objectMapper.readValue(predicateJson, JsonNode.class);
        JsonNode resultPredicateJsonNode = objectMapper.readValue(resultPredicateJson, JsonNode.class);
        
        Assert.assertEquals(predicateJsonNode, resultPredicateJsonNode);
        Assert.assertTrue(predicateJson.contains(PropertyNameConstants.OPERATOR_TYPE));
        Assert.assertTrue(predicateJson.contains(PropertyNameConstants.OPERATOR_ID));
    }
    
    private static List<String> attributeNames = Arrays.asList("attr1", "attr2");
    
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
                SplitType.STANDALONE);
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
                "./file.txt",
                "attr1");
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
    public void testExcelSink() throws Exception {
    	ExcelSinkPredicate excelSinkPredicate = new ExcelSinkPredicate(10, 10);
    	testPredicate(excelSinkPredicate);
    }

}
