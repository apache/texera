package edu.uci.ics.texera.dataflow.wordcount;

import java.util.HashMap;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.texera.dataflow.wordcount.WordCountOperator;
import edu.uci.ics.texera.dataflow.wordcount.WordCountIndexSource;
import edu.uci.ics.texera.dataflow.wordcount.WordCountIndexSourcePredicate;
import edu.uci.ics.texera.dataflow.wordcount.WordCountOperatorPredicate;
import edu.uci.ics.texera.api.constants.test.TestConstants;
import edu.uci.ics.texera.api.constants.test.TestConstantsChineseWordCount;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.TestUtils;
import edu.uci.ics.texera.dataflow.source.scan.ScanBasedSourceOperator;
import edu.uci.ics.texera.dataflow.source.scan.ScanSourcePredicate;
import edu.uci.ics.texera.dataflow.utils.DataflowUtils;
import edu.uci.ics.texera.storage.DataWriter;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

/**
 * @author Qinhua Huang
 */

public class WordCountTest {
    public static final String COUNT_TABLE = "wordcount_test";
    public static final String COUNT_CHINESE_TABLE = "wordcount_Chinese_test";
    
    public static HashMap<String, Integer> expectedResult = null;
    public static HashMap<String, Integer> expectedResultChinese = null;
    
    @BeforeClass
    public static void setUp() throws TexeraException {
        cleanUp();
        
        RelationManager relationManager = RelationManager.getInstance();
        // Create the people table and write tuples
        relationManager.createTable(COUNT_TABLE, TestUtils.getDefaultTestIndex().resolve(COUNT_TABLE), 
                TestConstants.SCHEMA_PEOPLE, LuceneAnalyzerConstants.standardAnalyzerString());
        DataWriter dataWriter = relationManager.getTableDataWriter(COUNT_TABLE);
        dataWriter.open();
        for (Tuple tuple : TestConstants.getSamplePeopleTuples()) {
            dataWriter.insertTuple(tuple);
        }
        dataWriter.close();
        
        expectedResult = computeExpectedResult(TestConstants.getSamplePeopleTuples(), TestConstants.DESCRIPTION,
                LuceneAnalyzerConstants.getStandardAnalyzer());
        
        
        relationManager.createTable(COUNT_CHINESE_TABLE, TestUtils.getDefaultTestIndex().resolve(COUNT_CHINESE_TABLE), 
                TestConstantsChineseWordCount.SCHEMA_PEOPLE, LuceneAnalyzerConstants.chineseAnalyzerString());
        DataWriter dataWriterChinese = relationManager.getTableDataWriter(COUNT_CHINESE_TABLE);
        dataWriterChinese.open();
        for (Tuple tuple : TestConstantsChineseWordCount.getSamplePeopleTuples()) {
            dataWriterChinese.insertTuple(tuple);
        }
        dataWriterChinese.close();
        
        expectedResultChinese = computeExpectedResult(TestConstantsChineseWordCount.getSamplePeopleTuples(),
                TestConstantsChineseWordCount.DESCRIPTION, LuceneAnalyzerConstants.getLuceneAnalyzer(
                        LuceneAnalyzerConstants.chineseAnalyzerString()));
    }
    
    @AfterClass
    public static void cleanUp() throws TexeraException {
        RelationManager.getInstance().deleteTable(COUNT_TABLE);
        RelationManager.getInstance().deleteTable(COUNT_CHINESE_TABLE);
        expectedResult = null;
        expectedResultChinese = null;
    }
    
    //Compute result by tuple's PayLoad.
    public static HashMap<String, Integer> computePayLoadWordCount(String tableName,
            String attribute) throws TexeraException {
        ScanBasedSourceOperator scanSource = new ScanBasedSourceOperator(new ScanSourcePredicate(tableName));
        WordCountOperator wordCount = null;
        HashMap<String, Integer> result = new HashMap<String, Integer>();
        
        if (tableName.equals(COUNT_TABLE)) {
            wordCount = new WordCountOperator(new WordCountOperatorPredicate(TestConstants.DESCRIPTION,
                    LuceneAnalyzerConstants.standardAnalyzerString()));
        } else if (tableName.equals(COUNT_CHINESE_TABLE)) {
            wordCount = new WordCountOperator(new WordCountOperatorPredicate(TestConstantsChineseWordCount.DESCRIPTION,
                    LuceneAnalyzerConstants.chineseAnalyzerString()) );
        }
        wordCount.setInputOperator(scanSource);
        
        wordCount.open();
        Tuple tuple;
        while ((tuple = wordCount.getNextTuple()) != null) {
            result.put((String) tuple.getField(WordCountOperator.WORD).getValue(), 
                    (Integer) tuple.getField(WordCountOperator.COUNT).getValue());
        }
        wordCount.close();

        return result;
    }
    
    //Compute result by scanning disk index.
    public static HashMap<String, Integer> computeWordCountIndexSourceResult(String tableName, String attribute)
            throws TexeraException {        
        WordCountIndexSource wordCountIndexSource = null;
        HashMap<String, Integer> result = new HashMap<String, Integer>();
        
        if (tableName.equals(COUNT_TABLE)) {
            wordCountIndexSource = new WordCountIndexSource(new WordCountIndexSourcePredicate(tableName, TestConstants.DESCRIPTION));
        } else if (tableName.equals(COUNT_CHINESE_TABLE)) {
            wordCountIndexSource = new WordCountIndexSource(new WordCountIndexSourcePredicate(
                    tableName, TestConstantsChineseWordCount.DESCRIPTION));
        }
        
        wordCountIndexSource.open();
        Tuple tuple;
        while((tuple = wordCountIndexSource.getNextTuple()) != null) {
            result.put((String) tuple.getField(WordCountIndexSource.WORD).getValue(), 
                    (Integer) tuple.getField(WordCountIndexSource.COUNT).getValue());
        }
        wordCountIndexSource.close();
        
        return result;
    }
    
    // Compute result from Constants.
    public static HashMap<String, Integer> computeExpectedResult(List<Tuple> tuplesList, String attribute, Analyzer analyzer) {
        HashMap<String, Integer> resultHashMap = new HashMap<String, Integer>();
        for (Tuple nextTuple : tuplesList) {
            String text = nextTuple.getField(attribute).getValue().toString();
            List<String> terms = DataflowUtils.tokenizeQuery(analyzer, text);
            for (String term : terms) {
                String key = term.toLowerCase();
                resultHashMap.put(key,
                        resultHashMap.get(key)==null ? 1 : resultHashMap.get(key) + 1);
            }
        }
        return resultHashMap;
    }
    
    // Test counting by reading disk index method.
    @Test
    public void test1() throws TexeraException {
        HashMap<String, Integer> results = computePayLoadWordCount(COUNT_TABLE,
                TestConstants.DESCRIPTION);
        Assert.assertTrue(results.equals(expectedResult));
    }
    
    // Test WordCountIndexSource 
    @Test
    public void test2() throws TexeraException {
        HashMap<String, Integer> results = computeWordCountIndexSourceResult(COUNT_TABLE,
                TestConstants.DESCRIPTION);
        Assert.assertTrue(results.equals(expectedResult));
    }
    
    // Test counting using reading disk index method on Chinese words .
    @Test
    public void test3() throws TexeraException {
        HashMap<String, Integer> results = computeWordCountIndexSourceResult(COUNT_CHINESE_TABLE,
                TestConstantsChineseWordCount.DESCRIPTION);
        Assert.assertTrue(results.equals(expectedResultChinese));
    }
    
 // Test words counting using payload reading method on Chinese .
    @Test
    public void test4() throws TexeraException {
        HashMap<String, Integer> results = computePayLoadWordCount(COUNT_CHINESE_TABLE,
                TestConstantsChineseWordCount.DESCRIPTION);
        Assert.assertTrue(results.equals(expectedResultChinese));
    }
    
}
