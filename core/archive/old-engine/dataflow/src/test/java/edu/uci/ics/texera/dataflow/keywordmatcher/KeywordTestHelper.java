package edu.uci.ics.texera.dataflow.keywordmatcher;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.texera.api.constants.test.TestConstants;
import edu.uci.ics.texera.api.constants.test.TestConstantsChinese;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.TestUtils;
import edu.uci.ics.texera.dataflow.source.scan.ScanBasedSourceOperator;
import edu.uci.ics.texera.dataflow.source.scan.ScanSourcePredicate;
import edu.uci.ics.texera.storage.DataWriter;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

/**
 * A helper class for functions that are used in multiple keyword matcher tests.
 * This class contains functions that: 
 *   create and write test tables,
 *   delete test tables
 *   get the results from a keyword matcher
 * @author Zuozhi Wang
 * @author Qinhua Huang
 *
 */
public class KeywordTestHelper {
    
    public static final String PEOPLE_TABLE = "keyword_test_people";
    public static final String MEDLINE_TABLE = "keyword_test_medline";
    public static final String CHINESE_TABLE = "keyword_test_chinese";
    
    public static final String RESULTS = "dictionary test results";
    
    public static void writeTestTables() throws TexeraException {
        RelationManager relationManager = RelationManager.getInstance();
        
        // create the people table and write tuples
        relationManager.createTable(PEOPLE_TABLE, TestUtils.getDefaultTestIndex().resolve(PEOPLE_TABLE), 
                TestConstants.SCHEMA_PEOPLE, LuceneAnalyzerConstants.standardAnalyzerString());

        DataWriter peopleDataWriter = relationManager.getTableDataWriter(PEOPLE_TABLE);
        peopleDataWriter.open();
        for (Tuple tuple : TestConstants.getSamplePeopleTuples()) {
            peopleDataWriter.insertTuple(tuple);
        }
        peopleDataWriter.close();
        
        // create the medline table and write tuples
        relationManager.createTable(MEDLINE_TABLE, TestUtils.getDefaultTestIndex().resolve(MEDLINE_TABLE),
                keywordTestConstants.SCHEMA_MEDLINE, LuceneAnalyzerConstants.standardAnalyzerString());
   
        DataWriter medDataWriter = relationManager.getTableDataWriter(MEDLINE_TABLE);
        medDataWriter.open();
        for (Tuple tuple : keywordTestConstants.getSampleMedlineRecord()) {
            medDataWriter.insertTuple(tuple);
        }
        medDataWriter.close();
        
        // create the people table and write tuples in Chinese
        relationManager.createTable(CHINESE_TABLE, TestUtils.getDefaultTestIndex().resolve(CHINESE_TABLE), 
                TestConstantsChinese.SCHEMA_PEOPLE, LuceneAnalyzerConstants.chineseAnalyzerString());
        DataWriter chineseDataWriter = relationManager.getTableDataWriter(CHINESE_TABLE);
        chineseDataWriter.open();
        for (Tuple tuple : TestConstantsChinese.getSamplePeopleTuples()) {
            chineseDataWriter.insertTuple(tuple);
        }
        chineseDataWriter.close();
    }
    
    public static void deleteTestTables() throws TexeraException {
        RelationManager relationManager = RelationManager.getInstance();

        relationManager.deleteTable(PEOPLE_TABLE);
        relationManager.deleteTable(MEDLINE_TABLE);
        relationManager.deleteTable(CHINESE_TABLE);
    }
    
    public static List<Tuple> getQueryResults(String tableName, String keywordQuery, List<String> attributeNames,
            KeywordMatchingType matchingType) throws TexeraException {
        return getQueryResults(tableName, keywordQuery, attributeNames, matchingType, Integer.MAX_VALUE, 0);
    }
    
    public static List<Tuple> getQueryResults(String tableName, String keywordQuery, List<String> attributeNames,
            KeywordMatchingType matchingType, int limit, int offset) throws TexeraException {
        
        // results from a scan on the table followed by a keyword match
        List<Tuple> scanSourceResults = getScanSourceResults(tableName, keywordQuery, attributeNames,
                matchingType, limit, offset);
        // results from index-based keyword search on the table
        List<Tuple> keywordSourceResults = getKeywordSourceResults(tableName, keywordQuery, attributeNames,
                matchingType, limit, offset);
        
        // if limit and offset are not relevant, the results from scan source and keyword source must be the same
        if (limit == Integer.MAX_VALUE && offset == 0) {
            if (TestUtils.equals(scanSourceResults, keywordSourceResults)) {
                return scanSourceResults;
            } else {
                throw new DataflowException("results from scanSource and keywordSource are inconsistent");
            }
        }
        // if limit and offset are relevant, then the results can be different (since the order doesn't matter)
        // in this case, we get all the results and test if the whole result set contains both results
        else {
            List<Tuple> allResults = getKeywordSourceResults(tableName, keywordQuery, attributeNames,
                    matchingType, Integer.MAX_VALUE, 0);
            
            if (scanSourceResults.size() == keywordSourceResults.size() &&
                    TestUtils.containsAll(allResults, scanSourceResults) && 
                    TestUtils.containsAll(allResults, keywordSourceResults)) {
                return scanSourceResults;
            } else {
                throw new DataflowException("results from scanSource and keywordSource are inconsistent");
            }   
        }
    }
    
    public static List<Tuple> getScanSourceResults(String tableName, String keywordQuery, List<String> attributeNames,
            KeywordMatchingType matchingType, int limit, int offset) throws TexeraException {
        RelationManager relationManager = RelationManager.getInstance();
        
        ScanBasedSourceOperator scanSource = new ScanBasedSourceOperator(new ScanSourcePredicate(tableName));
        
        KeywordPredicate keywordPredicate = new KeywordPredicate(
                keywordQuery, attributeNames, relationManager.getTableAnalyzerString(tableName), matchingType, 
                RESULTS);
        KeywordMatcher keywordMatcher = new KeywordMatcher(keywordPredicate);
        keywordMatcher.setLimit(limit);
        keywordMatcher.setOffset(offset);
        
        keywordMatcher.setInputOperator(scanSource);
        
        Tuple tuple;
        List<Tuple> results = new ArrayList<>();
        
        keywordMatcher.open();
        while ((tuple = keywordMatcher.getNextTuple()) != null) {
            results.add(tuple);
        }  
        keywordMatcher.close();
        
        return results;
    }
    
    public static List<Tuple> getKeywordSourceResults(String tableName, String keywordQuery, List<String> attributeNames,
            KeywordMatchingType matchingType, int limit, int offset) throws TexeraException {
        RelationManager relationManager = RelationManager.getInstance();
        KeywordSourcePredicate keywordSourcePredicate = new KeywordSourcePredicate(
                keywordQuery, attributeNames, relationManager.getTableAnalyzerString(tableName), matchingType, 
                tableName, RESULTS);
        KeywordMatcherSourceOperator keywordSource = new KeywordMatcherSourceOperator(
                keywordSourcePredicate);
        keywordSource.setLimit(limit);
        keywordSource.setOffset(offset);
        
        Tuple tuple;
        List<Tuple> results = new ArrayList<>();
        
        keywordSource.open();
        while ((tuple = keywordSource.getNextTuple()) != null) {
            results.add(tuple);
        }
        keywordSource.close();
        
        return results;
    }

}
