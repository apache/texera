package edu.uci.ics.textdb.exp.keywordmatcher;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.textdb.api.constants.TestConstants;
import edu.uci.ics.textdb.api.constants.TestConstantsChinese;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.TestUtils;
import edu.uci.ics.textdb.exp.source.scan.ScanBasedSourceOperator;
import edu.uci.ics.textdb.exp.source.scan.ScanSourcePredicate;
import edu.uci.ics.textdb.storage.DataWriter;
import edu.uci.ics.textdb.storage.RelationManager;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;

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
    
    public static void writeTestTables() throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        
        // create the people table and write tuples
        relationManager.createTable(PEOPLE_TABLE, "../index/test_tables/" + PEOPLE_TABLE, 
                TestConstants.SCHEMA_PEOPLE, LuceneAnalyzerConstants.standardAnalyzerString());

        DataWriter peopleDataWriter = relationManager.getTableDataWriter(PEOPLE_TABLE);
        peopleDataWriter.open();
        for (Tuple tuple : TestConstants.getSamplePeopleTuples()) {
            peopleDataWriter.insertTuple(tuple);
        }
        peopleDataWriter.close();
        
        // create the medline table and write tuples
        relationManager.createTable(MEDLINE_TABLE, "../index/test_tables/" + MEDLINE_TABLE,
                keywordTestConstants.SCHEMA_MEDLINE, LuceneAnalyzerConstants.standardAnalyzerString());
   
        DataWriter medDataWriter = relationManager.getTableDataWriter(MEDLINE_TABLE);
        medDataWriter.open();
        for (Tuple tuple : keywordTestConstants.getSampleMedlineRecord()) {
            medDataWriter.insertTuple(tuple);
        }
        medDataWriter.close();
        
        // create the people table and write tuples in Chinese
        relationManager.createTable(CHINESE_TABLE, "../index/test_tables/" + CHINESE_TABLE, 
                TestConstantsChinese.SCHEMA_PEOPLE, LuceneAnalyzerConstants.chineseAnalyzerString());
        DataWriter chineseDataWriter = relationManager.getTableDataWriter(CHINESE_TABLE);
        chineseDataWriter.open();
        for (Tuple tuple : TestConstantsChinese.getSamplePeopleTuples()) {
            chineseDataWriter.insertTuple(tuple);
        }
        chineseDataWriter.close();
    }
    
    public static void deleteTestTables() throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();

        relationManager.deleteTable(PEOPLE_TABLE);
        relationManager.deleteTable(MEDLINE_TABLE);
        relationManager.deleteTable(CHINESE_TABLE);
    }
    
    public static List<Tuple> getQueryResults(String tableName, String keywordQuery, List<String> attributeNames,
            KeywordMatchingType matchingType) throws TextDBException {
        return getQueryResults(tableName, keywordQuery, attributeNames, matchingType, Integer.MAX_VALUE, 0);
    }
    
    public static List<Tuple> getQueryResults(String tableName, String keywordQuery, List<String> attributeNames,
            KeywordMatchingType matchingType, int limit, int offset) throws TextDBException {
        
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
                throw new DataFlowException("results from scanSource and keywordSource are inconsistent");
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
                throw new DataFlowException("results from scanSource and keywordSource are inconsistent");
            }   
        }
    }
    
    public static List<Tuple> getScanSourceResults(String tableName, String keywordQuery, List<String> attributeNames,
            KeywordMatchingType matchingType, int limit, int offset) throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        
        ScanBasedSourceOperator scanSource = new ScanBasedSourceOperator(new ScanSourcePredicate(tableName));
        
        KeywordPredicate keywordPredicate = new KeywordPredicate(
                keywordQuery, attributeNames, relationManager.getTableAnalyzerString(tableName), matchingType, 
                RESULTS, limit, offset);
        KeywordMatcher keywordMatcher = new KeywordMatcher(keywordPredicate);
        
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
            KeywordMatchingType matchingType, int limit, int offset) throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        KeywordSourcePredicate keywordSourcePredicate = new KeywordSourcePredicate(
                keywordQuery, attributeNames, relationManager.getTableAnalyzerString(tableName), matchingType, 
                tableName, RESULTS, limit, offset);
        KeywordMatcherSourceOperator keywordSource = new KeywordMatcherSourceOperator(
                keywordSourcePredicate);
        
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
