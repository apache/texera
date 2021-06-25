package edu.uci.ics.texera.dataflow.fuzzytokenmatcher;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.texera.api.constants.test.TestConstants;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.TestUtils;
import edu.uci.ics.texera.dataflow.fuzzytokenmatcher.FuzzyTokenPredicate;
import edu.uci.ics.texera.dataflow.source.scan.ScanBasedSourceOperator;
import edu.uci.ics.texera.dataflow.source.scan.ScanSourcePredicate;
import edu.uci.ics.texera.storage.DataWriter;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

/**
 * A helper class for FuzzyTokenMatcher's test cases.
 * It includes functions that : 
 *   create and write data to the test tables, 
 *   delete the test tables
 *   get the query result from FuzzyTokenMatcher
 *   
 * @author Zuozhi Wang
 *
 */
public class FuzzyTokenMatcherTestHelper {
    
    public static final String PEOPLE_TABLE = "fuzzytoken_test_people";
    public static final String RESULTS = "fuzzy token matcher results";
    
    /*
     * Creates the test table(s) and writes data into it(them).
     */
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
    }
    
    /*
     * Deletes the test table(s)
     */
    public static void deleteTestTables() throws TexeraException {
        RelationManager relationManager = RelationManager.getInstance();
        relationManager.deleteTable(PEOPLE_TABLE);
    }
    
    /*
     * Gets the query results from FuzzyTokenMatcher (without limit and offset).
     */
    public static List<Tuple> getQueryResults(String tableName, String query, double threshold, List<String> attributeNames) throws TexeraException {
        return getQueryResults(tableName, query, threshold, attributeNames, Integer.MAX_VALUE, 0);
    }
    
    /*
     * Gets the query results from FuzzyTokenMatcher (with limit and offset options)
     */
    public static List<Tuple> getQueryResults(String tableName, String query, double threshold, List<String> attributeNames,
            int limit, int offset) throws TexeraException {
        
        // results from a scan on the table followed by a fuzzy token matcher
        List<Tuple> scanSourceResults = getScanSourceResults(tableName, query, threshold, attributeNames, limit, offset);
        // results from index-based look-ups on the table
        List<Tuple> fuzzyTokenSourceResults = getFuzzyTokenSourceResults(tableName, query, threshold, attributeNames, limit, offset);
        
        // if limit and offset are not relevant, the results from scan source and fuzzy token source must be the same
        if (limit == Integer.MAX_VALUE && offset == 0) {
            if (TestUtils.equals(scanSourceResults, fuzzyTokenSourceResults)) {
                return scanSourceResults;
            } else {
                throw new DataflowException("results from scanSource and fuzzyTokenSource are inconsistent");
            }
        }
        // if limit and offset are relevant, then the results can be different (since the order doesn't matter)
        // in this case, we get all the results and test if the whole result set contains both results
        else {
            List<Tuple> allResults = getFuzzyTokenSourceResults(tableName, query, threshold, attributeNames,
                    Integer.MAX_VALUE, 0);
            
            if (scanSourceResults.size() == fuzzyTokenSourceResults.size() &&
                    TestUtils.containsAll(allResults, scanSourceResults) && 
                    TestUtils.containsAll(allResults, fuzzyTokenSourceResults)) {
                return scanSourceResults;
            } else {
                throw new DataflowException("results from scanSource and fuzzyTokenSource are inconsistent");
            }   
        }
    }
    
    /*
     * Gets the query results by scanning the table and passing the data into a FuzzyTokenMatcher.
     */
    public static List<Tuple> getScanSourceResults(String tableName, String query, double threshold, List<String> attributeNames,
            int limit, int offset) throws TexeraException {
                
        ScanBasedSourceOperator scanSource = new ScanBasedSourceOperator(new ScanSourcePredicate(tableName)); 
        FuzzyTokenPredicate fuzzyTokenPredicate = new FuzzyTokenPredicate(
                query, attributeNames, RelationManager.getInstance().getTableAnalyzerString(tableName), threshold, RESULTS);
        FuzzyTokenMatcher fuzzyTokenMatcher = new FuzzyTokenMatcher(fuzzyTokenPredicate);
        
        fuzzyTokenMatcher.setLimit(limit);
        fuzzyTokenMatcher.setOffset(offset);
        
        fuzzyTokenMatcher.setInputOperator(scanSource);
        
        Tuple tuple;
        List<Tuple> results = new ArrayList<>();
        
        fuzzyTokenMatcher.open();
        while ((tuple = fuzzyTokenMatcher.getNextTuple()) != null) {
            results.add(tuple);
        }  
        fuzzyTokenMatcher.close();
        
        return results;
    }
    
    /*
     * Gets the query results by using a FuzzyTokenMatcherSourceOperator (which performs index-based lookups on the table)
     */
    public static List<Tuple> getFuzzyTokenSourceResults(String tableName, String query, double threshold, List<String> attributeNames,
            int limit, int offset) throws TexeraException {
        
        FuzzyTokenSourcePredicate fuzzyTokenSourcePredicate = new FuzzyTokenSourcePredicate(
                query, attributeNames, RelationManager.getInstance().getTableAnalyzerString(tableName), 
                threshold, tableName, RESULTS);
        
        FuzzyTokenMatcherSourceOperator fuzzyTokenSource = new FuzzyTokenMatcherSourceOperator(
                fuzzyTokenSourcePredicate);
        
        fuzzyTokenSource.setLimit(limit);
        fuzzyTokenSource.setOffset(offset);
        
        Tuple tuple;
        List<Tuple> results = new ArrayList<>();
        
        fuzzyTokenSource.open();
        while ((tuple = fuzzyTokenSource.getNextTuple()) != null) {
            results.add(tuple);
        }
        fuzzyTokenSource.close();
        
        return results;
    }

}
