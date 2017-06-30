package edu.uci.ics.textdb.exp.dictionarymatcher;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.textdb.api.constants.TestConstants;
import edu.uci.ics.textdb.api.constants.TestConstantsChinese;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.span.Span;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.TestUtils;
import edu.uci.ics.textdb.exp.dictionarymatcher.Dictionary;
import edu.uci.ics.textdb.exp.dictionarymatcher.DictionaryPredicate;
import edu.uci.ics.textdb.exp.keywordmatcher.KeywordMatchingType;
import edu.uci.ics.textdb.exp.source.scan.ScanBasedSourceOperator;
import edu.uci.ics.textdb.exp.source.scan.ScanSourcePredicate;
import edu.uci.ics.textdb.storage.DataWriter;
import edu.uci.ics.textdb.storage.RelationManager;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;

public class DictionaryMatcherTestHelper {
    
    public static final String PEOPLE_TABLE = "dictionary_test_people";
    public static final String CHINESE_TABLE = "dictionary_test_chinese";
    
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
        relationManager.deleteTable(CHINESE_TABLE);
    }
    
    public static List<Tuple> getQueryResults(String tableName, Dictionary dictionary, List<String> attributeNames,
            KeywordMatchingType matchingType) throws TextDBException {
        return getQueryResults(tableName, dictionary, attributeNames, matchingType, Integer.MAX_VALUE, 0);
    }
    
    public static List<Tuple> getQueryResults(String tableName, Dictionary dictionary, List<String> attributeNames,
            KeywordMatchingType matchingType, int limit, int offset) throws TextDBException {
        
        // results from a scan on the table followed by a keyword match
        List<Tuple> scanSourceResults = getScanSourceResults(tableName, dictionary, attributeNames,
                matchingType, limit, offset);
        // reset the dictionary's cursor after each run because the same dictionary may be used later.
        dictionary.resetCursor();
        // results from index-based keyword search on the table
        List<Tuple> dictionarySourceResults = getDictionarySourceResults(tableName, dictionary, attributeNames,
                matchingType, limit, offset);
        dictionary.resetCursor();
        
        // if limit and offset are not relevant, the results from scan source and keyword source must be the same
        if (limit == Integer.MAX_VALUE && offset == 0) {
            if (TestUtils.equals(scanSourceResults, dictionarySourceResults)) {
                return scanSourceResults;
            } else {
                throw new DataFlowException("results from scanSource and dictionarySource are inconsistent");
            }
        }
        // if limit and offset are relevant, then the results can be different (since the order doesn't matter)
        // in this case, we get all the results and test if the whole result set contains both results
        else {
            List<Tuple> allResults = getDictionarySourceResults(tableName, dictionary, attributeNames,
                    matchingType, Integer.MAX_VALUE, 0);
            dictionary.resetCursor();
            
            if (scanSourceResults.size() == dictionarySourceResults.size() &&
                    TestUtils.containsAll(allResults, scanSourceResults) && 
                    TestUtils.containsAll(allResults, dictionarySourceResults)) {
                return scanSourceResults;
            } else {
                throw new DataFlowException("results from scanSource and dictionarySource are inconsistent");
            }   
        }
    }
    
    /**
     * Get the results from a DictionaryMatcher with a ScanSource Operator 
     *   (which scans the table first and then feeds the data into the dictionary matcher)
     * 
     * @param tableName
     * @param dictionary
     * @param attributeNames
     * @param matchingType
     * @param limit
     * @param offset
     * @return
     * @throws TextDBException
     */
    public static List<Tuple> getScanSourceResults(String tableName, Dictionary dictionary, List<String> attributeNames,
            KeywordMatchingType matchingType, int limit, int offset) throws TextDBException {
        
        RelationManager relationManager = RelationManager.getRelationManager();
        String luceneAnalyzerStr = relationManager.getTableAnalyzerString(tableName);
        
        ScanBasedSourceOperator scanSource = new ScanBasedSourceOperator(new ScanSourcePredicate(tableName));
        
        DictionaryPredicate dictiaonryPredicate = new DictionaryPredicate(
                dictionary, attributeNames, luceneAnalyzerStr, matchingType, RESULTS);
        DictionaryMatcher dictionaryMatcher = new DictionaryMatcher(dictiaonryPredicate);
        
        dictionaryMatcher.setLimit(limit);
        dictionaryMatcher.setOffset(offset);
        
        dictionaryMatcher.setInputOperator(scanSource);
        
        Tuple tuple;
        List<Tuple> results = new ArrayList<>();
        
        dictionaryMatcher.open();
        while ((tuple = dictionaryMatcher.getNextTuple()) != null) {
            results.add(tuple);
        }  
        dictionaryMatcher.close();
//        for(Tuple t: results){
//            // System.out.println(t.getField("abstract").toString());
//            for(Span span2: (List<Span>) t.getField(RESULTS).getValue()){
//                System.out.println(span2.getAttributeName() + " " + span2.getStart() + " " + span2.getEnd() + " " + span2.getValue());
//            }
//            System.out.println("This is for another dictionary matcher");
//            //  for(Span span: (List<Span>) t.getField(per).getValue()){
//            //     System.out.println(span.getAttributeName() + " " + span.getStart() + " " + span.getEnd() + " " + span.getValue());
//            //   }
//        }
        
        return results;
    }
    
    /**
     * Get the results from the DictionarySourceOperator 
     *   (which performs a index-based lookup for each keyword in the dictionary)
     * 
     * @param tableName
     * @param dictionary
     * @param attributeNames
     * @param matchingType
     * @param limit
     * @param offset
     * @return
     * @throws TextDBException
     */
    public static List<Tuple> getDictionarySourceResults(String tableName, Dictionary dictionary, List<String> attributeNames,
            KeywordMatchingType matchingType, int limit, int offset) throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        String luceneAnalyzerStr = relationManager.getTableAnalyzerString(tableName);
        
        DictionarySourcePredicate dictiaonrySourcePredicate = new DictionarySourcePredicate(
                dictionary, attributeNames, luceneAnalyzerStr, matchingType, tableName, RESULTS);
        DictionaryMatcherSourceOperator dictionarySource = new DictionaryMatcherSourceOperator(
                dictiaonrySourcePredicate);

        dictionarySource.setLimit(limit);
        dictionarySource.setOffset(offset);
        
        Tuple tuple;
        List<Tuple> results = new ArrayList<>();
        
        dictionarySource.open();
        while ((tuple = dictionarySource.getNextTuple()) != null) {
            results.add(tuple);
        }
        dictionarySource.close();
        
        return results;
    }

}
