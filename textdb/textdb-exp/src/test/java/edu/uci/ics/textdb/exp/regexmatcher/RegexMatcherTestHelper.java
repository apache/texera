package edu.uci.ics.textdb.exp.regexmatcher;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.textdb.api.constants.TestConstants;
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
 * @author zuozhi
 * @author shuying 
 * 
 * Helper class to quickly create unit test
 */
public class RegexMatcherTestHelper {
    
    public static final String RESULTS = "regex test results";
    
    public static final String PEOPLE_TABLE = "regex_test_people";
    public static final String CORP_TABLE = "regex_test_corp";
    public static final String STAFF_TABLE = "regex_test_staff";
    public static final String TEXT_TABLE = "regex_test_text";
    
    public static void writeTestTables() throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        
        // create the people table and write tuples
        relationManager.createTable(PEOPLE_TABLE, "../index/test_tables/" + PEOPLE_TABLE, 
                TestConstants.SCHEMA_PEOPLE, LuceneAnalyzerConstants.nGramAnalyzerString(3));

        DataWriter peopleDataWriter = relationManager.getTableDataWriter(PEOPLE_TABLE);
        peopleDataWriter.open();
        for (Tuple tuple : TestConstants.getSamplePeopleTuples()) {
            peopleDataWriter.insertTuple(tuple);
        }
        peopleDataWriter.close();
        
        // create the corporation table and write tuples
        relationManager.createTable(CORP_TABLE, "../index/test_tables/" + CORP_TABLE,
                RegexTestConstantsCorp.SCHEMA_CORP, LuceneAnalyzerConstants.nGramAnalyzerString(3));

        DataWriter corpDataWriter = relationManager.getTableDataWriter(CORP_TABLE);
        corpDataWriter.open();
        for (Tuple tuple : RegexTestConstantsCorp.getSampleCorpTuples()) {
            corpDataWriter.insertTuple(tuple);
        }
        corpDataWriter.close();
        
        // create the staff table
        relationManager.createTable(STAFF_TABLE, "../index/tests/" + STAFF_TABLE,
                RegexTestConstantStaff.SCHEMA_STAFF, LuceneAnalyzerConstants.nGramAnalyzerString(3));

        DataWriter staffDataWriter = relationManager.getTableDataWriter(STAFF_TABLE);
        staffDataWriter.open();
        for (Tuple tuple : RegexTestConstantStaff.getSampleStaffTuples()) {
            staffDataWriter.insertTuple(tuple);
        }
        staffDataWriter.close();
        
        // create the text table
        relationManager.createTable(TEXT_TABLE, "../index/tests/" + TEXT_TABLE,
                RegexTestConstantsText.SCHEMA_TEXT, LuceneAnalyzerConstants.nGramAnalyzerString(3));

        DataWriter textDataWriter = relationManager.getTableDataWriter(TEXT_TABLE);
        textDataWriter.open();
        for (Tuple tuple : RegexTestConstantsText.getSampleTextTuples()) {
            textDataWriter.insertTuple(tuple);
        }
        textDataWriter.close();
    }
    
    public static void deleteTestTables() throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();

        relationManager.deleteTable(PEOPLE_TABLE);
        relationManager.deleteTable(CORP_TABLE);
        relationManager.deleteTable(STAFF_TABLE);
        relationManager.deleteTable(TEXT_TABLE);
    }

    public static List<Tuple> getQueryResults(String tableName, String regex, List<String> attributeNames) throws Exception {
        return getQueryResults(tableName, regex, attributeNames, true);
    }

    public static List<Tuple> getQueryResults(String tableName, String regex,  List<String> attributeNames, boolean useTranslator) throws Exception {
        return getQueryResults(tableName, regex, attributeNames, useTranslator, Integer.MAX_VALUE, 0);
    }

    public static List<Tuple> getQueryResults(String tableName, String regex,  List<String> attributeNames, boolean useTranslator, 
            int limit, int offset) throws Exception {
        
        // if the translator is not used, then use a scan source operator
        if (! useTranslator) {
            // results from a scan on the table followed by a regex match
            List<Tuple> scanSourceResults = getScanSourceResults(tableName, regex, attributeNames, limit, offset);
            return scanSourceResults;
        // if the translator is used, compare the results from scan source and regex source
        } else {
            List<Tuple> scanSourceResults = getScanSourceResults(tableName, regex, attributeNames, limit, offset);
            List<Tuple> regexSourceResults = getRegexSourceResults(tableName, regex, attributeNames, limit, offset);

            // if limit and offset are not relevant, the results from scan source and keyword source must be the same
            if (limit == Integer.MAX_VALUE && offset == 0) {
                if (TestUtils.equals(scanSourceResults, regexSourceResults)) {
                    return scanSourceResults;
                } else {
                    throw new DataFlowException("results from scanSource and keywordSource are inconsistent");
                }
            } else {
                // if limit and offset are relevant, then the results can be different (since the order doesn't matter)
                // in this case, we get all the results and test if the whole result set contains both results
                List<Tuple> allResults = getScanSourceResults(tableName, regex, attributeNames, Integer.MAX_VALUE, 0);
                
                if (scanSourceResults.size() == regexSourceResults.size() &&
                        TestUtils.containsAll(allResults, scanSourceResults) && 
                        TestUtils.containsAll(allResults, regexSourceResults)) {
                    return scanSourceResults;
                } else {
                    throw new DataFlowException("results from scanSource and keywordSource are inconsistent");
                }   
            }          
        }        
    }
    
    public static List<Tuple> getScanSourceResults(String tableName, String regex, List<String> attributeNames,
            int limit, int offset) throws TextDBException {
        ScanBasedSourceOperator scanSource = new ScanBasedSourceOperator(new ScanSourcePredicate(tableName));
        
        RegexPredicate regexPredicate = new RegexPredicate(regex, attributeNames, RESULTS);
        RegexMatcher regexMatcher = new RegexMatcher(regexPredicate);
        
        regexMatcher.setLimit(limit);
        regexMatcher.setOffset(offset);
        
        regexMatcher.setInputOperator(scanSource);
        
        Tuple tuple;
        List<Tuple> results = new ArrayList<>();
        
        regexMatcher.open();
        while ((tuple = regexMatcher.getNextTuple()) != null) {
            results.add(tuple);
        }  
        regexMatcher.close();
        
        return results;
    }
    
    public static List<Tuple> getRegexSourceResults(String tableName, String regex, List<String> attributeNames,
            int limit, int offset) throws TextDBException {
        RegexSourcePredicate regexSourcePredicate = new RegexSourcePredicate(regex, attributeNames, tableName, RESULTS);
        RegexMatcherSourceOperator regexSource = new RegexMatcherSourceOperator(regexSourcePredicate);
        
        regexSource.setLimit(limit);
        regexSource.setOffset(offset);
        
        Tuple tuple;
        List<Tuple> results = new ArrayList<>();
        
        regexSource.open();
        while ((tuple = regexSource.getNextTuple()) != null) {
            results.add(tuple);
        }
        regexSource.close();
        
        return results;
    }


}
