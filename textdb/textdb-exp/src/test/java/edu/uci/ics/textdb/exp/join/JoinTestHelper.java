package edu.uci.ics.textdb.exp.join;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.StorageException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.IField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.exp.keywordmatcher.KeywordSourcePredicate;
import edu.uci.ics.textdb.exp.regexmatcher.RegexMatcher;
import edu.uci.ics.textdb.exp.regexmatcher.RegexPredicate;
import edu.uci.ics.textdb.exp.join.IJoinPredicate;
import edu.uci.ics.textdb.exp.join.Join;
import edu.uci.ics.textdb.exp.keywordmatcher.KeywordMatcherSourceOperator;
import edu.uci.ics.textdb.exp.keywordmatcher.KeywordMatchingType;
import edu.uci.ics.textdb.exp.source.scan.ScanBasedSourceOperator;
import edu.uci.ics.textdb.exp.source.scan.ScanSourcePredicate;
import edu.uci.ics.textdb.exp.utils.DataflowUtils;
import edu.uci.ics.textdb.storage.DataWriter;
import edu.uci.ics.textdb.storage.RelationManager;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;


public class JoinTestHelper {
    
    public static final String BOOK_TABLE = "join_test_book";
    
    public static final String NEWS_TABLE_OUTER = "join_test_news_outer";
    public static final String NEWS_TABLE_INNER = "join_test_news_inner";
    
    public static void createTestTables() throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        
        // create the book table
        relationManager.createTable(BOOK_TABLE, "../index/test_tables/" + BOOK_TABLE, 
                JoinTestConstants.BOOK_SCHEMA, LuceneAnalyzerConstants.standardAnalyzerString());     
        // data for the book table are written in each test cases
    
        // create the news table
        relationManager.createTable(NEWS_TABLE_OUTER, "../index/test_tables/" + NEWS_TABLE_OUTER,
                JoinTestConstants.NEWS_SCHEMA, LuceneAnalyzerConstants.standardAnalyzerString());

        relationManager.createTable(NEWS_TABLE_INNER, "../index/test_tables/" + NEWS_TABLE_INNER,
                JoinTestConstants.NEWS_SCHEMA, LuceneAnalyzerConstants.standardAnalyzerString());
    
    }

    public static void insertToTable(String tableName, Tuple... tuples) throws StorageException {
        RelationManager relationManager = RelationManager.getRelationManager();
        
        DataWriter tableDataWriter = relationManager.getTableDataWriter(tableName);
        tableDataWriter.open();
        for (Tuple tuple : Arrays.asList(tuples)) {
            tableDataWriter.insertTuple(tuple);
        }
        tableDataWriter.close();
    }

    public static void insertToTable(String tableName, List<Tuple> tuples) throws StorageException {
        RelationManager relationManager = RelationManager.getRelationManager();
        
        DataWriter outerDataWriter = relationManager.getTableDataWriter(tableName);
        outerDataWriter.open();
        for (Tuple tuple : tuples) {
            outerDataWriter.insertTuple(tuple);
        }
        outerDataWriter.close();
    }
    
    /**
     * Clears the data of the inner and outer test tables.
     * @throws TextDBException
     */
    public static void clearTestTables() throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();

        DataWriter bookDataWriter = relationManager.getTableDataWriter(BOOK_TABLE);
        bookDataWriter.open();
        bookDataWriter.clearData();
        bookDataWriter.close();
        
        DataWriter innerNewsDataWriter = relationManager.getTableDataWriter(NEWS_TABLE_INNER);
        innerNewsDataWriter.open();
        innerNewsDataWriter.clearData();
        innerNewsDataWriter.close();
        
        DataWriter outerNewsDataWriter = relationManager.getTableDataWriter(NEWS_TABLE_OUTER);
        outerNewsDataWriter.open();
        outerNewsDataWriter.clearData();
        outerNewsDataWriter.close();  
    }
    
    /**
     * Deletes all test tables
     * @throws TextDBException
     */
    public static void deleteTestTables() throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        relationManager.deleteTable(BOOK_TABLE);
        relationManager.deleteTable(NEWS_TABLE_OUTER);
        relationManager.deleteTable(NEWS_TABLE_INNER);
    }
    
    /**
     * Provides a KeywordMatcherSourceOperator for a test table given a keyword.
     * ( KeywordMatcher is used in most of Join test cases )
     * @param tableName
     * @param query
     * @param matchingType
     * @return
     * @throws TextDBException
     */
    public static KeywordMatcherSourceOperator getKeywordSource(String tableName, String query, 
            KeywordMatchingType matchingType) throws TextDBException {
        KeywordSourcePredicate keywordSourcePredicate = new KeywordSourcePredicate(query, 
                Arrays.asList(JoinTestConstants.AUTHOR, JoinTestConstants.TITLE, JoinTestConstants.REVIEW),
                RelationManager.getRelationManager().getTableAnalyzerString(tableName), 
                matchingType, tableName, SchemaConstants.SPAN_LIST);
        KeywordMatcherSourceOperator keywordSource = new KeywordMatcherSourceOperator(keywordSourcePredicate);
        return keywordSource;
    }

    public static RegexMatcher getRegexMatcher(String tableName, String query, String attrName) {
        try {
            ScanBasedSourceOperator scanBasedSourceOperator = new ScanBasedSourceOperator(
                    new ScanSourcePredicate(tableName));
            RegexMatcher regexMatcher = new RegexMatcher(new RegexPredicate(query, Arrays.asList(attrName), SchemaConstants.SPAN_LIST));
            regexMatcher.setInputOperator(scanBasedSourceOperator);
            return regexMatcher;
        } catch (DataFlowException e) {
            e.printStackTrace();
            return null;
        }
    }

    
    /**
     * Wraps the logic of creating a Join Operator, getting all the results,
     *   and returning the result tuples in a list.
     * 
     * @param outerOp
     * @param innerOp
     * @param joinPredicate
     * @param limit
     * @param offset
     * @return
     * @throws TextDBException
     */
    public static List<Tuple> getJoinDistanceResults(IOperator innerOp, IOperator outerOp,
            IJoinPredicate joinPredicate, int limit, int offset) throws TextDBException {
        Join join = new Join(joinPredicate);
        join.setInnerInputOperator(innerOp);
        join.setOuterInputOperator(outerOp);
        join.setLimit(limit);
        join.setOffset(offset);
        
        Tuple tuple;
        List<Tuple> results = new ArrayList<>();
        
        join.open();
        while ((tuple = join.getNextTuple()) != null) {
            results.add(tuple);
        }
        join.close();
        
        return results;
    }
    
    /**
     * Alter a field of a tuple. ( The schema will also be changed accordingly. )
     * @param originalTuple
     * @param fieldIndex
     * @param newField
     * @return
     */
    public static Tuple alterField(Tuple originalTuple, int fieldIndex, IField newField) {
        List<Attribute> originalAttributes = originalTuple.getSchema().getAttributes();
        List<Attribute> newAttributes = new ArrayList<>();
        List<IField> newFields = new ArrayList<>();
        for (int i = 0; i < originalAttributes.size(); i++) {
            if (i == fieldIndex) {
                newAttributes.add(new Attribute(originalAttributes.get(i).getAttributeName(),
                        DataflowUtils.getAttributeType(newField)));
                newFields.add(newField);
            } else {
                newAttributes.add(originalAttributes.get(i));
                newFields.add(originalTuple.getField(i));
            }
        }
        return new Tuple(new Schema(newAttributes.stream().toArray(Attribute[]::new)), 
                newFields.stream().toArray(IField[]::new));
    }

}
