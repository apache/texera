package edu.uci.ics.texera.dataflow.join;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.StorageException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.TestUtils;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordSourcePredicate;
import edu.uci.ics.texera.dataflow.regexmatcher.RegexMatcher;
import edu.uci.ics.texera.dataflow.regexmatcher.RegexPredicate;
import edu.uci.ics.texera.dataflow.join.IJoinPredicate;
import edu.uci.ics.texera.dataflow.join.Join;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordMatcherSourceOperator;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordMatchingType;
import edu.uci.ics.texera.dataflow.source.scan.ScanBasedSourceOperator;
import edu.uci.ics.texera.dataflow.source.scan.ScanSourcePredicate;
import edu.uci.ics.texera.storage.DataWriter;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;


public class JoinTestHelper {
    
    public static final String BOOK_TABLE = "join_test_book";
    
    public static final String NEWS_TABLE_OUTER = "join_test_news_outer";
    public static final String NEWS_TABLE_INNER = "join_test_news_inner";
    
    public static void createTestTables() throws TexeraException {
        RelationManager relationManager = RelationManager.getInstance();
        
        // create the book table
        relationManager.createTable(BOOK_TABLE, TestUtils.getDefaultTestIndex().resolve(BOOK_TABLE), 
                JoinTestConstants.BOOK_SCHEMA, LuceneAnalyzerConstants.standardAnalyzerString());     
        // data for the book table are written in each test cases
    
        // create the news table
        relationManager.createTable(NEWS_TABLE_OUTER, TestUtils.getDefaultTestIndex().resolve(NEWS_TABLE_OUTER),
                JoinTestConstants.NEWS_SCHEMA, LuceneAnalyzerConstants.standardAnalyzerString());

        relationManager.createTable(NEWS_TABLE_INNER, TestUtils.getDefaultTestIndex().resolve(NEWS_TABLE_INNER),
                JoinTestConstants.NEWS_SCHEMA, LuceneAnalyzerConstants.standardAnalyzerString());
    
    }

    public static void insertToTable(String tableName, Tuple... tuples) throws StorageException {
        RelationManager relationManager = RelationManager.getInstance();
        
        DataWriter tableDataWriter = relationManager.getTableDataWriter(tableName);
        tableDataWriter.open();
        for (Tuple tuple : Arrays.asList(tuples)) {
            tableDataWriter.insertTuple(tuple);
        }
        tableDataWriter.close();
    }

    public static void insertToTable(String tableName, List<Tuple> tuples) throws StorageException {
        RelationManager relationManager = RelationManager.getInstance();
        
        DataWriter outerDataWriter = relationManager.getTableDataWriter(tableName);
        outerDataWriter.open();
        for (Tuple tuple : tuples) {
            outerDataWriter.insertTuple(tuple);
        }
        outerDataWriter.close();
    }
    
    /**
     * Clears the data of the inner and outer test tables.
     * @throws TexeraException
     */
    public static void clearTestTables() throws TexeraException {
        RelationManager relationManager = RelationManager.getInstance();

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
     * @throws TexeraException
     */
    public static void deleteTestTables() throws TexeraException {
        RelationManager relationManager = RelationManager.getInstance();
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
     * @throws TexeraException
     */
    public static KeywordMatcherSourceOperator getKeywordSource(String tableName, String query, 
            KeywordMatchingType matchingType) throws TexeraException {
        KeywordSourcePredicate keywordSourcePredicate = new KeywordSourcePredicate(query, 
                Arrays.asList(JoinTestConstants.AUTHOR, JoinTestConstants.TITLE, JoinTestConstants.REVIEW),
                RelationManager.getInstance().getTableAnalyzerString(tableName), 
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
        } catch (DataflowException e) {
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
     * @throws TexeraException
     */
    public static List<Tuple> getJoinDistanceResults(IOperator innerOp, IOperator outerOp,
            IJoinPredicate joinPredicate, int limit, int offset) throws TexeraException {
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
                newAttributes.add(new Attribute(originalAttributes.get(i).getName(),
                        AttributeType.getAttributeType(newField.getClass())));
                newFields.add(newField);
            } else {
                newAttributes.add(originalAttributes.get(i));
                newFields.add(originalTuple.getFields().get(i));
            }
        }
        return new Tuple(new Schema(newAttributes.stream().toArray(Attribute[]::new)), 
                newFields.stream().toArray(IField[]::new));
    }

}
