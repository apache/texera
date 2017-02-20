package edu.uci.ics.textdb.dataflow.source;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.textdb.api.exception.TextDBException;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.DataWriter;
import edu.uci.ics.textdb.storage.RelationManager;

/**
 * @author akshaybetala
 * @author Zuozhi Wang
 *
 */
public class IndexBasedSourceOperatorTest {

    public static final String PEOPLE_TABLE = "index_source_test_people";

    @BeforeClass
    public static void setUp() throws Exception {
        RelationManager relationManager = RelationManager.getRelationManager();
        
        // create the people table and write tuples
        relationManager.createTable(PEOPLE_TABLE, "../index/test_tables/" + PEOPLE_TABLE, 
                TestConstants.SCHEMA_PEOPLE, LuceneAnalyzerConstants.standardAnalyzerString());

        DataWriter peopleDataWriter = relationManager.getTableDataWriter(PEOPLE_TABLE);
        peopleDataWriter.open();
        for (ITuple tuple : TestConstants.getSamplePeopleTuples()) {
            peopleDataWriter.insertTuple(tuple);
        }
        peopleDataWriter.close();
    }

    @AfterClass
    public static void cleanUp() throws Exception {
        RelationManager relationManager = RelationManager.getRelationManager();
        relationManager.deleteTable(PEOPLE_TABLE);
    }

    public List<ITuple> getQueryResults(String fieldName, String query) throws TextDBException, ParseException {
        return getQueryResults(new TermQuery(new Term(fieldName, query)));
    }
    
    public List<ITuple> getQueryResults(Query query) throws TextDBException, ParseException {
        IndexBasedSourceOperator indexBasedSourceOperator = 
                new IndexBasedSourceOperator(PEOPLE_TABLE, query);
        indexBasedSourceOperator.open();

        List<ITuple> results = new ArrayList<ITuple>();
        ITuple nextTuple = null;
        while ((nextTuple = indexBasedSourceOperator.getNextTuple()) != null) {
            results.add(nextTuple);
        }
        indexBasedSourceOperator.close();
        return results;
    }

    /**
     * Search in a text field with multiple tokens.
     * 
     * @throws DataFlowException
     * @throws ParseException
     */
    @Test
    public void testTextSearchWithMultipleTokens() throws TextDBException, ParseException {
        List<ITuple> results = getQueryResults(TestConstants.DESCRIPTION, "tall");
        int numTuples = results.size();
        Assert.assertEquals(2, numTuples);

        boolean check = TestUtils.checkResults(results, "Tall", 
                LuceneAnalyzerConstants.getLuceneAnalyzer(LuceneAnalyzerConstants.standardAnalyzerString()), 
                TestConstants.DESCRIPTION);
        Assert.assertTrue(check);
    }

    /**
     * Search in a text field with a single token
     * 
     * @throws DataFlowException
     * @throws ParseException
     */
    @Test
    public void testTextSearchWithSingleToken() throws TextDBException, ParseException {
        List<ITuple> results = getQueryResults(TestConstants.DESCRIPTION, "angry");
        int numTuples = results.size();
        boolean check = TestUtils.checkResults(results, "angry", 
                LuceneAnalyzerConstants.getLuceneAnalyzer(LuceneAnalyzerConstants.standardAnalyzerString()), 
                TestConstants.DESCRIPTION);
        Assert.assertTrue(check);
        Assert.assertEquals(4, numTuples);
    }


    /**
     * 
     * Test a query which has multiple field
     * 
     * @throws DataFlowException
     * @throws ParseException
     */
    @Test
    public void testMultipleFields() throws TextDBException, ParseException {
        
        BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
        Query query1 = new TermQuery(new Term(TestConstants.DESCRIPTION, "brown"));
        Query query2 = new TermQuery(new Term(TestConstants.LAST_NAME, "cruise")); 
        booleanQueryBuilder.add(query1, BooleanClause.Occur.MUST);
        booleanQueryBuilder.add(query2, BooleanClause.Occur.MUST);


        List<ITuple> results = getQueryResults(booleanQueryBuilder.build());
        
        int numTuples = results.size();
        Assert.assertEquals(1, numTuples);

        for (ITuple tuple : results) {
            String descriptionValue = (String) tuple.getField(TestConstants.DESCRIPTION).getValue();
            String lastNameValue = (String) tuple.getField(TestConstants.LAST_NAME).getValue();
            Assert.assertTrue(descriptionValue.toLowerCase().contains("tall")
                    || descriptionValue.toLowerCase().contains("brown"));
            Assert.assertTrue(lastNameValue.toLowerCase().contains("cruise"));
        }
    }

}
