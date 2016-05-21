package edu.uci.ics.textdb.dataflow.nlpextractor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.dataflow.Nlpextrator.NlpExtractor;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.DataReaderPredicate;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.reader.DataReader;
import edu.uci.ics.textdb.storage.writer.DataWriter;

/**
 * @author Feng [sam0227]
 */
public class NlpExtractorTest {
    private NlpExtractor nlpExtractor;

    private IDataWriter dataWriter;
    private IDataReader dataReader;
    private IDataStore dataStore;

    private Query query;
    private Analyzer analyzer;

    private IPredicate dataReaderPredicate;


    @After
    public void cleanUp() throws Exception {
        dataWriter.clearData();
    }


    /**
     *
     * @about Using NlpExtractor to get all returned results from sourceOperator,
     * return as a list of tuples
     *
     * @param sourceOperator
     * @param attributes
     * @return
     * @throws Exception
     */
    public List<ITuple> getQueryResults(ISourceOperator sourceOperator, List<Attribute> attributes) throws Exception {

        //TODO: Change input format
        nlpExtractor = new NlpExtractor(sourceOperator,attributes, NlpExtractor.NE_ALL);
        nlpExtractor.open();
        ITuple nextTuple = null;
        List<ITuple> results = new ArrayList<ITuple>();
        while ((nextTuple = nlpExtractor.getNextTuple()) != null) {
            results.add(nextTuple);
        }
        nlpExtractor.close();
        return results;
    }

    /**
     * Scenario 1: Test getNextTuple with only one span in the return list
     * Text : Microsoft is a organization.
     *
     * @throws Exception
     */
    @Test
    public void getNextTupleTest1() throws Exception {
        List<ITuple> data = InfoExtractorTestConstants.getTest1Tuple();
        ISourceOperator sourceOperator = getSourceOperator(data.get(0).getSchema(), data);

        Attribute attribute1 = InfoExtractorTestConstants.SENTENCE_ONE_ATTR;
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(attribute1);

        List<ITuple> returnedResults = getQueryResults(sourceOperator,attributes);

        List<ITuple> expectedResults = InfoExtractorTestConstants.getTest1ResultTuples();

        boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);
        Assert.assertTrue(contains);
    }

    /**
     * Scenario 2: Test getNextTuple with more than one span in the return list
     * Text: Microsoft, Google and Facebook are organizations
     */
    @Test
    public void getNextTupleTest2() throws Exception {
        List<ITuple> data = InfoExtractorTestConstants.getTest2Tuple();
        ISourceOperator sourceOperator = getSourceOperator(data.get(0).getSchema(), data);

        Attribute attribute1 = InfoExtractorTestConstants.SENTENCE_ONE_ATTR;
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(attribute1);

        List<ITuple> returnedResults = getQueryResults(sourceOperator,attributes);
        List<ITuple> expectedResults = InfoExtractorTestConstants.getTest2ResultTuples();

        boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);
        Assert.assertTrue(contains);

    }

    /**
     * Scenario 3: Test getNextTuple with more than one span in the return list and with different recognized classes.
     * Text: Microsoft, Google and Facebook are organizations and Donald Trump and Barack Obama are persons.
     */
    @Test
    public void getNextTupleTest3() throws Exception {
        List<ITuple> data = InfoExtractorTestConstants.getTest3Tuple();
        ISourceOperator sourceOperator = getSourceOperator(data.get(0).getSchema(), data);

        Attribute attribute1 = InfoExtractorTestConstants.SENTENCE_ONE_ATTR;
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(attribute1);

        List<ITuple> returnedResults = getQueryResults(sourceOperator,attributes);
        List<ITuple> expectedResults = InfoExtractorTestConstants.getTest3ResultTuples();

        boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);

        Assert.assertTrue(contains);
    }


    /**
     * Scenario 4:Test getNextTuple with more than one span in the return list and with different recognized classes
     * and more than one fields in the source tuple.
     * <p>
     * Sentence1: Microsoft, Google and Facebook are organizations.
     * Sentence2: Donald Trump and Barack Obama are persons.
     */
    @Test
    public void getNextTupleTest4() throws Exception {
        List<ITuple> data = InfoExtractorTestConstants.getTest4Tuple();
        ISourceOperator sourceOperator = getSourceOperator(data.get(0).getSchema(), data);

        Attribute attribute1 = InfoExtractorTestConstants.SENTENCE_ONE_ATTR;
        Attribute attribute2 = InfoExtractorTestConstants.SENTENCE_TWO_ATTR;

        List<Attribute> attributes = new ArrayList<>();
        attributes.add(attribute1);
        attributes.add(attribute2);

        List<ITuple> returnedResults = getQueryResults(sourceOperator,attributes);
        List<ITuple> expectedResults = InfoExtractorTestConstants.getTest4ResultTuples();

        boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);


        Assert.assertTrue(contains);
    }


    /**
     * Scenario 5:Test getNextTuple using two fields:
     * <p>
     *  Sentence1: Microsoft, Google and Facebook are organizations.
     *  Sentence2: Donald Trump and Barack Obama are persons.
     * <p>
     * Only search the second field.
     */
    @Test
    public void getNextTupleTest5() throws Exception {
        List<ITuple> data = InfoExtractorTestConstants.getTest4Tuple();
        ISourceOperator sourceOperator = getSourceOperator(data.get(0).getSchema(), data);

        Attribute attribute = InfoExtractorTestConstants.SENTENCE_TWO_ATTR;
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(attribute);

        List<ITuple> returnedResults = getQueryResults(sourceOperator,attributes);

        List<ITuple> expectedResults = InfoExtractorTestConstants.getTest5ResultTuples();

        boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);


        Assert.assertTrue(contains);
    }


    /**
     * @param schema The data schema
     * @param data
     * @return
     * @throws Exception
     * @about construct a source operator using given schema and data
     */

    public ISourceOperator getSourceOperator(Schema schema, List<ITuple> data) throws Exception {
        dataStore = new DataStore(DataConstants.INDEX_DIR, schema);
        analyzer = new StandardAnalyzer();
        dataWriter = new DataWriter(dataStore, analyzer);
        dataWriter.writeData(data);

        QueryParser queryParser = new QueryParser(InfoExtractorTestConstants.ATTRIBUTES_ONE_SENTENCE.get(0).getFieldName(), analyzer);
        query = queryParser.parse(DataConstants.SCAN_QUERY);
        dataReaderPredicate = new DataReaderPredicate(dataStore, query, DataConstants.SCAN_QUERY,
                analyzer, Arrays.asList(InfoExtractorTestConstants.ATTRIBUTES_ONE_SENTENCE.get(0)));
        dataReader = new DataReader(dataReaderPredicate);

        ISourceOperator sourceOperator = new ScanBasedSourceOperator(dataReader);
        return sourceOperator;

    }

}
