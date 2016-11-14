package edu.uci.ics.textdb.dataflow.connector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.textdb.api.exception.TextDBException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.TextField;
import edu.uci.ics.textdb.dataflow.projection.ProjectionOperator;
import edu.uci.ics.textdb.dataflow.projection.ProjectionPredicate;
import edu.uci.ics.textdb.dataflow.source.IndexBasedSourceOperator;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.DataReaderPredicate;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.writer.DataWriter;
import junit.framework.Assert;

public class OneToNBroadcastConnectorTest {
    
    private IDataStore dataStore;
    private IDataWriter dataWriter;
    private Analyzer luceneAnalyzer;

    @Before
    public void setUp() throws Exception {
        dataStore = new DataStore(DataConstants.INDEX_DIR, TestConstants.SCHEMA_PEOPLE);
        luceneAnalyzer = new StandardAnalyzer();
        dataWriter = new DataWriter(dataStore, luceneAnalyzer);
        dataWriter.clearData();
        for (ITuple tuple : TestConstants.getSamplePeopleTuples()) {
            dataWriter.insertTuple(tuple);
        }
    }
    
    @After
    public void cleanUp() throws Exception {
        dataWriter.clearData();
    }
    
    private IOperator getScanSourceOperator(IDataStore dataStore) throws DataFlowException {
        DataReaderPredicate dataReaderPredicate = new DataReaderPredicate(
                new MatchAllDocsQuery(), dataStore);
        IndexBasedSourceOperator sourceOperator = new IndexBasedSourceOperator(dataReaderPredicate);
        return sourceOperator;
    }
    
    
    /*
     * This test connects Connector with Projection
     */
    @Test
    public void testTwoOutputsWithProjection() throws TextDBException {
        IOperator sourceOperator = getScanSourceOperator(dataStore);
        
        List<String> projectionFields = Arrays.asList(
                TestConstants.DESCRIPTION);
        Schema projectionSchema = new Schema(TestConstants.DESCRIPTION_ATTR);
        
        IField[] fields1 = { new TextField("Tall Angry") };
        IField[] fields2 = { new TextField("Short Brown") };
        IField[] fields3 = { new TextField("White Angry") };
        IField[] fields4 = { new TextField("Lin Clooney is Short and lin clooney is Angry") };
        IField[] fields5 = { new TextField("Tall Fair") };
        IField[] fields6 = { new TextField("Short angry") };

        ITuple tuple1 = new DataTuple(projectionSchema, fields1);
        ITuple tuple2 = new DataTuple(projectionSchema, fields2);
        ITuple tuple3 = new DataTuple(projectionSchema, fields3);
        ITuple tuple4 = new DataTuple(projectionSchema, fields4);
        ITuple tuple5 = new DataTuple(projectionSchema, fields5);
        ITuple tuple6 = new DataTuple(projectionSchema, fields6);
        
        List<ITuple> expectedResults = Arrays.asList(tuple1, tuple2, tuple3, tuple4, tuple5, tuple6);
        
        
        ProjectionPredicate projectionPredicate = new ProjectionPredicate(projectionFields);
        ProjectionOperator projection1 = new ProjectionOperator(projectionPredicate);
        ProjectionOperator projection2 = new ProjectionOperator(projectionPredicate);
              
        OneToNBroadcastConnector connector = new OneToNBroadcastConnector(2);       
        connector.setInputOperator(sourceOperator);
        projection1.setInputOperator(connector.getOutputOperator(0));
        projection2.setInputOperator(connector.getOutputOperator(1));
        
        projection1.open();    
        
        List<ITuple> projection1Results = new ArrayList<>();
        ITuple nextTuple = null;
        while ((nextTuple = projection1.getNextTuple()) != null) {
            projection1Results.add(nextTuple);
        }
        projection1.close();
        
        projection2.open();

        List<ITuple> projection2Results = new ArrayList<>();
        nextTuple = null;
        while ((nextTuple = projection2.getNextTuple()) != null) {
            projection2Results.add(nextTuple);
        }
        projection2.close();
        
        Assert.assertTrue(TestUtils.containsAllResults(expectedResults, projection1Results));
        Assert.assertTrue(TestUtils.containsAllResults(expectedResults, projection2Results));
        Assert.assertTrue(TestUtils.containsAllResults(projection1Results, projection2Results));
   
    }
    
    
    /*
     * This test tests if the connectors' three outputs are the same.
     */
    @Test
    public void testThreeOutputsWithItself() throws Exception {
        IOperator sourceOperator = getScanSourceOperator(dataStore);

              
        OneToNBroadcastConnector connector = new OneToNBroadcastConnector(3);       
        connector.setInputOperator(sourceOperator);
        IOperator output1 = connector.getOutputOperator(0);
        IOperator output2 = connector.getOutputOperator(1);
        IOperator output3 = connector.getOutputOperator(2);

        output1.open();
        output2.open();
        output3.open();
 
        List<ITuple> output1Results = new ArrayList<>();
        ITuple nextTuple = null;
        while ((nextTuple = output1.getNextTuple()) != null) {
            output1Results.add(nextTuple);
        }
        
        List<ITuple> output2Results = new ArrayList<>();
        nextTuple = null;
        while ((nextTuple = output2.getNextTuple()) != null) {
            output2Results.add(nextTuple);
        }
        
        List<ITuple> output3Results = new ArrayList<>();
        nextTuple = null;
        while ((nextTuple = output3.getNextTuple()) != null) {
            output3Results.add(nextTuple);
        }
        
        output1.close();
        output2.close();
        output3.close();
        
        List<ITuple> expectedResults = TestConstants.getSamplePeopleTuples();
        

        Assert.assertTrue(TestUtils.containsAllResults(expectedResults, output1Results));
        Assert.assertTrue(TestUtils.containsAllResults(expectedResults, output2Results));
        Assert.assertTrue(TestUtils.containsAllResults(expectedResults, output3Results));
   
    }


}
