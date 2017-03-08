package edu.uci.ics.textdb.dataflow.projection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.textdb.api.exception.TextDBException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.Tuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.field.TextField;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.DataWriter;
import edu.uci.ics.textdb.storage.RelationManager;

public class ProjectionOperatorTest {
    
    public static final String PEOPLE_TABLE = "projection_test_people";
    
    @BeforeClass
    public static void setUp() throws Exception {
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
    }
    
    @AfterClass
    public static void cleanUp() throws Exception {
        RelationManager relationManager = RelationManager.getRelationManager();
        relationManager.deleteTable(PEOPLE_TABLE);
    }   
    
    public List<Tuple> getProjectionResults(IOperator inputOperator, List<String> projectionFields) throws TextDBException {
        ProjectionPredicate projectionPredicate = new ProjectionPredicate(projectionFields);
        ProjectionOperator projection = new ProjectionOperator(projectionPredicate);
        projection.setInputOperator(inputOperator);
        projection.open();
        
        List<Tuple> results = new ArrayList<>();;
        Tuple nextTuple = null;
        while ((nextTuple = projection.getNextTuple()) != null) {
            results.add(nextTuple);
        }
     
        return results;
    }
    

    @Test
    public void testProjection1() throws Exception {
        List<String> projectionFields = Arrays.asList(
                TestConstants.DESCRIPTION);
        Schema projectionSchema = new Schema(TestConstants.DESCRIPTION_ATTR);
                
        IField[] fields1 = { new TextField("Tall Angry") };
        IField[] fields2 = { new TextField("Short Brown") };
        IField[] fields3 = { new TextField("White Angry") };
        IField[] fields4 = { new TextField("Lin Clooney is Short and lin clooney is Angry") };
        IField[] fields5 = { new TextField("Tall Fair") };
        IField[] fields6 = { new TextField("Short angry") };

        Tuple tuple1 = new Tuple(projectionSchema, fields1);
        Tuple tuple2 = new Tuple(projectionSchema, fields2);
        Tuple tuple3 = new Tuple(projectionSchema, fields3);
        Tuple tuple4 = new Tuple(projectionSchema, fields4);
        Tuple tuple5 = new Tuple(projectionSchema, fields5);
        Tuple tuple6 = new Tuple(projectionSchema, fields6);
        
        List<Tuple> expectedResults = Arrays.asList(tuple1, tuple2, tuple3, tuple4, tuple5, tuple6);
        List<Tuple> returnedResults = getProjectionResults(new ScanBasedSourceOperator(PEOPLE_TABLE), projectionFields);
        
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }
    
    @Test
    public void testProjection2() throws Exception {
        List<String> projectionFields = Arrays.asList(
                TestConstants.FIRST_NAME, TestConstants.DESCRIPTION);
        Schema projectionSchema = new Schema(TestConstants.FIRST_NAME_ATTR, TestConstants.DESCRIPTION_ATTR);
                
        IField[] fields1 = { new StringField("bruce"), new TextField("Tall Angry") };
        IField[] fields2 = { new StringField("tom hanks"), new TextField("Short Brown") };
        IField[] fields3 = { new StringField("brad lie angelina"), new TextField("White Angry") };
        IField[] fields4 = { new StringField("george lin lin"), new TextField("Lin Clooney is Short and lin clooney is Angry") };
        IField[] fields5 = { new StringField("christian john wayne"), new TextField("Tall Fair") };
        IField[] fields6 = { new StringField("Mary brown"), new TextField("Short angry") };

        Tuple tuple1 = new Tuple(projectionSchema, fields1);
        Tuple tuple2 = new Tuple(projectionSchema, fields2);
        Tuple tuple3 = new Tuple(projectionSchema, fields3);
        Tuple tuple4 = new Tuple(projectionSchema, fields4);
        Tuple tuple5 = new Tuple(projectionSchema, fields5);
        Tuple tuple6 = new Tuple(projectionSchema, fields6);
        
        List<Tuple> expectedResults = Arrays.asList(tuple1, tuple2, tuple3, tuple4, tuple5, tuple6);
        List<Tuple> returnedResults = getProjectionResults(new ScanBasedSourceOperator(PEOPLE_TABLE), projectionFields);
        
        Assert.assertTrue(TestUtils.equals(expectedResults, returnedResults));
    }

}
