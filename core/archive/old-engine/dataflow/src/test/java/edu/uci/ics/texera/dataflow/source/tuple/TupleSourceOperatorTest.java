package edu.uci.ics.texera.dataflow.source.tuple;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.constants.test.TestConstants;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IDField;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.TestUtils;

public class TupleSourceOperatorTest {
    
    @Test
    public void test1() throws TexeraException {
        TupleSourceOperator tupleSource = new TupleSourceOperator(
                TestConstants.getSamplePeopleTuples(), TestConstants.SCHEMA_PEOPLE);
        
        tupleSource.open();
        
        Tuple tuple;
        List<Tuple> results = new ArrayList<>();
        Schema outputSchema = tupleSource.getOutputSchema();
        while ((tuple = tupleSource.getNextTuple()) != null) {
            results.add(tuple);
        }
        
        tupleSource.close();
        
        // assert result is equal to input
        Assert.assertTrue(TestUtils.equals(TestConstants.getSamplePeopleTuples(), results));
        // assert _id is added to schema
        Assert.assertTrue(outputSchema.equals(
                new Schema.Builder().add(SchemaConstants._ID_ATTRIBUTE).add(TestConstants.SCHEMA_PEOPLE).build()));
        // assert all tuples contain _id field
        for (Tuple resultTuple : results) {
            Assert.assertTrue(resultTuple.getField(SchemaConstants._ID).getClass().equals(IDField.class));
        }
        
    }

}
