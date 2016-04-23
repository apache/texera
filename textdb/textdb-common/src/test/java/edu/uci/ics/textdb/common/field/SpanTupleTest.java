package edu.uci.ics.textdb.common.field;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;

public class SpanTupleTest {
    
    private ITuple spanTuple;
    
    @Before
    public void setUp(){
        
    }
    
    @Test
    public void testGetters() throws ParseException{
        //create data tuple first
        List<Attribute> attributes = new ArrayList<Attribute>(TestConstants.ATTRIBUTES_PEOPLE);
        List<IField> fields = new ArrayList<IField>(
                Arrays.asList(new IField[]{new StringField("bruce"), new StringField("lee"), new IntegerField(46), 
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970"))}));
        
        //populate span related fields
        attributes.add(SchemaConstants.SPAN_FIELD_NAME_ATTRIBUTE);
        attributes.add(SchemaConstants.SPAN_KEY_ATTRIBUTE);
        attributes.add(SchemaConstants.SPAN_BEGIN_ATTRIBUTE);
        attributes.add(SchemaConstants.SPAN_END_ATTRIBUTE);
        
        String spanFieldName = "city";
        String spanKey = "San Fransisco";
        Integer spanBegin = 20;
        Integer spanEnd = 33; //spanBegin + spanKey.length();
        
        fields.add(new StringField(spanFieldName));
        fields.add(new StringField(spanKey));        
        fields.add(new IntegerField(spanBegin));        
        fields.add(new IntegerField(spanEnd));
        
        
        spanTuple = new DataTuple(new Schema(attributes), fields.toArray(new IField[fields.size()]));
        
        IField beginField = spanTuple.getField(SchemaConstants.SPAN_BEGIN);
        Assert.assertEquals(spanBegin, beginField.getValue());
        
        IField endField = spanTuple.getField(SchemaConstants.SPAN_END);
        Assert.assertEquals(spanEnd, endField.getValue());
        
        IField keyField = spanTuple.getField(SchemaConstants.SPAN_KEY);
        Assert.assertEquals(spanKey, keyField.getValue());
        
        IField fieldNameField = spanTuple.getField(SchemaConstants.SPAN_FIELD_NAME);
        Assert.assertEquals(spanFieldName, fieldNameField.getValue());
        
    }
    
}
