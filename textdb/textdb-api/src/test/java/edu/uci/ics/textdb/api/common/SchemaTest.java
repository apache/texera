package edu.uci.ics.textdb.api.common;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class SchemaTest {
    private Schema schema;
    private List<Attribute> attributes;
    private String fieldName_1 = "sampleField_1";
    private FieldType type_1 = FieldType.STRING;
    private String fieldName_2 = "sampleField_2";
    private FieldType type_2 = FieldType.STRING;
    
    @Before
    public void setUp(){
        attributes = new ArrayList<Attribute>();
        attributes.add(new Attribute(fieldName_1, type_1));
        attributes.add(new Attribute(fieldName_2, type_2));
        schema = new Schema(attributes);
    }
    
    @Test
    public void testGetAttributes(){
        List<Attribute> attributes = schema.getAttributes();
        //"assertSame" tests if the variables are pointing to the same object or not
        //So no need to check for the internal fields.
        Assert.assertSame(this.attributes, attributes);
    }
}
