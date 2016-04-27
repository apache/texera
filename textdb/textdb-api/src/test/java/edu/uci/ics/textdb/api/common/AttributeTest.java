package edu.uci.ics.textdb.api.common;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class AttributeTest {
    
    private Attribute attribute;
    private String fieldName = "sampleFieldName";
    private FieldType type = FieldType.STRING;
    
    @Before
    public void setUp(){
        attribute = new Attribute(fieldName, type);
    }
    
    @Test
    public void testGetterMethods(){
        Assert.assertEquals(fieldName, attribute.getFieldName());
        Assert.assertEquals(type, attribute.getFieldType());
    }
}
