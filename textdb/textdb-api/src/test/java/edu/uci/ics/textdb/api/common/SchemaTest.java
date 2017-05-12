package edu.uci.ics.textdb.api.common;

import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;

public class SchemaTest {
    private Schema schema;
    private String attributeName1 = "sampleAttribute_1";
    private AttributeType type1 = AttributeType.STRING;
    private String attributeName2 = "sampleAttribute_2";
    private AttributeType type2 = AttributeType.STRING;

    private Attribute[] attributes = { new Attribute(attributeName1, type1), new Attribute(attributeName2, type2) };;

    @Before
    public void setUp() {
        schema = new Schema(attributes);
    }

    @Test
    public void testGetAttributes() {
        List<Attribute> attributes = schema.getAttributes();
        Assert.assertEquals(this.attributes.length, attributes.size());
        for (Attribute attribute : this.attributes) {
            Assert.assertTrue(attributes.contains(attribute));
        }
    }

    @Test
    public void testGetIndex() {

        int expectedIndex1 = 0;
        int expectedIndex2 = 1;
        int retrievedIndex1 = schema.getIndex(attributeName1);
        int retrievedIndex2 = schema.getIndex(attributeName2.toUpperCase());
        Assert.assertEquals(expectedIndex1, retrievedIndex1);
        Assert.assertEquals(expectedIndex2, retrievedIndex2);

    }
    
    @Test
    public void testGetAttribute() {
        
        Attribute expectedAttribute1 = new Attribute("sampleAttribute_1", AttributeType.STRING);
        Attribute expectedAttribute2 = new Attribute("sampleAttribute_2", AttributeType.STRING);

        Attribute retrievedAttribute1 = schema.getAttribute(attributeName1);
        Attribute retrievedAttribute2 = schema.getAttribute(attributeName2.toUpperCase());
        
        Assert.assertEquals(expectedAttribute1, retrievedAttribute1);
        Assert.assertEquals(expectedAttribute2, retrievedAttribute2);

    }
    
    @Test
    public void testGetAttributeNames() {
        List<String> expectedAttrNames = Arrays.asList("sampleAttribute_1", "sampleAttribute_2");
        List<String> actualAttrNames = schema.getAttributeNames();
        
        Assert.assertEquals(expectedAttrNames, actualAttrNames);
    }
    
    @Test(expected = TextDBException.class)
    public void testGetInvalidAttribute() {
        schema.getAttribute("invalid_attribute");
    }

    @Test
    public void testAddingNewAttribute() {
        // modifying the attributes returned by schema shouldn't affect the original schema
        // because schema is immutable
        List<Attribute> attributes = schema.getAttributes();
        attributes.add(new Attribute("sampleField_3", AttributeType.STRING));
        Assert.assertFalse(attributes.equals(schema.getAttributes()));
    }

    @Test
    public void testRemovingAttribute() {
        // modifying the attributes returned by schema shouldn't affect the original schema
        // because schema is immutable
        List<Attribute> attributes = schema.getAttributes();
        attributes.remove(0);
        Assert.assertFalse(attributes.equals(schema.getAttributes()));        
    }

}