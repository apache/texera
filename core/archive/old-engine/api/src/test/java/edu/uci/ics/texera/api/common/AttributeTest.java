package edu.uci.ics.texera.api.common;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;

public class AttributeTest {

    private Attribute attribute;
    private String attributeName = "sampleAttributeName";
    private AttributeType type = AttributeType.STRING;

    @Before
    public void setUp() {
        attribute = new Attribute(attributeName, type);
    }

    @Test
    public void testGetterMethods() {
        Assert.assertEquals(attributeName, attribute.getName());
        Assert.assertEquals(type, attribute.getType());
    }
}
