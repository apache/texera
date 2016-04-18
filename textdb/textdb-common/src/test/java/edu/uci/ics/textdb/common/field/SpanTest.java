package edu.uci.ics.textdb.common.field;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class SpanTest {
    
    private Span span;
    
    @Before
    public void setUp(){
        
    }
    
    @Test
    public void testGetters(){
        int start = 10;
        int end = 20;
        span = new Span(start, end);
        Assert.assertEquals(start, span.getStart());
        Assert.assertEquals(end, span.getEnd());
    }
}
