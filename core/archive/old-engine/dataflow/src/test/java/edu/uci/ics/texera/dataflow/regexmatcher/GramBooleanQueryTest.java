package edu.uci.ics.texera.dataflow.regexmatcher;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

public class GramBooleanQueryTest {

    @Test
    public void testEmptySameOp() {
        GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
        GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
        Assert.assertTrue(query1.equals(query2));
    }

    @Test
    public void testEmptyDifferentOp() {
        GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
        GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        Assert.assertFalse(query1.equals(query2));
    }

    @Test
    public void testSameAnd() {
        GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
        GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
        query1 = GramBooleanQuery.combine(query1, (Arrays.asList("abc")));
        query2 = GramBooleanQuery.combine(query2, (Arrays.asList("abc")));

        Assert.assertEquals(query1, query2);
        Assert.assertEquals(query2, query1);
    }

    @Test
    public void testSameOr() {
        GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        query1 = GramBooleanQuery.combine(query1, (Arrays.asList("abcdef")));
        query2 = GramBooleanQuery.combine(query2, (Arrays.asList("abcdef")));

        Assert.assertEquals(query1, query2);
        Assert.assertEquals(query2, query1);
    }

    @Test
    public void testDifferentAnd() {
        GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
        GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
        query1 = GramBooleanQuery.combine(query1, (Arrays.asList("abc")));
        query2 = GramBooleanQuery.combine(query2, (Arrays.asList("pqr")));

        Assert.assertFalse(query1.equals(query2));
        Assert.assertFalse(query2.equals(query1));
    }

    @Test
    public void testSameMultiple() {
        GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        query1 = GramBooleanQuery.combine(query1, (Arrays.asList("abc")));
        query1 = GramBooleanQuery.combine(query1, (Arrays.asList("pqr")));
        query2 = GramBooleanQuery.combine(query2, (Arrays.asList("abc")));
        query2 = GramBooleanQuery.combine(query2, (Arrays.asList("pqr")));

        Assert.assertEquals(query1, query2);
        Assert.assertEquals(query2, query1);
    }

    @Test
    public void testDifferentMultiple() {
        GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        query1 = GramBooleanQuery.combine(query1, (Arrays.asList("qwe")));
        query1 = GramBooleanQuery.combine(query1, (Arrays.asList("asd")));
        query2 = GramBooleanQuery.combine(query2, (Arrays.asList("zxc")));
        query2 = GramBooleanQuery.combine(query2, (Arrays.asList("vbn")));

        Assert.assertFalse(query1.equals(query2));
        Assert.assertFalse(query2.equals(query1));
    }

    @Test
    public void testSameDifferentOrder() {
        GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        query1 = GramBooleanQuery.combine(query1, (Arrays.asList("abc", "pqr")));
        query2 = GramBooleanQuery.combine(query2, (Arrays.asList("pqr", "abc")));

        Assert.assertEquals(query1, query2);
        Assert.assertEquals(query2, query1);
    }

    @Test
    public void testSameDifferentOrder2() {
        GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        query1 = GramBooleanQuery.combine(query1, (Arrays.asList("asdfg", "poiuy")));
        query2 = GramBooleanQuery.combine(query2, (Arrays.asList("poiuy", "asdfg")));

        Assert.assertEquals(query1, query2);
        Assert.assertEquals(query2, query1);
    }

    @Test
    public void testSameDifferentOrder3() {
        GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        query1 = GramBooleanQuery.combine(query1, (Arrays.asList("abcd", "qwer", "zxc")));
        query2 = GramBooleanQuery.combine(query2, (Arrays.asList("zxc", "qwer", "abcd")));

        Assert.assertEquals(query1, query2);
        Assert.assertEquals(query2, query1);
    }

    @Test
    public void testDifferentDifferentOrder() {
        GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        query1 = GramBooleanQuery.combine(query1, (Arrays.asList("abcd", "qwer", "zxc")));
        query2 = GramBooleanQuery.combine(query2, (Arrays.asList("abc", "qwe", "zxcv")));

        Assert.assertFalse(query1.equals(query2));
        Assert.assertFalse(query2.equals(query1));
    }

}
