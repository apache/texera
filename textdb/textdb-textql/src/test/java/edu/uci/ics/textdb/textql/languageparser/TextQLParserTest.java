package edu.uci.ics.textdb.textql.languageparser;

import junit.framework.Assert;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.textql.languageparser.Statement;
import edu.uci.ics.textdb.textql.languageparser.SelectStatement;
import edu.uci.ics.textdb.textql.languageparser.CreateViewStatement;
import edu.uci.ics.textdb.textql.languageparser.ExtractPredicate;
import edu.uci.ics.textdb.textql.languageparser.KeywordExtractPredicate;

/**
 * Test cases for the TextQLParser class.
 * Test if the parsing rules return the expected result and expected exceptions.
 * 
 * @author Flavio Bayer
 */
public class TextQLParserTest {

    /**
     * Test the numberLiteralToString method of the parser.
     * It should parse a NUMBER_LITERAL from the input and return it as a string.
     * @throws ParseException if an unexpected ParseException is thrown
     */
    @Test
    public void testNumberLiteralToString() throws ParseException {
        Assert.assertEquals((new TextQLParser(string2InputStream(" 0 "))).numberLiteralToString(), "0");
        Assert.assertEquals((new TextQLParser(string2InputStream(" 12 "))).numberLiteralToString(), "12");
        Assert.assertEquals((new TextQLParser(string2InputStream(" 34566 "))).numberLiteralToString(), "34566");
        Assert.assertEquals((new TextQLParser(string2InputStream(" 78.90 "))).numberLiteralToString(), "78.90");
        Assert.assertEquals((new TextQLParser(string2InputStream(" 123. "))).numberLiteralToString(), "123.");
        Assert.assertEquals((new TextQLParser(string2InputStream(" .456 "))).numberLiteralToString(), ".456");
        Assert.assertEquals((new TextQLParser(string2InputStream(" -0 "))).numberLiteralToString(), "-0");
        Assert.assertEquals((new TextQLParser(string2InputStream(" -12 "))).numberLiteralToString(), "-12");
        Assert.assertEquals((new TextQLParser(string2InputStream(" -34566 "))).numberLiteralToString(), "-34566");
        Assert.assertEquals((new TextQLParser(string2InputStream(" -78.90 "))).numberLiteralToString(), "-78.90");
        Assert.assertEquals((new TextQLParser(string2InputStream(" -123. "))).numberLiteralToString(), "-123.");
        Assert.assertEquals((new TextQLParser(string2InputStream(" -.456 "))).numberLiteralToString(), "-.456");
        Assert.assertEquals((new TextQLParser(string2InputStream(" -.789.001 "))).numberLiteralToString(), "-.789");
        assertException(()->(new TextQLParser(string2InputStream(" -e "))).numberLiteralToString(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" -e 21"))).numberLiteralToString(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" +4 "))).numberLiteralToString(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" a "))).numberLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" a 22 "))).numberLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" a45 "))).numberLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" A45 "))).numberLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" FROM45 "))).numberLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" \"4\" "))).numberLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" /4/ "))).numberLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" /4 "))).numberLiteralToString(), TokenMgrError.class);
        
    }

    /**
     * Test the numberLiteralToDouble method of the parser.
     * It should parse a NUMBER_LITERAL from the input and return it as a double.
     * @throws ParseException if an unexpected ParseException is thrown
     */
    @Test
    public void testNumberLiteralToDouble() throws ParseException {
        Assert.assertEquals((new TextQLParser(string2InputStream(" 0 "))).numberLiteralToDouble(), 0.);
        Assert.assertEquals((new TextQLParser(string2InputStream(" 12 "))).numberLiteralToDouble(), 12.);
        Assert.assertEquals((new TextQLParser(string2InputStream(" 34566 "))).numberLiteralToDouble(), 34566.);
        Assert.assertEquals((new TextQLParser(string2InputStream(" 78.90 "))).numberLiteralToDouble(), 78.90);
        Assert.assertEquals((new TextQLParser(string2InputStream(" 123. "))).numberLiteralToDouble(), 123.);
        Assert.assertEquals((new TextQLParser(string2InputStream(" .456 "))).numberLiteralToDouble(), .456);
        Assert.assertEquals((new TextQLParser(string2InputStream(" -0 "))).numberLiteralToDouble(), -0.);
        Assert.assertEquals((new TextQLParser(string2InputStream(" -12 "))).numberLiteralToDouble(), -12.);
        Assert.assertEquals((new TextQLParser(string2InputStream(" -34566 "))).numberLiteralToDouble(), -34566.);
        Assert.assertEquals((new TextQLParser(string2InputStream(" -78.90 "))).numberLiteralToDouble(), -78.90);
        Assert.assertEquals((new TextQLParser(string2InputStream(" -123. "))).numberLiteralToDouble(), -123.);
        Assert.assertEquals((new TextQLParser(string2InputStream(" -.456 "))).numberLiteralToDouble(), -.456);
        assertException(()->(new TextQLParser(string2InputStream(" -e "))).numberLiteralToDouble(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" -e 21"))).numberLiteralToDouble(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" +4 "))).numberLiteralToDouble(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" a "))).numberLiteralToDouble(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" a 22 "))).numberLiteralToDouble(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" a45 "))).numberLiteralToDouble(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" A45 "))).numberLiteralToDouble(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" FROM45 "))).numberLiteralToDouble(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" \"4\" "))).numberLiteralToDouble(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" /4/ "))).numberLiteralToDouble(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" /4 "))).numberLiteralToDouble(), TokenMgrError.class);
    }

    /**
     * Test the numberLiteralToInteger method  of the parser.
     * It should parse a NUMBER_LITERAL from the input and return it as an integer.
     * @throws ParseException if an unexpected ParseException is thrown
     */
    @Test
    public void testNumberLiteralToInteger() throws ParseException {
        Assert.assertEquals((new TextQLParser(string2InputStream(" 0 "))).numberLiteralToInteger(), 0);
        Assert.assertEquals((new TextQLParser(string2InputStream(" 12 "))).numberLiteralToInteger(), 12);
        Assert.assertEquals((new TextQLParser(string2InputStream(" 34566 "))).numberLiteralToInteger(), 34566);
        Assert.assertEquals((new TextQLParser(string2InputStream(" -0 "))).numberLiteralToInteger(), 0);
        Assert.assertEquals((new TextQLParser(string2InputStream(" -12 "))).numberLiteralToInteger(), -12);
        Assert.assertEquals((new TextQLParser(string2InputStream(" -34566 "))).numberLiteralToInteger(), -34566);
        assertException(()->(new TextQLParser(string2InputStream(" 78.90 "))).numberLiteralToInteger(), NumberFormatException.class);
        assertException(()->(new TextQLParser(string2InputStream(" 123. "))).numberLiteralToInteger(), NumberFormatException.class);
        assertException(()->(new TextQLParser(string2InputStream(" .456 "))).numberLiteralToInteger(), NumberFormatException.class);
        assertException(()->(new TextQLParser(string2InputStream(" -78.90 "))).numberLiteralToInteger(), NumberFormatException.class);
        assertException(()->(new TextQLParser(string2InputStream(" -123. "))).numberLiteralToInteger(), NumberFormatException.class);
        assertException(()->(new TextQLParser(string2InputStream(" -.456 "))).numberLiteralToInteger(), NumberFormatException.class);
        assertException(()->(new TextQLParser(string2InputStream(" -e "))).numberLiteralToInteger(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" -e 21"))).numberLiteralToInteger(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" +4 "))).numberLiteralToInteger(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" a "))).numberLiteralToInteger(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" a 22 "))).numberLiteralToInteger(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" a45 "))).numberLiteralToInteger(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" A45 "))).numberLiteralToInteger(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" FROM45 "))).numberLiteralToInteger(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" \"4\" "))).numberLiteralToInteger(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" /4/ "))).numberLiteralToInteger(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" /4 "))).numberLiteralToInteger(), TokenMgrError.class);
    }

    /**
     * Test the regexLiteralToString method of the parser.
     * It should parse a REGEX_LITERAL from the input and return it as a string.
     * @throws ParseException if an unexpected ParseException is thrown
     */    
    @Test
    public void testRegexLiteralToString() throws ParseException {
        Assert.assertEquals((new TextQLParser(string2InputStream(" // "))).regexLiteralToString(), "");
        Assert.assertEquals((new TextQLParser(string2InputStream(" /abc/ "))).regexLiteralToString(), "abc");
        Assert.assertEquals((new TextQLParser(string2InputStream(" /d\\/e/ "))).regexLiteralToString(), "d/e");
        Assert.assertEquals((new TextQLParser(string2InputStream(" /d\\/e\\/f/ "))).regexLiteralToString(), "d/e/f");
        Assert.assertEquals((new TextQLParser(string2InputStream(" /FROM/ "))).regexLiteralToString(), "FROM");
        Assert.assertEquals((new TextQLParser(string2InputStream(" /\"/ "))).regexLiteralToString(), "\"");
        Assert.assertEquals((new TextQLParser(string2InputStream(" /\\/ "))).regexLiteralToString(), "\\");
        assertException(()->(new TextQLParser(string2InputStream(" /21 "))).regexLiteralToString(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" 2/1/ "))).regexLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" \"4/ "))).regexLiteralToString(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" FROM// "))).regexLiteralToString(), ParseException.class);
    }

    /**
     * Test the stringLiteralToString method of the parser.
     * It should parse a STRING_LITERAL from the input and return it as a string.
     * @throws ParseException if an unexpected ParseException is thrown
     */ 
    @Test
    public void testStringLiteralToString() throws ParseException {
        Assert.assertEquals((new TextQLParser(string2InputStream(" \"\" "))).stringLiteralToString(), "");
        Assert.assertEquals((new TextQLParser(string2InputStream(" \"abc\" "))).stringLiteralToString(), "abc");
        Assert.assertEquals((new TextQLParser(string2InputStream(" \"de f\" "))).stringLiteralToString(), "de f");
        Assert.assertEquals((new TextQLParser(string2InputStream(" \"d\\\"e\" "))).stringLiteralToString(), "d\"e");
        Assert.assertEquals((new TextQLParser(string2InputStream(" \"d\\\"e\\\"f\" "))).stringLiteralToString(), "d\"e\"f");
        Assert.assertEquals((new TextQLParser(string2InputStream(" \"\\\"\" "))).stringLiteralToString(), "\"");
        Assert.assertEquals((new TextQLParser(string2InputStream(" \"\\\" "))).stringLiteralToString(), "\\");
        assertException(()->(new TextQLParser(string2InputStream(" \"21 "))).stringLiteralToString(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" 'aa' "))).stringLiteralToString(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" 2\"1\" "))).stringLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" 21 "))).stringLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" SELECTa "))).stringLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" abc "))).stringLiteralToString(), ParseException.class);
    }

    /**
     * Test the identifierLiteralToString method of the parser.
     * It should parse a IDENTIFIER_LITERAL from the input and return it as a string.
     * @throws ParseException if an unexpected ParseException is thrown
     */ 
    @Test
    public void testIdentifierLiteralToString() throws ParseException {
        Assert.assertEquals((new TextQLParser(string2InputStream(" i "))).identifierLiteralToString(), "i");
        Assert.assertEquals((new TextQLParser(string2InputStream(" id "))).identifierLiteralToString(), "id");
        Assert.assertEquals((new TextQLParser(string2InputStream(" id de "))).identifierLiteralToString(), "id");
        Assert.assertEquals((new TextQLParser(string2InputStream(" id0 "))).identifierLiteralToString(), "id0");
        Assert.assertEquals((new TextQLParser(string2InputStream(" i6i8s7s "))).identifierLiteralToString(), "i6i8s7s");
        Assert.assertEquals((new TextQLParser(string2InputStream(" j7i\\8s7s "))).identifierLiteralToString(), "j7i");
        Assert.assertEquals((new TextQLParser(string2InputStream(" k8i\"8s7s "))).identifierLiteralToString(), "k8i");
        Assert.assertEquals((new TextQLParser(string2InputStream(" aFROM "))).identifierLiteralToString(), "aFROM");
        Assert.assertEquals((new TextQLParser(string2InputStream(" A "))).identifierLiteralToString(), "A");
        Assert.assertEquals((new TextQLParser(string2InputStream(" FROMa "))).identifierLiteralToString(), "FROMa");
        Assert.assertEquals((new TextQLParser(string2InputStream(" Ed0 "))).identifierLiteralToString(), "Ed0");
        assertException(()->(new TextQLParser(string2InputStream(" 2df "))).identifierLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" _a "))).identifierLiteralToString(), TokenMgrError.class);
    }

    /**
     * Test the testIdentifierListToListString method of the parser.
     * It should parse a list of IDENTIFIER_LITERAL from the input and return it as a list of strings.
     * @throws ParseException if an unexpected ParseException is thrown
     */ 
    @Test
    public void testIdentifierListToListString() throws ParseException {
        Assert.assertEquals((new TextQLParser(string2InputStream(" i "))).identifierListToListString(), Arrays.asList("i"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" id "))).identifierListToListString(), Arrays.asList("id"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" id de "))).identifierListToListString(), Arrays.asList("id"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" id,de "))).identifierListToListString(), Arrays.asList("id","de"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" id0 "))).identifierListToListString(), Arrays.asList("id0"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" i6i8s7s "))).identifierListToListString(), Arrays.asList("i6i8s7s"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" i6,i8,s7,s "))).identifierListToListString(), Arrays.asList("i6","i8","s7","s"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" j7i/8s7s/ "))).identifierListToListString(), Arrays.asList("j7i"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" k8i\"8s7s\" "))).identifierListToListString(), Arrays.asList("k8i"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" aFROM "))).identifierListToListString(), Arrays.asList("aFROM"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" B7FROM "))).identifierListToListString(), Arrays.asList("B7FROM"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" A "))).identifierListToListString(), Arrays.asList("A"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" FROMa "))).identifierListToListString(), Arrays.asList("FROMa"));
        assertException(()->(new TextQLParser(string2InputStream(" j7i,/8s7s/ "))).identifierListToListString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" k8i,\"8s7s\" "))).identifierListToListString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" k8i,,k9j "))).identifierListToListString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" k8i,/8s7s/ "))).identifierListToListString(),ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" k8i, "))).identifierListToListString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" j7i\\8s7s "))).identifierListToListString(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" k8i\"8s7s "))).identifierListToListString(), TokenMgrError.class);
        assertException(()->(new TextQLParser(string2InputStream(" 2df "))).identifierListToListString(), ParseException.class);
        assertException(()->(new TextQLParser(string2InputStream(" _a "))).identifierListToListString(), TokenMgrError.class);
    }

    /**
     * Test the extractPredicate method of the parser.
     * It should parse an extract predicate and return the expected ExtractPredicate object.
     * @throws ParseException if an unexpected ParseException is thrown
     */ 
    @Test
    public void testExtractPredicate() throws ParseException {
        String keywordMatchPredicate00 = " KEYWORDMATCH(g0, \"key1\") ";
        ExtractPredicate keywordMatchParameters00 = new KeywordExtractPredicate(Arrays.asList("g0"), "key1", null);
        Assert.assertEquals((new TextQLParser(string2InputStream(keywordMatchPredicate00))).extractKeywordMatchPredicate(), keywordMatchParameters00);
        
        String keywordMatchPredicate01 = " KEYWORDMATCH(g1, \"key2\", conjunction) ";
        ExtractPredicate keywordMatchParameters01 = new KeywordExtractPredicate(Arrays.asList("g1"), "key2", "conjunction");
        Assert.assertEquals((new TextQLParser(string2InputStream(keywordMatchPredicate01))).extractKeywordMatchPredicate(), keywordMatchParameters01);
        
        String keywordMatchPredicate04 = " KEYWORDMATCH([g4], \"key0\") ";
        ExtractPredicate keywordMatchParameters04 = new KeywordExtractPredicate(Arrays.asList("g4"), "key0", null);
        Assert.assertEquals((new TextQLParser(string2InputStream(keywordMatchPredicate04))).extractKeywordMatchPredicate(), keywordMatchParameters04);
        
        String keywordMatchPredicate06 = " KEYWORDMATCH([g6,g7,h8,i9], \"key\") ";
        ExtractPredicate keywordMatchParameters06 = new KeywordExtractPredicate(Arrays.asList("g6","g7","h8","i9"), "key", null);
        Assert.assertEquals((new TextQLParser(string2InputStream(keywordMatchPredicate06))).extractKeywordMatchPredicate(), keywordMatchParameters06);
        
        String keywordMatchPredicate07 = " KEYWORDMATCH([g6,g7,h8,i9], \"key\", substring) ";
        ExtractPredicate keywordMatchParameters07 = new KeywordExtractPredicate(Arrays.asList("g6","g7","h8","i9"), "key", "substring");
        Assert.assertEquals((new TextQLParser(string2InputStream(keywordMatchPredicate07))).extractKeywordMatchPredicate(), keywordMatchParameters07);
    }

    /**
     * Test the statement method of the parser.
     * It should parse a statement and return the expected Statement object.
     * @throws ParseException if an unexpected ParseException is thrown
     */ 
    @Test
    public void testStatement() throws ParseException {
        String selectStatement00 = "SELECT * FROM a;";
        Statement selectStatementParameters00 = new SelectStatement("_sid0", true, null, null, "a", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement00))).statement(), selectStatementParameters00);
        
        String selectStatement06 = "SELECT f8, fa, fc, df, ff FROM j;";
        Statement selectStatementParameters06 = new SelectStatement("_sid0", null, Arrays.asList("f8","fa","fc","df","ff"), null, "j", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement06))).statement(), selectStatementParameters06);
        
        String selectStatement13 = "SELECT h, i, j EXTRACT KEYWORDMATCH([h6,h7,k8,k9], \"key5\") FROM q;";
        ExtractPredicate selectStatementExtract13 = new KeywordExtractPredicate(Arrays.asList("h6","h7","k8","k9"), "key5", null);
        Statement selectStatementParameters13 = new SelectStatement("_sid0", null, Arrays.asList("h","i","j"), selectStatementExtract13, "q", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement13))).statement(), selectStatementParameters13);
        
        String selectStatement14 = "EXTRACT KEYWORDMATCH([i6,j7,l8,m9], \"key5\") FROM q;";
        ExtractPredicate selectStatementExtract14 = new KeywordExtractPredicate(Arrays.asList("i6","j7","l8","m9"), "key5", null);
        Statement selectStatementParameters14 = new SelectStatement("_sid0", null, null, selectStatementExtract14, "q", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement14))).statement(), selectStatementParameters14);
        
        String selectStatement21 = "EXTRACT KEYWORDMATCH([h3,i2,j1,k0], \"key\\\"/\") FROM m LIMIT 4 OFFSET 25 ;";
        ExtractPredicate selectStatementExtract21 = new KeywordExtractPredicate(Arrays.asList("h3","i2","j1","k0"), "key\"/", null);
        Statement selectStatementParameters21 = new SelectStatement("_sid0", null, null, selectStatementExtract21, "m", 4, 25);
        Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement21))).statement(), selectStatementParameters21);
        
        String createViewStatement00 = " CREATE VIEW v0 AS SELECT * FROM a; ";
        Statement createViewStatementSelect00 = new SelectStatement("_sid0", true, null, null, "a", null, null);
        Statement createViewStatementParameters00 = new CreateViewStatement("v0", createViewStatementSelect00);
        Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement00))).statement(), createViewStatementParameters00);
        
        String createViewStatement01 = " CREATE VIEW v1 AS SELECT f8, fa, fc, df, ff FROM j LIMIT 1 OFFSET 8; ";
        Statement createViewStatementSelect01 = new SelectStatement("_sid0", null, Arrays.asList("f8","fa","fc","df","ff"), null, "j", 1, 8);
        Statement createViewStatementParameters01 = new CreateViewStatement("v1", createViewStatementSelect01);
        Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement01))).statement(), createViewStatementParameters01);
        
        String createViewStatement02 = " CREATE VIEW v2 AS SELECT e EXTRACT KEYWORDMATCH([g4,g5], \"key0\") FROM o ;";
        ExtractPredicate createViewStatementExtract02 = new KeywordExtractPredicate(Arrays.asList("g4","g5"), "key0", null);
        Statement createViewStatementSelect02 = new SelectStatement("_sid0", null, Arrays.asList("e"), createViewStatementExtract02, "o", null, null);
        Statement createViewStatementParameters02 = new CreateViewStatement("v2", createViewStatementSelect02);
        Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement02))).statement(), createViewStatementParameters02);
        
        String createViewStatement03 = " CREATE VIEW v2 AS EXTRACT KEYWORDMATCH([g4,g5], \"key0\", substring) FROM o ;";
        ExtractPredicate createViewStatementExtract03 = new KeywordExtractPredicate(Arrays.asList("g4","g5"), "key0", "substring");
        Statement createViewStatementSelect03 = new SelectStatement("_sid0", null, null, createViewStatementExtract03, "o", null, null);
        Statement createViewStatementParameters03 = new CreateViewStatement("v2", createViewStatementSelect03);
        Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement03))).statement(), createViewStatementParameters03);

    }

    /**
     * Test the testStatementsMain method of the parser.
     * It should parse a list of statements and return the expected list of Statement object and provide the Statements to the consumer.
     * @throws ParseException if an unexpected ParseException is thrown
     */ 
    @Test
    public void testStatementsMain() throws ParseException {
        //Declaration of multiple statements for testing
        String selectStatement00 = "SELECT * FROM a;";
        String selectStatement13 = "SELECT h, i, j EXTRACT KEYWORDMATCH([h6,h7,k8,k9], \"key5\") FROM q LIMIT 5 OFFSET 6;";
        String selectStatement14 = "EXTRACT KEYWORDMATCH(i6, \"key5\") FROM q;";
        String createViewStatement00 = " CREATE VIEW v0 AS SELECT * FROM a; ";
        String createViewStatement01 = " CREATE VIEW v1 AS SELECT f8, fa, fc, df, ff FROM j LIMIT 1 OFFSET 8; ";
        String createViewStatement02 = " CREATE VIEW v2 AS SELECT e EXTRACT KEYWORDMATCH([g4,g5], \"key0\") FROM o ;";
        String createViewStatement03 = " CREATE VIEW v2 AS EXTRACT KEYWORDMATCH([g4,g5], \"key0\", substring) FROM o ;";
        
        //Example of statement objects used for testing        
        //Statement selectStatementParameters00 = new SelectStatement("_sid0", true, null, null, "a", null, null);

        ExtractPredicate selectStatementExtract13 = new KeywordExtractPredicate(Arrays.asList("h6","h7","k8","k9"), "key5", null);
        //Statement selectStatementParameters13 = new SelectStatement("_sid0", null, Arrays.asList("h","i","j"), selectStatementExtract13, "q", 5, 6);

        ExtractPredicate selectStatementExtract14 = new KeywordExtractPredicate(Arrays.asList("i6"), "key5", null);
        //Statement selectStatementParameters14 = new SelectStatement("_sid0", null, null, selectStatementExtract14, "q", null, null);

        //Statement createViewStatementSelect00 = new SelectStatement("_sid0", true, null, null, "a", null, null);
        //Statement createViewStatementParameters00 = new CreateViewStatement("v0", createViewStatementSelect00);
        
        //Statement createViewStatementSelect01 = new SelectStatement("_sid0", null, Arrays.asList("f8","fa","fc","df","ff"), null, "j", 1, 8);
        //Statement createViewStatementParameters01 = new CreateViewStatement("v1", createViewStatementSelect01);

        ExtractPredicate createViewStatementExtract02 = new KeywordExtractPredicate(Arrays.asList("g4","g5"), "key0", null);
        //Statement createViewStatementSelect02 = new SelectStatement("_sid0", null, Arrays.asList("e"), createViewStatementExtract02, "o", null, null);
        //Statement createViewStatementParameters02 = new CreateViewStatement("v2", createViewStatementSelect02);
        
        ExtractPredicate createViewStatementExtract03 = new KeywordExtractPredicate(Arrays.asList("g4","g5"), "key0", "substring");
        //Statement createViewStatementSelect03 = new SelectStatement("_sid0", null, null, createViewStatementExtract03, "o", null, null);
        //Statement createViewStatementParameters03 = new CreateViewStatement("v2", createViewStatementSelect03);
        
        //Test combinations of statements
        String statements00 = selectStatement00;
        Statement statements00Select = new SelectStatement("_sid0", true, null, null, "a", null, null);
        List<Statement> statements00Result = Arrays.asList(statements00Select);
        Assert.assertEquals((new TextQLParser(string2InputStream(statements00))).mainStatementList(null), statements00Result);

        String statements01 = createViewStatement02;
        Statement statements01Select = new SelectStatement("_sid0", null, Arrays.asList("e"), createViewStatementExtract02, "o", null, null);
        Statement statements01CreateView = new CreateViewStatement("v2", statements01Select);
        List<Statement> statements01Result = Arrays.asList(statements01CreateView);
        Assert.assertEquals((new TextQLParser(string2InputStream(statements01))).mainStatementList(null), statements01Result);
        
        String statements02 = createViewStatement02 + selectStatement00;
        Statement statements02Select00 = new SelectStatement("_sid0", null, Arrays.asList("e"), createViewStatementExtract02, "o", null, null);
        Statement statements02CreateView00 = new CreateViewStatement("v2", statements02Select00);
        Statement statements02Select01 = new SelectStatement("_sid1", true, null, null, "a", null, null);
        List<Statement> statementsResult02 = Arrays.asList(statements02CreateView00, statements02Select01);
        List<Statement> statementsConsumed02 = new ArrayList<>();
        Assert.assertEquals((new TextQLParser(string2InputStream(statements02))).mainStatementList(null), statementsResult02);
        Assert.assertEquals((new TextQLParser(string2InputStream(statements02))).mainStatementList( s -> statementsConsumed02.add(s) ), statementsResult02);
        Assert.assertEquals(statementsConsumed02, statementsResult02);

        String statements03 = selectStatement00 + createViewStatement00 + createViewStatement03;
        Statement statements03Select00 = new SelectStatement("_sid0", true, null, null, "a", null, null);
        Statement statements03Select01 = new SelectStatement("_sid1", true, null, null, "a", null, null);
        Statement statements03CreateView01 = new CreateViewStatement("v0", statements03Select01);
        Statement statements03Select02 = new SelectStatement("_sid2", null, null, createViewStatementExtract03, "o", null, null);
        Statement statements03CreateView02 = new CreateViewStatement("v2", statements03Select02);
        List<Statement> statements03Result = Arrays.asList(statements03Select00, statements03CreateView01, statements03CreateView02);
        List<Statement> statements03Consumed = new ArrayList<>();
        Assert.assertEquals((new TextQLParser(string2InputStream(statements03))).mainStatementList(null), statements03Result);
        Assert.assertEquals((new TextQLParser(string2InputStream(statements03))).mainStatementList( s -> statements03Consumed.add(s) ), statements03Result);
        Assert.assertEquals(statements03Consumed, statements03Result);
        
        String statements04 = createViewStatement02 + selectStatement14 + selectStatement13;
        Statement statements04Select00 = new SelectStatement("_sid0", null, Arrays.asList("e"), createViewStatementExtract02, "o", null, null);
        Statement statements04CreateView00 = new CreateViewStatement("v2", statements04Select00);
        Statement statements04Select01 = new SelectStatement("_sid1", null, null, selectStatementExtract14, "q", null, null);
        Statement statements04Select02 = new SelectStatement("_sid2", null, Arrays.asList("h","i","j"), selectStatementExtract13, "q", 5, 6);
        List<Statement> statements04Result = Arrays.asList(statements04CreateView00, statements04Select01, statements04Select02);
        List<Statement> statements04Consumed = new ArrayList<>();
        Assert.assertEquals((new TextQLParser(string2InputStream(statements04))).mainStatementList(null), statements04Result);
        Assert.assertEquals((new TextQLParser(string2InputStream(statements04))).mainStatementList( s -> statements04Consumed.add(s) ), statements04Result);
        Assert.assertEquals(statements04Consumed, statements04Result);
        
        String statements05 = createViewStatement01 + selectStatement13 + createViewStatement03;
        Statement statements05Select00 = new SelectStatement("_sid0", null, Arrays.asList("f8","fa","fc","df","ff"), null, "j", 1, 8);
        Statement statements05CreateView00 = new CreateViewStatement("v1", statements05Select00);
        Statement statements05Select01 = new SelectStatement("_sid1", null, Arrays.asList("h","i","j"), selectStatementExtract13, "q", 5, 6);
        Statement statements05Select02 = new SelectStatement("_sid2", null, null, createViewStatementExtract03, "o", null, null);
        Statement statements05CreateView02 = new CreateViewStatement("v2", statements05Select02);
        List<Statement> statements05Result = Arrays.asList(statements05CreateView00, statements05Select01, statements05CreateView02);
        List<Statement> statements05Consumed = new ArrayList<>();
        Assert.assertEquals((new TextQLParser(string2InputStream(statements05))).mainStatementList(null), statements05Result);
        Assert.assertEquals((new TextQLParser(string2InputStream(statements05))).mainStatementList( s -> statements05Consumed.add(s) ), statements05Result);
        Assert.assertEquals(statements05Consumed, statements05Result);        
    } 

    /**
     * Test the testSelectStatement method of the parser.
     * It should parse a select statements and return the expected SelectStatement object.
     * @throws ParseException if an unexpected ParseException is thrown
     */ 
    @Test
    public void testSelectStatement() throws ParseException {
        String selectStatement00 = "SELECT * FROM a";
        Statement selectStatementParameters00 = new SelectStatement("_sid0", true, null, null, "a", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement00))).selectStatement(), selectStatementParameters00);
        
        String selectStatement01 = "SELECT * FROM b LIMIT 5";
        Statement selectStatementParameters01 = new SelectStatement("_sid0", true, null, null, "b", 5, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement01))).selectStatement(), selectStatementParameters01);
        
        String selectStatement02 = "SELECT * FROM c LIMIT 1 OFFSET 8";
        Statement selectStatementParameters02 = new SelectStatement("_sid0", true, null, null, "c", 1, 8);
        Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement02))).selectStatement(), selectStatementParameters02);
        
        String selectStatement03 = "SELECT * FROM d OFFSET 6";
        Statement selectStatementParameters03 = new SelectStatement("_sid0", true, null, null, "d", null, 6);
        Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement03))).selectStatement(), selectStatementParameters03);
        
        String selectStatement04 = "SELECT f1 FROM e";
        Statement selectStatementParameters04 = new SelectStatement("_sid0", null, Arrays.asList("f1"), null, "e", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement04))).selectStatement(), selectStatementParameters04);
        
        String selectStatement05 = "SELECT f1, f5 FROM i";
        Statement selectStatementParameters05 = new SelectStatement("_sid0", null, Arrays.asList("f1","f5"), null, "i", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement05))).selectStatement(), selectStatementParameters05);
        
        String selectStatement06 = "SELECT f8, fa, fc, df, ff FROM j";
        Statement selectStatementParameters06 = new SelectStatement("_sid0", null, Arrays.asList("f8","fa","fc","df","ff"), null, "j", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement06))).selectStatement(), selectStatementParameters06);
        
        String selectStatement07 = "SELECT a EXTRACT KEYWORDMATCH(g0, \"key1\") FROM k";
        ExtractPredicate selectStatementExtract07 = new KeywordExtractPredicate(Arrays.asList("g0"), "key1", null);
        Statement selectStatementParameters07 = new SelectStatement("_sid0", null, Arrays.asList("a"), selectStatementExtract07, "k", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement07))).selectStatement(), selectStatementParameters07);
        
        String selectStatement08 = "SELECT b EXTRACT KEYWORDMATCH(g1, \"key2\", conjunction) FROM l";
        ExtractPredicate selectStatementExtract08 = new KeywordExtractPredicate(Arrays.asList("g1"), "key2", "conjunction");
        Statement selectStatementParameters08 = new SelectStatement("_sid0", null, Arrays.asList("b"), selectStatementExtract08, "l", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement08))).selectStatement(), selectStatementParameters08);
        
        String selectStatement10 = "SELECT v EXTRACT KEYWORDMATCH(u, \"keyZ\") FROM t";
        ExtractPredicate selectStatementExtract10 = new KeywordExtractPredicate(Arrays.asList("u"), "keyZ", null);
        Statement selectStatementParameters10 = new SelectStatement("_sid0", null, Arrays.asList("v"), selectStatementExtract10, "t", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement10))).selectStatement(), selectStatementParameters10);
        
        String selectStatement11 = "SELECT e EXTRACT KEYWORDMATCH([g4], \"key0\") FROM o";
        ExtractPredicate selectStatementExtract11 = new KeywordExtractPredicate(Arrays.asList("g4"), "key0", null);
        Statement selectStatementParameters11 = new SelectStatement("_sid0", null, Arrays.asList("e"), selectStatementExtract11, "o", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement11))).selectStatement(), selectStatementParameters11);
        
        String selectStatement12 = "SELECT f EXTRACT KEYWORDMATCH([g6,g7,h8,i9], \"key\") FROM p";
        ExtractPredicate selectStatementExtract12 = new KeywordExtractPredicate(Arrays.asList("g6","g7","h8","i9"), "key", null);
        Statement selectStatementParameters12 = new SelectStatement("_sid0", null, Arrays.asList("f"), selectStatementExtract12, "p", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement12))).selectStatement(), selectStatementParameters12);
        
        String selectStatement13 = "SELECT h, i, j EXTRACT KEYWORDMATCH([h6,h7,k8,k9], \"key5\") FROM q";
        ExtractPredicate selectStatementExtract13 = new KeywordExtractPredicate(Arrays.asList("h6","h7","k8","k9"), "key5", null);
        Statement selectStatementParameters13 = new SelectStatement("_sid0", null, Arrays.asList("h","i","j"), selectStatementExtract13, "q", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement13))).selectStatement(), selectStatementParameters13);
        
        String selectStatement14 = "EXTRACT KEYWORDMATCH([i6,j7,l8,m9], \"key5\") FROM q";
        ExtractPredicate selectStatementExtract14 = new KeywordExtractPredicate(Arrays.asList("i6","j7","l8","m9"), "key5", null);
        Statement selectStatementParameters14 = new SelectStatement("_sid0", null, null, selectStatementExtract14, "q", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement14))).selectStatement(), selectStatementParameters14);
        
        String selectStatement15 = "EXTRACT KEYWORDMATCH(g0, \"key1\") FROM k";
        ExtractPredicate selectStatementExtract15 = new KeywordExtractPredicate(Arrays.asList("g0"), "key1", null);
        Statement selectStatementParameters15 = new SelectStatement("_sid0", null, null, selectStatementExtract15, "k", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement15))).selectStatement(), selectStatementParameters15);
        
        String selectStatement16 = "EXTRACT KEYWORDMATCH(g1, \"key2\", phrase) FROM l";
        ExtractPredicate selectStatementExtract16 = new KeywordExtractPredicate(Arrays.asList("g1"), "key2", "phrase");
        Statement selectStatementParameters16 = new SelectStatement("_sid0", null, null, selectStatementExtract16, "l", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement16))).selectStatement(), selectStatementParameters16);
        
        String selectStatement19 = "EXTRACT KEYWORDMATCH([g4], \"key0\") FROM o";
        ExtractPredicate selectStatementExtract19 = new KeywordExtractPredicate(Arrays.asList("g4"), "key0", null);
        Statement selectStatementParameters19 = new SelectStatement("_sid0", null, null, selectStatementExtract19, "o", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement19))).selectStatement(), selectStatementParameters19);
        
        String selectStatement20 = "EXTRACT KEYWORDMATCH([g6,g7,h8,i9], \"key\") FROM p";
        ExtractPredicate selectStatementExtract20 = new KeywordExtractPredicate(Arrays.asList("g6","g7","h8","i9"), "key", null);
        Statement selectStatementParameters20 = new SelectStatement("_sid0", null, null, selectStatementExtract20, "p", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement20))).selectStatement(), selectStatementParameters20);
        
        String selectStatement21 = "EXTRACT KEYWORDMATCH([h3,i2,j1,k0], \"key\\\"/\") FROM m LIMIT 4 OFFSET 25 ";
        ExtractPredicate selectStatementExtract21 = new KeywordExtractPredicate(Arrays.asList("h3","i2","j1","k0"), "key\"/", null);
        Statement selectStatementParameters21 = new SelectStatement("_sid0", null, null, selectStatementExtract21, "m", 4, 25);
        Assert.assertEquals((new TextQLParser(string2InputStream(selectStatement21))).selectStatement(), selectStatementParameters21);
        
        String selectStatement22 = "SELECT FROM a";
        assertException(()->(new TextQLParser(string2InputStream(selectStatement22))).selectStatement(), ParseException.class);
        
        String selectStatement23 = "SELECT FROM a OFFSET 5 LIMIT 6";
        assertException(()->(new TextQLParser(string2InputStream(selectStatement23))).selectStatement(), ParseException.class);
        
        String selectStatement24 = "SELECT 25 FROM a";
        assertException(()->(new TextQLParser(string2InputStream(selectStatement24))).selectStatement(), ParseException.class);
        
        String selectStatement25 = "SELECT [a,b] FROM a";
        assertException(()->(new TextQLParser(string2InputStream(selectStatement25))).selectStatement(), ParseException.class);
        
        String selectStatement26 = "SELECT *,a FROM a";
        assertException(()->(new TextQLParser(string2InputStream(selectStatement26))).selectStatement(), ParseException.class);
        
        String selectStatement27 = "SELECT * FROM [a,b]";
        assertException(()->(new TextQLParser(string2InputStream(selectStatement27))).selectStatement(), ParseException.class);
        
        String selectStatement28 = "SELECT KEYWORDMATCH(g0, \"key1\") FROM a";
        assertException(()->(new TextQLParser(string2InputStream(selectStatement28))).selectStatement(), ParseException.class);
        
        String selectStatement29 = "SELECT EXTRACT KEYWORDMATCH(g0, \"key1\") FROM a";
        assertException(()->(new TextQLParser(string2InputStream(selectStatement29))).selectStatement(), ParseException.class);
        
        String selectStatement30 = "EXTRACT a FROM a";
        assertException(()->(new TextQLParser(string2InputStream(selectStatement30))).selectStatement(), ParseException.class);
        
        String selectStatement31 = "EXTRACT * FROM a";
        assertException(()->(new TextQLParser(string2InputStream(selectStatement31))).selectStatement(), ParseException.class);
        
        String selectStatement32 = "EXTRACT KEYWORDMATCH(g0, \"key1\") SELECT a FROM k";
        assertException(()->(new TextQLParser(string2InputStream(selectStatement32))).selectStatement(), ParseException.class);
        
        String selectStatement33 = "SELECT a";
        assertException(()->(new TextQLParser(string2InputStream(selectStatement33))).selectStatement(), ParseException.class);
    }

    /**
     * Test the testExtractKeywordMatchPredicate method of the parser.
     * It should parse an extract keyword predicate and return the expected KeywordExtractPredicate object.
     * @throws ParseException if an unexpected ParseException is thrown
     */ 
    @Test
    public void testExtractKeywordMatchPredicate() throws ParseException {
        String keywordMatchPredicate00 = " KEYWORDMATCH(g0, \"key1\") ";
        ExtractPredicate keywordMatchParameters00 = new KeywordExtractPredicate(Arrays.asList("g0"), "key1", null);
        Assert.assertEquals((new TextQLParser(string2InputStream(keywordMatchPredicate00))).extractKeywordMatchPredicate(), keywordMatchParameters00);
        
        String keywordMatchPredicate01 = " KEYWORDMATCH(g1, \"key2\", conjunction) ";
        ExtractPredicate keywordMatchParameters01 = new KeywordExtractPredicate(Arrays.asList("g1"), "key2", "conjunction");
        Assert.assertEquals((new TextQLParser(string2InputStream(keywordMatchPredicate01))).extractKeywordMatchPredicate(), keywordMatchParameters01);
        
        String keywordMatchPredicate02 = " KEYWORDMATCH(g2, \"key3\", phrase) ";
        ExtractPredicate keywordMatchParameters02 = new KeywordExtractPredicate(Arrays.asList("g2"), "key3", "phrase");
        Assert.assertEquals((new TextQLParser(string2InputStream(keywordMatchPredicate02))).extractKeywordMatchPredicate(), keywordMatchParameters02);
        
        String keywordMatchPredicate03 = " KEYWORDMATCH(g3, \"key4\", substring) ";
        ExtractPredicate keywordMatchParameters03 = new KeywordExtractPredicate(Arrays.asList("g3"), "key4", "substring");
        Assert.assertEquals((new TextQLParser(string2InputStream(keywordMatchPredicate03))).extractKeywordMatchPredicate(), keywordMatchParameters03);
        
        String keywordMatchPredicate04 = " KEYWORDMATCH([g4], \"key0\") ";
        ExtractPredicate keywordMatchParameters04 = new KeywordExtractPredicate(Arrays.asList("g4"), "key0", null);
        Assert.assertEquals((new TextQLParser(string2InputStream(keywordMatchPredicate04))).extractKeywordMatchPredicate(), keywordMatchParameters04);
        
        String keywordMatchPredicate05 = " KEYWORDMATCH([g4,g5], \"key0\") ";
        ExtractPredicate keywordMatchParameters05 = new KeywordExtractPredicate(Arrays.asList("g4","g5"), "key0", null);
        Assert.assertEquals((new TextQLParser(string2InputStream(keywordMatchPredicate05))).extractKeywordMatchPredicate(), keywordMatchParameters05);
        
        String keywordMatchPredicate06 = " KEYWORDMATCH([g6,g7,h8,i9], \"key\") ";
        ExtractPredicate keywordMatchParameters06 = new KeywordExtractPredicate(Arrays.asList("g6","g7","h8","i9"), "key", null);
        Assert.assertEquals((new TextQLParser(string2InputStream(keywordMatchPredicate06))).extractKeywordMatchPredicate(), keywordMatchParameters06);

        String keywordMatchPredicate07 = " KEYWORDMATCH([g6,g7,h8,i9], \"key\", substring) ";
        ExtractPredicate keywordMatchParameters07 = new KeywordExtractPredicate(Arrays.asList("g6","g7","h8","i9"), "key", "substring");
        Assert.assertEquals((new TextQLParser(string2InputStream(keywordMatchPredicate07))).extractKeywordMatchPredicate(), keywordMatchParameters07);
                
        String keywordMatchPredicate08 = " KEYWORDMATCH ([i6,j7,l8,m9, \"key5\") ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchPredicate08))).extractKeywordMatchPredicate(), ParseException.class);
        
        String keywordMatchPredicate09 = " KEYWORDMATCH (i6,j7,l8,m9, \"key5\") ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchPredicate09))).extractKeywordMatchPredicate(), ParseException.class);
        
        String keywordMatchPredicate10 = " KEYWORDMATCH (i6,j7,l8,m9], \"key5\") ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchPredicate10))).extractKeywordMatchPredicate(), ParseException.class);
        
        String keywordMatchPredicate11 = " KEYWORDMATCH ([i6,j7,l8,m9, \"key5\", conjunction) ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchPredicate11))).extractKeywordMatchPredicate(), ParseException.class);
        
        String keywordMatchPredicate12 = " KEYWORDMATCH (i6,j7,l8,m9, \"key5\", substring) ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchPredicate12))).extractKeywordMatchPredicate(), ParseException.class);
        
        String keywordMatchPredicate13 = " KEYWORDMATCH ([i6,j7,l8,m9, \"key5\", phrase) ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchPredicate13))).extractKeywordMatchPredicate(), ParseException.class);
        
        String keywordMatchPredicate14 = " KEYWORDMATCH ([], key5) ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchPredicate14))).extractKeywordMatchPredicate(), ParseException.class);
        
        String keywordMatchPredicate15 = " KEYWORDMATCH ([a], key5) ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchPredicate15))).extractKeywordMatchPredicate(), ParseException.class);
        
        String keywordMatchPredicate16 = " KEYWORDMATCH ([a]) ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchPredicate16))).extractKeywordMatchPredicate(), ParseException.class);
        
        String keywordMatchPredicate17 = " KEYWORDMATCH (\"key1\") ";
        assertException(()->(new TextQLParser(string2InputStream(keywordMatchPredicate17))).extractKeywordMatchPredicate(), ParseException.class);
        
    }

    /**
     * Test the testCreateViewStatement method of the parser.
     * It should parse a create view statement and return the expected CreateViewStatement object.
     * @throws ParseException if an unexpected ParseException is thrown
     */ 
    @Test
    public void testCreateViewStatement() throws ParseException {
        String createViewStatement00 = " CREATE VIEW v0 AS SELECT * FROM a ";
        Statement createViewStatementSelect00 = new SelectStatement("_sid0", true, null, null, "a", null, null);
        Statement createViewStatementParameters00 = new CreateViewStatement("v0", createViewStatementSelect00);
        Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement00))).createViewStatement(), createViewStatementParameters00);
        
        String createViewStatement01 = " CREATE VIEW v1 AS SELECT f8, fa, fc, df, ff FROM j LIMIT 1 OFFSET 8 ";
        Statement createViewStatementSelect01 = new SelectStatement("_sid0", null, Arrays.asList("f8","fa","fc","df","ff"), null, "j", 1, 8);
        Statement createViewStatementParameters01 = new CreateViewStatement("v1", createViewStatementSelect01);
        Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement01))).createViewStatement(), createViewStatementParameters01);
        
        String createViewStatement02 = " CREATE VIEW v2 AS SELECT e EXTRACT KEYWORDMATCH([g4,g5], \"key0\") FROM o ";
        ExtractPredicate createViewStatementExtract02 = new KeywordExtractPredicate(Arrays.asList("g4","g5"), "key0", null);
        Statement createViewStatementSelect02 = new SelectStatement("_sid0", null, Arrays.asList("e"), createViewStatementExtract02, "o", null, null);
        Statement createViewStatementParameters02 = new CreateViewStatement("v2", createViewStatementSelect02);
        Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement02))).createViewStatement(), createViewStatementParameters02);
        
        String createViewStatement03 = " CREATE VIEW v2 AS EXTRACT KEYWORDMATCH([g4,g5], \"key0\", substring) FROM o ";
        ExtractPredicate createViewStatementExtract03 = new KeywordExtractPredicate(Arrays.asList("g4","g5"), "key0", "substring");
        Statement createViewStatementSelect03 = new SelectStatement("_sid0", null, null, createViewStatementExtract03, "o", null, null);
        Statement createViewStatementParameters03 = new CreateViewStatement("v2", createViewStatementSelect03);
        Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement03))).createViewStatement(), createViewStatementParameters03);
        
        String createViewStatement04 = " CREATE VIEW v3 AS CREATE VIEW v4 AS SELECT * FROM a ";
        assertException(()->(new TextQLParser(string2InputStream(createViewStatement04))).createViewStatement(), ParseException.class);
        
        String createViewStatement05 = " CREATE VIEW v0 AS ";
        assertException(()->(new TextQLParser(string2InputStream(createViewStatement05))).createViewStatement(), ParseException.class);
        
        String createViewStatement06 = " CREATE VIEW v0 ";
        assertException(()->(new TextQLParser(string2InputStream(createViewStatement06))).createViewStatement(), ParseException.class);
        
        String createViewStatement08 = " CREATE v0 AS SELECT * FROM a ";
        assertException(()->(new TextQLParser(string2InputStream(createViewStatement08))).createViewStatement(), ParseException.class);
        
        String createViewStatement09 = " VIEW v0 AS SELECT * FROM a ";
        assertException(()->(new TextQLParser(string2InputStream(createViewStatement09))).createViewStatement(), ParseException.class);
    }    

    /**
     * Test if the execution of the given Callable object produces an expected Throwable.
     * @param callable the code to be executed
     * @param expectedThrowable the class of the expected Throwable that the callable should throw
     */ 
    private void assertException(Callable<Object> callable, Class<?> expectedThrowable){
        try{
            callable.call();//run the code
            Assert.fail("Callable did not trow a " + expectedThrowable.getName());//if the call didn't throw an exception that's an error
        }catch(Throwable thrown){
            //Check if got the right kind of exception
            if(!(thrown.getClass().equals(expectedThrowable))){
                Assert.fail("Callable has trown a " + thrown.getClass().getName() + " instead of " + expectedThrowable.getName());//not the right kind of exception
            }
        }
    }
    

    /**
     * Create an InputStream that contain a given { @code String }.
     * @param string The content to be available in the built InputStream.
     * @return An InputStream containing the input string.
     */ 
	private InputStream string2InputStream(String string){
        try {
            // create a PipedOutputStream to write the input string
            PipedOutputStream pipedOutputStream = new PipedOutputStream();
            // create a PipedInputStream that contain the content written to the PipedOutputStream 
            PipedInputStream pipedInputStream = new PipedInputStream(pipedOutputStream);
            // print the string into pipedOutputStream
            PrintStream printStream = new PrintStream(pipedOutputStream);
            printStream.print(string);
            printStream.close();
            // return the build InputStream
            return pipedInputStream;
        } catch (IOException e) {
            //return null if for some reason an IOException is thrown
            return null;
        }
	}
    
}
