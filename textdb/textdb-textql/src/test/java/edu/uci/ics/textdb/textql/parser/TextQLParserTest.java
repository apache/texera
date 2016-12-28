package edu.uci.ics.textdb.textql.parser;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.Test;

import edu.uci.ics.textdb.textql.parser.ParseException;
import edu.uci.ics.textdb.textql.parser.TextQLParser;
import edu.uci.ics.textdb.textql.parser.TokenMgrError;
import edu.uci.ics.textdb.textql.statements.CreateViewStatement;
import edu.uci.ics.textdb.textql.statements.SelectExtractStatement;
import edu.uci.ics.textdb.textql.statements.Statement;
import edu.uci.ics.textdb.textql.statements.predicates.ExtractPredicate;
import edu.uci.ics.textdb.textql.statements.predicates.KeywordExtractPredicate;
import edu.uci.ics.textdb.textql.statements.predicates.SelectPredicate;
import edu.uci.ics.textdb.textql.statements.predicates.SelectAllPredicate;
import edu.uci.ics.textdb.textql.statements.predicates.SelectFieldsPredicate;

import junit.framework.Assert;

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
        Assert.assertEquals((new TextQLParser(new StringReader(" 0 "))).numberLiteralToString(), "0");
        Assert.assertEquals((new TextQLParser(new StringReader(" 12 "))).numberLiteralToString(), "12");
        Assert.assertEquals((new TextQLParser(new StringReader(" 34566 "))).numberLiteralToString(), "34566");
        Assert.assertEquals((new TextQLParser(new StringReader(" 78.90 "))).numberLiteralToString(), "78.90");
        Assert.assertEquals((new TextQLParser(new StringReader(" 123. "))).numberLiteralToString(), "123.");
        Assert.assertEquals((new TextQLParser(new StringReader(" .456 "))).numberLiteralToString(), ".456");
        Assert.assertEquals((new TextQLParser(new StringReader(" -0 "))).numberLiteralToString(), "-0");
        Assert.assertEquals((new TextQLParser(new StringReader(" -12 "))).numberLiteralToString(), "-12");
        Assert.assertEquals((new TextQLParser(new StringReader(" -34566 "))).numberLiteralToString(), "-34566");
        Assert.assertEquals((new TextQLParser(new StringReader(" -78.90 "))).numberLiteralToString(), "-78.90");
        Assert.assertEquals((new TextQLParser(new StringReader(" -123. "))).numberLiteralToString(), "-123.");
        Assert.assertEquals((new TextQLParser(new StringReader(" -.456 "))).numberLiteralToString(), "-.456");
        Assert.assertEquals((new TextQLParser(new StringReader(" -.789.001 "))).numberLiteralToString(), "-.789");
        assertException(()->(new TextQLParser(new StringReader(" -e "))).numberLiteralToString(), TokenMgrError.class);
        assertException(()->(new TextQLParser(new StringReader(" -e 21"))).numberLiteralToString(), TokenMgrError.class);
        assertException(()->(new TextQLParser(new StringReader(" +4 "))).numberLiteralToString(), TokenMgrError.class);
        assertException(()->(new TextQLParser(new StringReader(" a "))).numberLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" a 22 "))).numberLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" a45 "))).numberLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" A45 "))).numberLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" FROM45 "))).numberLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" \"4\" "))).numberLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" /4/ "))).numberLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" /4 "))).numberLiteralToString(), TokenMgrError.class);
        
    }

    /**
     * Test the numberLiteralToDouble method of the parser.
     * It should parse a NUMBER_LITERAL from the input and return it as a double.
     * @throws ParseException if an unexpected ParseException is thrown
     */
    @Test
    public void testNumberLiteralToDouble() throws ParseException {
        Assert.assertEquals((new TextQLParser(new StringReader(" 0 "))).numberLiteralToDouble(), 0.);
        Assert.assertEquals((new TextQLParser(new StringReader(" 12 "))).numberLiteralToDouble(), 12.);
        Assert.assertEquals((new TextQLParser(new StringReader(" 34566 "))).numberLiteralToDouble(), 34566.);
        Assert.assertEquals((new TextQLParser(new StringReader(" 78.90 "))).numberLiteralToDouble(), 78.90);
        Assert.assertEquals((new TextQLParser(new StringReader(" 123. "))).numberLiteralToDouble(), 123.);
        Assert.assertEquals((new TextQLParser(new StringReader(" .456 "))).numberLiteralToDouble(), .456);
        Assert.assertEquals((new TextQLParser(new StringReader(" -0 "))).numberLiteralToDouble(), -0.);
        Assert.assertEquals((new TextQLParser(new StringReader(" -12 "))).numberLiteralToDouble(), -12.);
        Assert.assertEquals((new TextQLParser(new StringReader(" -34566 "))).numberLiteralToDouble(), -34566.);
        Assert.assertEquals((new TextQLParser(new StringReader(" -78.90 "))).numberLiteralToDouble(), -78.90);
        Assert.assertEquals((new TextQLParser(new StringReader(" -123. "))).numberLiteralToDouble(), -123.);
        Assert.assertEquals((new TextQLParser(new StringReader(" -.456 "))).numberLiteralToDouble(), -.456);
        assertException(()->(new TextQLParser(new StringReader(" -e "))).numberLiteralToDouble(), TokenMgrError.class);
        assertException(()->(new TextQLParser(new StringReader(" -e 21"))).numberLiteralToDouble(), TokenMgrError.class);
        assertException(()->(new TextQLParser(new StringReader(" +4 "))).numberLiteralToDouble(), TokenMgrError.class);
        assertException(()->(new TextQLParser(new StringReader(" a "))).numberLiteralToDouble(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" a 22 "))).numberLiteralToDouble(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" a45 "))).numberLiteralToDouble(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" A45 "))).numberLiteralToDouble(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" FROM45 "))).numberLiteralToDouble(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" \"4\" "))).numberLiteralToDouble(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" /4/ "))).numberLiteralToDouble(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" /4 "))).numberLiteralToDouble(), TokenMgrError.class);
    }

    /**
     * Test the numberLiteralToInteger method  of the parser.
     * It should parse a NUMBER_LITERAL from the input and return it as an integer.
     * @throws ParseException if an unexpected ParseException is thrown
     */
    @Test
    public void testNumberLiteralToInteger() throws ParseException {
        Assert.assertEquals((new TextQLParser(new StringReader(" 0 "))).numberLiteralToInteger(), 0);
        Assert.assertEquals((new TextQLParser(new StringReader(" 12 "))).numberLiteralToInteger(), 12);
        Assert.assertEquals((new TextQLParser(new StringReader(" 34566 "))).numberLiteralToInteger(), 34566);
        Assert.assertEquals((new TextQLParser(new StringReader(" -0 "))).numberLiteralToInteger(), 0);
        Assert.assertEquals((new TextQLParser(new StringReader(" -12 "))).numberLiteralToInteger(), -12);
        Assert.assertEquals((new TextQLParser(new StringReader(" -34566 "))).numberLiteralToInteger(), -34566);
        assertException(()->(new TextQLParser(new StringReader(" 78.90 "))).numberLiteralToInteger(), NumberFormatException.class);
        assertException(()->(new TextQLParser(new StringReader(" 123. "))).numberLiteralToInteger(), NumberFormatException.class);
        assertException(()->(new TextQLParser(new StringReader(" .456 "))).numberLiteralToInteger(), NumberFormatException.class);
        assertException(()->(new TextQLParser(new StringReader(" -78.90 "))).numberLiteralToInteger(), NumberFormatException.class);
        assertException(()->(new TextQLParser(new StringReader(" -123. "))).numberLiteralToInteger(), NumberFormatException.class);
        assertException(()->(new TextQLParser(new StringReader(" -.456 "))).numberLiteralToInteger(), NumberFormatException.class);
        assertException(()->(new TextQLParser(new StringReader(" -e "))).numberLiteralToInteger(), TokenMgrError.class);
        assertException(()->(new TextQLParser(new StringReader(" -e 21"))).numberLiteralToInteger(), TokenMgrError.class);
        assertException(()->(new TextQLParser(new StringReader(" +4 "))).numberLiteralToInteger(), TokenMgrError.class);
        assertException(()->(new TextQLParser(new StringReader(" a "))).numberLiteralToInteger(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" a 22 "))).numberLiteralToInteger(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" a45 "))).numberLiteralToInteger(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" A45 "))).numberLiteralToInteger(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" FROM45 "))).numberLiteralToInteger(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" \"4\" "))).numberLiteralToInteger(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" /4/ "))).numberLiteralToInteger(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" /4 "))).numberLiteralToInteger(), TokenMgrError.class);
    }

    /**
     * Test the regexLiteralToString method of the parser.
     * It should parse a REGEX_LITERAL from the input and return it as a string.
     * @throws ParseException if an unexpected ParseException is thrown
     */    
    @Test
    public void testRegexLiteralToString() throws ParseException {
        Assert.assertEquals((new TextQLParser(new StringReader(" // "))).regexLiteralToString(), "");
        Assert.assertEquals((new TextQLParser(new StringReader(" /abc/ "))).regexLiteralToString(), "abc");
        Assert.assertEquals((new TextQLParser(new StringReader(" /d\\/e/ "))).regexLiteralToString(), "d/e");
        Assert.assertEquals((new TextQLParser(new StringReader(" /d\\/e\\/f/ "))).regexLiteralToString(), "d/e/f");
        Assert.assertEquals((new TextQLParser(new StringReader(" /FROM/ "))).regexLiteralToString(), "FROM");
        Assert.assertEquals((new TextQLParser(new StringReader(" /\"/ "))).regexLiteralToString(), "\"");
        Assert.assertEquals((new TextQLParser(new StringReader(" /\\/ "))).regexLiteralToString(), "\\");
        assertException(()->(new TextQLParser(new StringReader(" /21 "))).regexLiteralToString(), TokenMgrError.class);
        assertException(()->(new TextQLParser(new StringReader(" 2/1/ "))).regexLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" \"4/ "))).regexLiteralToString(), TokenMgrError.class);
        assertException(()->(new TextQLParser(new StringReader(" FROM// "))).regexLiteralToString(), ParseException.class);
    }

    /**
     * Test the stringLiteralToString method of the parser.
     * It should parse a STRING_LITERAL from the input and return it as a string.
     * @throws ParseException if an unexpected ParseException is thrown
     */ 
    @Test
    public void testStringLiteralToString() throws ParseException {
        Assert.assertEquals((new TextQLParser(new StringReader(" \"\" "))).stringLiteralToString(), "");
        Assert.assertEquals((new TextQLParser(new StringReader(" \"abc\" "))).stringLiteralToString(), "abc");
        Assert.assertEquals((new TextQLParser(new StringReader(" \"de f\" "))).stringLiteralToString(), "de f");
        Assert.assertEquals((new TextQLParser(new StringReader(" \"d\\\"e\" "))).stringLiteralToString(), "d\"e");
        Assert.assertEquals((new TextQLParser(new StringReader(" \"d\\\"e\\\"f\" "))).stringLiteralToString(), "d\"e\"f");
        Assert.assertEquals((new TextQLParser(new StringReader(" \"\\\"\" "))).stringLiteralToString(), "\"");
        Assert.assertEquals((new TextQLParser(new StringReader(" \"\\\" "))).stringLiteralToString(), "\\");
        assertException(()->(new TextQLParser(new StringReader(" \"21 "))).stringLiteralToString(), TokenMgrError.class);
        assertException(()->(new TextQLParser(new StringReader(" 'aa' "))).stringLiteralToString(), TokenMgrError.class);
        assertException(()->(new TextQLParser(new StringReader(" 2\"1\" "))).stringLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" 21 "))).stringLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" SELECTa "))).stringLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" abc "))).stringLiteralToString(), ParseException.class);
    }

    /**
     * Test the identifierLiteralToString method of the parser.
     * It should parse a IDENTIFIER_LITERAL from the input and return it as a string.
     * @throws ParseException if an unexpected ParseException is thrown
     */ 
    @Test
    public void testIdentifierLiteralToString() throws ParseException {
        Assert.assertEquals((new TextQLParser(new StringReader(" i "))).identifierLiteralToString(), "i");
        Assert.assertEquals((new TextQLParser(new StringReader(" id "))).identifierLiteralToString(), "id");
        Assert.assertEquals((new TextQLParser(new StringReader(" id de "))).identifierLiteralToString(), "id");
        Assert.assertEquals((new TextQLParser(new StringReader(" id0 "))).identifierLiteralToString(), "id0");
        Assert.assertEquals((new TextQLParser(new StringReader(" i6i8s7s "))).identifierLiteralToString(), "i6i8s7s");
        Assert.assertEquals((new TextQLParser(new StringReader(" j7i\\8s7s "))).identifierLiteralToString(), "j7i");
        Assert.assertEquals((new TextQLParser(new StringReader(" k8i\"8s7s "))).identifierLiteralToString(), "k8i");
        Assert.assertEquals((new TextQLParser(new StringReader(" aFROM "))).identifierLiteralToString(), "aFROM");
        Assert.assertEquals((new TextQLParser(new StringReader(" A "))).identifierLiteralToString(), "A");
        Assert.assertEquals((new TextQLParser(new StringReader(" FROMa "))).identifierLiteralToString(), "FROMa");
        Assert.assertEquals((new TextQLParser(new StringReader(" Ed0 "))).identifierLiteralToString(), "Ed0");
        assertException(()->(new TextQLParser(new StringReader(" 2df "))).identifierLiteralToString(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" _a "))).identifierLiteralToString(), TokenMgrError.class);
    }

    /**
     * Test the testIdentifierListToListString method of the parser.
     * It should parse a list of IDENTIFIER_LITERAL from the input and return it as a list of strings.
     * @throws ParseException if an unexpected ParseException is thrown
     */ 
    @Test
    public void testIdentifierListToListString() throws ParseException {
        Assert.assertEquals((new TextQLParser(new StringReader(" i "))).identifierListToListString(), Arrays.asList("i"));
        Assert.assertEquals((new TextQLParser(new StringReader(" id "))).identifierListToListString(), Arrays.asList("id"));
        Assert.assertEquals((new TextQLParser(new StringReader(" id de "))).identifierListToListString(), Arrays.asList("id"));
        Assert.assertEquals((new TextQLParser(new StringReader(" id,de "))).identifierListToListString(), Arrays.asList("id","de"));
        Assert.assertEquals((new TextQLParser(new StringReader(" id0 "))).identifierListToListString(), Arrays.asList("id0"));
        Assert.assertEquals((new TextQLParser(new StringReader(" i6i8s7s "))).identifierListToListString(), Arrays.asList("i6i8s7s"));
        Assert.assertEquals((new TextQLParser(new StringReader(" i6,i8,s7,s "))).identifierListToListString(), Arrays.asList("i6","i8","s7","s"));
        Assert.assertEquals((new TextQLParser(new StringReader(" j7i/8s7s/ "))).identifierListToListString(), Arrays.asList("j7i"));
        Assert.assertEquals((new TextQLParser(new StringReader(" k8i\"8s7s\" "))).identifierListToListString(), Arrays.asList("k8i"));
        Assert.assertEquals((new TextQLParser(new StringReader(" aFROM "))).identifierListToListString(), Arrays.asList("aFROM"));
        Assert.assertEquals((new TextQLParser(new StringReader(" B7FROM "))).identifierListToListString(), Arrays.asList("B7FROM"));
        Assert.assertEquals((new TextQLParser(new StringReader(" A "))).identifierListToListString(), Arrays.asList("A"));
        Assert.assertEquals((new TextQLParser(new StringReader(" FROMa "))).identifierListToListString(), Arrays.asList("FROMa"));
        assertException(()->(new TextQLParser(new StringReader(" j7i,/8s7s/ "))).identifierListToListString(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" k8i,\"8s7s\" "))).identifierListToListString(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" k8i,,k9j "))).identifierListToListString(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" k8i,/8s7s/ "))).identifierListToListString(),ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" k8i, "))).identifierListToListString(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" j7i\\8s7s "))).identifierListToListString(), TokenMgrError.class);
        assertException(()->(new TextQLParser(new StringReader(" k8i\"8s7s "))).identifierListToListString(), TokenMgrError.class);
        assertException(()->(new TextQLParser(new StringReader(" 2df "))).identifierListToListString(), ParseException.class);
        assertException(()->(new TextQLParser(new StringReader(" _a "))).identifierListToListString(), TokenMgrError.class);
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
        Assert.assertEquals((new TextQLParser(new StringReader(keywordMatchPredicate00))).extractKeywordMatchPredicate(), keywordMatchParameters00);
        
        String keywordMatchPredicate01 = " KEYWORDMATCH(g1, \"key2\", conjunction) ";
        ExtractPredicate keywordMatchParameters01 = new KeywordExtractPredicate(Arrays.asList("g1"), "key2", "conjunction");
        Assert.assertEquals((new TextQLParser(new StringReader(keywordMatchPredicate01))).extractKeywordMatchPredicate(), keywordMatchParameters01);
        
        String keywordMatchPredicate04 = " KEYWORDMATCH([g4], \"key0\") ";
        ExtractPredicate keywordMatchParameters04 = new KeywordExtractPredicate(Arrays.asList("g4"), "key0", null);
        Assert.assertEquals((new TextQLParser(new StringReader(keywordMatchPredicate04))).extractKeywordMatchPredicate(), keywordMatchParameters04);
        
        String keywordMatchPredicate06 = " KEYWORDMATCH([g6,g7,h8,i9], \"key\") ";
        ExtractPredicate keywordMatchParameters06 = new KeywordExtractPredicate(Arrays.asList("g6","g7","h8","i9"), "key", null);
        Assert.assertEquals((new TextQLParser(new StringReader(keywordMatchPredicate06))).extractKeywordMatchPredicate(), keywordMatchParameters06);
        
        String keywordMatchPredicate07 = " KEYWORDMATCH([g6,g7,h8,i9], \"key\", substring) ";
        ExtractPredicate keywordMatchParameters07 = new KeywordExtractPredicate(Arrays.asList("g6","g7","h8","i9"), "key", "substring");
        Assert.assertEquals((new TextQLParser(new StringReader(keywordMatchPredicate07))).extractKeywordMatchPredicate(), keywordMatchParameters07);
    }

    /**
     * Test the statement method of the parser.
     * It should parse a statement and return the expected Statement object.
     * @throws ParseException if an unexpected ParseException is thrown
     */ 
    @Test
    public void testStatement() throws ParseException {
        String selectExtractStatement00 = "SELECT * FROM a;";
        SelectPredicate selectExtractStatementSelect00 = new SelectAllPredicate();
        Statement selectExtractStatementParameters00 = new SelectExtractStatement("_sid0", selectExtractStatementSelect00, null, "a", null, null);
        Assert.assertEquals((new TextQLParser(new StringReader(selectExtractStatement00))).statement(), selectExtractStatementParameters00);
        
        String selectExtractStatement06 = "SELECT f8, fa, fc, df, ff FROM j;";
        SelectPredicate selectExtractStatementSelect06 = new SelectFieldsPredicate(Arrays.asList("f8","fa","fc","df","ff"));
        Statement selectExtractStatementParameters06 = new SelectExtractStatement("_sid0", selectExtractStatementSelect06, null, "j", null, null);
        Assert.assertEquals((new TextQLParser(new StringReader(selectExtractStatement06))).statement(), selectExtractStatementParameters06);
        
        String selectExtractStatement13 = "SELECT h, i, j EXTRACT KEYWORDMATCH([h6,h7,k8,k9], \"key5\") FROM q;";
        SelectPredicate selectExtractStatementSelect13 = new SelectFieldsPredicate(Arrays.asList("h","i","j"));
        ExtractPredicate selectExtractStatementExtract13 = new KeywordExtractPredicate(Arrays.asList("h6","h7","k8","k9"), "key5", null);
        Statement selectExtractStatementParameters13 = new SelectExtractStatement("_sid0", selectExtractStatementSelect13, selectExtractStatementExtract13, "q", null, null);
        Assert.assertEquals((new TextQLParser(new StringReader(selectExtractStatement13))).statement(), selectExtractStatementParameters13);
        
        String selectExtractStatement14 = "EXTRACT KEYWORDMATCH([i6,j7,l8,m9], \"key5\") FROM q;";
        ExtractPredicate selectExtractStatementExtract14 = new KeywordExtractPredicate(Arrays.asList("i6","j7","l8","m9"), "key5", null);
        Statement selectExtractStatementParameters14 = new SelectExtractStatement("_sid0", null, selectExtractStatementExtract14, "q", null, null);
        Assert.assertEquals((new TextQLParser(new StringReader(selectExtractStatement14))).statement(), selectExtractStatementParameters14);
        
        String selectExtractStatement21 = "EXTRACT KEYWORDMATCH([h3,i2,j1,k0], \"key\\\"/\") FROM m LIMIT 4 OFFSET 25 ;";
        ExtractPredicate selectExtractStatementExtract21 = new KeywordExtractPredicate(Arrays.asList("h3","i2","j1","k0"), "key\"/", null);
        Statement selectExtractStatementParameters21 = new SelectExtractStatement("_sid0", null, selectExtractStatementExtract21, "m", 4, 25);
        Assert.assertEquals((new TextQLParser(new StringReader(selectExtractStatement21))).statement(), selectExtractStatementParameters21);
        
        String createViewStatement00 = " CREATE VIEW v0 AS SELECT * FROM a; ";
        SelectPredicate createViewStatementSelectP00 = new SelectAllPredicate();
        Statement createViewStatementSelect00 = new SelectExtractStatement("_sid0", createViewStatementSelectP00, null, "a", null, null);
        Statement createViewStatementParameters00 = new CreateViewStatement("v0", createViewStatementSelect00);
        Assert.assertEquals((new TextQLParser(new StringReader(createViewStatement00))).statement(), createViewStatementParameters00);
        
        String createViewStatement01 = " CREATE VIEW v1 AS SELECT f8, fa, fc, df, ff FROM j LIMIT 1 OFFSET 8; ";
        SelectPredicate createViewStatementSelectP01 = new SelectFieldsPredicate(Arrays.asList("f8","fa","fc","df","ff"));
        Statement createViewStatementSelect01 = new SelectExtractStatement("_sid0", createViewStatementSelectP01, null, "j", 1, 8);
        Statement createViewStatementParameters01 = new CreateViewStatement("v1", createViewStatementSelect01);
        Assert.assertEquals((new TextQLParser(new StringReader(createViewStatement01))).statement(), createViewStatementParameters01);
        
        String createViewStatement02 = " CREATE VIEW v2 AS SELECT e EXTRACT KEYWORDMATCH([g4,g5], \"key0\") FROM o ;";
        SelectPredicate createViewStatementSelectP02 = new SelectFieldsPredicate(Arrays.asList("e"));
        ExtractPredicate createViewStatementExtract02 = new KeywordExtractPredicate(Arrays.asList("g4","g5"), "key0", null);
        Statement createViewStatementSelect02 = new SelectExtractStatement("_sid0", createViewStatementSelectP02, createViewStatementExtract02, "o", null, null);
        Statement createViewStatementParameters02 = new CreateViewStatement("v2", createViewStatementSelect02);
        Assert.assertEquals((new TextQLParser(new StringReader(createViewStatement02))).statement(), createViewStatementParameters02);
        
        String createViewStatement03 = " CREATE VIEW v2 AS EXTRACT KEYWORDMATCH([g4,g5], \"key0\", substring) FROM o ;";
        ExtractPredicate createViewStatementExtract03 = new KeywordExtractPredicate(Arrays.asList("g4","g5"), "key0", "substring");
        Statement createViewStatementSelect03 = new SelectExtractStatement("_sid0", null, createViewStatementExtract03, "o", null, null);
        Statement createViewStatementParameters03 = new CreateViewStatement("v2", createViewStatementSelect03);
        Assert.assertEquals((new TextQLParser(new StringReader(createViewStatement03))).statement(), createViewStatementParameters03);

    }

    /**
     * Test the testStatementsMain method of the parser.
     * It should parse a list of statements and return the expected list of Statement object and provide the Statements to the consumer.
     * @throws ParseException if an unexpected ParseException is thrown
     */ 
    @Test
    public void testStatementsMain() throws ParseException {
        //Declaration of multiple statements for testing
        String selectExtractStatement00 = "SELECT * FROM a;";
        String selectExtractStatement13 = "SELECT h, i, j EXTRACT KEYWORDMATCH([h6,h7,k8,k9], \"key5\") FROM q LIMIT 5 OFFSET 6;";
        String selectExtractStatement14 = "EXTRACT KEYWORDMATCH(i6, \"key5\") FROM q;";
        String createViewStatement00 = " CREATE VIEW v0 AS SELECT * FROM a; ";
        String createViewStatement01 = " CREATE VIEW v1 AS SELECT f8, fa, fc, df, ff FROM j LIMIT 1 OFFSET 8; ";
        String createViewStatement02 = " CREATE VIEW v2 AS SELECT e EXTRACT KEYWORDMATCH([g4,g5], \"key0\") FROM o ;";
        String createViewStatement03 = " CREATE VIEW v2 AS EXTRACT KEYWORDMATCH([g4,g5], \"key0\", substring) FROM o ;";
        
        //Example of statement objects used for testing
        SelectPredicate selectExtractStatementSelect00 = new SelectAllPredicate();
        //Statement selectExtractStatementParameters00 = new SelectStatement("_sid0", , null, "a", null, null);

        SelectPredicate selectExtractStatementSelect13 = new SelectFieldsPredicate(Arrays.asList("h","i","j"));
        ExtractPredicate selectExtractStatementExtract13 = new KeywordExtractPredicate(Arrays.asList("h6","h7","k8","k9"), "key5", null);
        //Statement selectExtractStatementParameters13 = new SelectStatement("_sid0", , selectExtractStatementExtract13, "q", 5, 6);

        ExtractPredicate selectExtractStatementExtract14 = new KeywordExtractPredicate(Arrays.asList("i6"), "key5", null);
        //Statement selectExtractStatementParameters14 = new SelectStatement("_sid0", null, selectExtractStatementExtract14, "q", null, null);

        SelectPredicate cfreateViewStatementSelect00 = new SelectAllPredicate();
        //Statement createViewStatementSelect00 = new SelectStatement("_sid0", , null, "a", null, null);
        //Statement createViewStatementParameters00 = new CreateViewStatement("v0", createViewStatementSelect00);
        
        SelectPredicate createViewStatementSelect01 = new SelectFieldsPredicate(Arrays.asList("f8","fa","fc","df","ff"));
        //Statement createViewStatementSelect01 = new SelectStatement("_sid0", , null, "j", 1, 8);
        //Statement createViewStatementParameters01 = new CreateViewStatement("v1", createViewStatementSelect01);

        SelectPredicate createViewStatementSelect02 = new SelectFieldsPredicate(Arrays.asList("e"));
        ExtractPredicate createViewStatementExtract02 = new KeywordExtractPredicate(Arrays.asList("g4","g5"), "key0", null);
        //Statement createViewStatementSelect02 = new SelectStatement("_sid0", , createViewStatementExtract02, "o", null, null);
        //Statement createViewStatementParameters02 = new CreateViewStatement("v2", createViewStatementSelect02);
        
        ExtractPredicate createViewStatementExtract03 = new KeywordExtractPredicate(Arrays.asList("g4","g5"), "key0", "substring");
        //Statement createViewStatementSelect03 = new SelectStatement("_sid0", null, createViewStatementExtract03, "o", null, null);
        //Statement createViewStatementParameters03 = new CreateViewStatement("v2", createViewStatementSelect03);
        
        //Test combinations of statements
        String statements00 = selectExtractStatement00;
        Statement statements00Select = new SelectExtractStatement("_sid0", selectExtractStatementSelect00, null, "a", null, null);
        List<Statement> statements00Result = Arrays.asList(statements00Select);
        Assert.assertEquals((new TextQLParser(new StringReader(statements00))).mainStatementList(null), statements00Result);

        String statements01 = createViewStatement02;
        Statement statements01Select = new SelectExtractStatement("_sid0", createViewStatementSelect02, createViewStatementExtract02, "o", null, null);
        Statement statements01CreateView = new CreateViewStatement("v2", statements01Select);
        List<Statement> statements01Result = Arrays.asList(statements01CreateView);
        Assert.assertEquals((new TextQLParser(new StringReader(statements01))).mainStatementList(null), statements01Result);
        
        String statements02 = createViewStatement02 + selectExtractStatement00;
        Statement statements02Select00 = new SelectExtractStatement("_sid0", createViewStatementSelect02, createViewStatementExtract02, "o", null, null);
        Statement statements02CreateView00 = new CreateViewStatement("v2", statements02Select00);
        Statement statements02Select01 = new SelectExtractStatement("_sid1", selectExtractStatementSelect00, null, "a", null, null);
        List<Statement> statementsResult02 = Arrays.asList(statements02CreateView00, statements02Select01);
        List<Statement> statementsConsumed02 = new ArrayList<>();
        Assert.assertEquals((new TextQLParser(new StringReader(statements02))).mainStatementList(null), statementsResult02);
        Assert.assertEquals((new TextQLParser(new StringReader(statements02))).mainStatementList( s -> statementsConsumed02.add(s) ), statementsResult02);
        Assert.assertEquals(statementsConsumed02, statementsResult02);

        String statements03 = selectExtractStatement00 + createViewStatement00 + createViewStatement03;
        Statement statements03Select00 = new SelectExtractStatement("_sid0", selectExtractStatementSelect00, null, "a", null, null);
        Statement statements03Select01 = new SelectExtractStatement("_sid1", cfreateViewStatementSelect00, null, "a", null, null);
        Statement statements03CreateView01 = new CreateViewStatement("v0", statements03Select01);
        Statement statements03Select02 = new SelectExtractStatement("_sid2", null, createViewStatementExtract03, "o", null, null);
        Statement statements03CreateView02 = new CreateViewStatement("v2", statements03Select02);
        List<Statement> statements03Result = Arrays.asList(statements03Select00, statements03CreateView01, statements03CreateView02);
        List<Statement> statements03Consumed = new ArrayList<>();
        Assert.assertEquals((new TextQLParser(new StringReader(statements03))).mainStatementList(null), statements03Result);
        Assert.assertEquals((new TextQLParser(new StringReader(statements03))).mainStatementList( s -> statements03Consumed.add(s) ), statements03Result);
        Assert.assertEquals(statements03Consumed, statements03Result);
        
        String statements04 = createViewStatement02 + selectExtractStatement14 + selectExtractStatement13;
        Statement statements04Select00 = new SelectExtractStatement("_sid0", createViewStatementSelect02, createViewStatementExtract02, "o", null, null);
        Statement statements04CreateView00 = new CreateViewStatement("v2", statements04Select00);
        Statement statements04Select01 = new SelectExtractStatement("_sid1", null, selectExtractStatementExtract14, "q", null, null);
        Statement statements04Select02 = new SelectExtractStatement("_sid2", selectExtractStatementSelect13, selectExtractStatementExtract13, "q", 5, 6);
        List<Statement> statements04Result = Arrays.asList(statements04CreateView00, statements04Select01, statements04Select02);
        List<Statement> statements04Consumed = new ArrayList<>();
        Assert.assertEquals((new TextQLParser(new StringReader(statements04))).mainStatementList(null), statements04Result);
        Assert.assertEquals((new TextQLParser(new StringReader(statements04))).mainStatementList( s -> statements04Consumed.add(s) ), statements04Result);
        Assert.assertEquals(statements04Consumed, statements04Result);
        
        String statements05 = createViewStatement01 + selectExtractStatement13 + createViewStatement03;
        Statement statements05Select00 = new SelectExtractStatement("_sid0", createViewStatementSelect01, null, "j", 1, 8);
        Statement statements05CreateView00 = new CreateViewStatement("v1", statements05Select00);
        Statement statements05Select01 = new SelectExtractStatement("_sid1", selectExtractStatementSelect13, selectExtractStatementExtract13, "q", 5, 6);
        Statement statements05Select02 = new SelectExtractStatement("_sid2", null, createViewStatementExtract03, "o", null, null);
        Statement statements05CreateView02 = new CreateViewStatement("v2", statements05Select02);
        List<Statement> statements05Result = Arrays.asList(statements05CreateView00, statements05Select01, statements05CreateView02);
        List<Statement> statements05Consumed = new ArrayList<>();
        Assert.assertEquals((new TextQLParser(new StringReader(statements05))).mainStatementList(null), statements05Result);
        Assert.assertEquals((new TextQLParser(new StringReader(statements05))).mainStatementList( s -> statements05Consumed.add(s) ), statements05Result);
        Assert.assertEquals(statements05Consumed, statements05Result);        
    } 

    /**
     * Test the testSelectStatement method of the parser.
     * It should parse a select statements and return the expected SelectStatement object.
     * @throws ParseException if an unexpected ParseException is thrown
     */ 
    @Test
    public void testSelectStatement() throws ParseException {
        String selectExtractStatement00 = "SELECT * FROM a";
        SelectPredicate selectExtractStatementSelect00 = new SelectAllPredicate();
        Statement selectExtractStatementParameters00 = new SelectExtractStatement("_sid0", selectExtractStatementSelect00, null, "a", null, null);
        Assert.assertEquals((new TextQLParser(new StringReader(selectExtractStatement00))).selectExtractStatement(), selectExtractStatementParameters00);
        
        String selectExtractStatement01 = "SELECT * FROM b LIMIT 5";
        SelectPredicate selectExtractStatementSelect01 = new SelectAllPredicate();
        Statement selectExtractStatementParameters01 = new SelectExtractStatement("_sid0", selectExtractStatementSelect01, null, "b", 5, null);
        Assert.assertEquals((new TextQLParser(new StringReader(selectExtractStatement01))).selectExtractStatement(), selectExtractStatementParameters01);
        
        String selectExtractStatement02 = "SELECT * FROM c LIMIT 1 OFFSET 8";
        SelectPredicate selectExtractStatementSelect02 = new SelectAllPredicate();
        Statement selectExtractStatementParameters02 = new SelectExtractStatement("_sid0", selectExtractStatementSelect02, null, "c", 1, 8);
        Assert.assertEquals((new TextQLParser(new StringReader(selectExtractStatement02))).selectExtractStatement(), selectExtractStatementParameters02);
        
        String selectExtractStatement03 = "SELECT * FROM d OFFSET 6";
        SelectPredicate selectExtractStatementSelect03 = new SelectAllPredicate();
        Statement selectExtractStatementParameters03 = new SelectExtractStatement("_sid0", selectExtractStatementSelect03, null, "d", null, 6);
        Assert.assertEquals((new TextQLParser(new StringReader(selectExtractStatement03))).selectExtractStatement(), selectExtractStatementParameters03);
        
        String selectExtractStatement04 = "SELECT f1 FROM e";
        SelectPredicate selectExtractStatementSelect04 = new SelectFieldsPredicate(Arrays.asList("f1"));
        Statement selectExtractStatementParameters04 = new SelectExtractStatement("_sid0", selectExtractStatementSelect04, null, "e", null, null);
        Assert.assertEquals((new TextQLParser(new StringReader(selectExtractStatement04))).selectExtractStatement(), selectExtractStatementParameters04);
        
        String selectExtractStatement05 = "SELECT f1, f5 FROM i";
        SelectPredicate selectExtractStatementSelect05 = new SelectFieldsPredicate(Arrays.asList("f1","f5"));
        Statement selectExtractStatementParameters05 = new SelectExtractStatement("_sid0", selectExtractStatementSelect05, null, "i", null, null);
        Assert.assertEquals((new TextQLParser(new StringReader(selectExtractStatement05))).selectExtractStatement(), selectExtractStatementParameters05);
        
        String selectExtractStatement06 = "SELECT f8, fa, fc, df, ff FROM j";
        SelectPredicate selectExtractStatementSelect06 = new SelectFieldsPredicate(Arrays.asList("f8","fa","fc","df","ff"));
        Statement selectExtractStatementParameters06 = new SelectExtractStatement("_sid0", selectExtractStatementSelect06, null, "j", null, null);
        Assert.assertEquals((new TextQLParser(new StringReader(selectExtractStatement06))).selectExtractStatement(), selectExtractStatementParameters06);
        
        String selectExtractStatement07 = "SELECT a EXTRACT KEYWORDMATCH(g0, \"key1\") FROM k";
        SelectPredicate selectExtractStatementSelect07 = new SelectFieldsPredicate(Arrays.asList("a"));
        ExtractPredicate selectExtractStatementExtract07 = new KeywordExtractPredicate(Arrays.asList("g0"), "key1", null);
        Statement selectExtractStatementParameters07 = new SelectExtractStatement("_sid0", selectExtractStatementSelect07, selectExtractStatementExtract07, "k", null, null);
        Assert.assertEquals((new TextQLParser(new StringReader(selectExtractStatement07))).selectExtractStatement(), selectExtractStatementParameters07);
        
        String selectExtractStatement08 = "SELECT b EXTRACT KEYWORDMATCH(g1, \"key2\", conjunction) FROM l";
        SelectPredicate selectExtractStatementSelect08 = new SelectFieldsPredicate(Arrays.asList("b"));
        ExtractPredicate selectExtractStatementExtract08 = new KeywordExtractPredicate(Arrays.asList("g1"), "key2", "conjunction");
        Statement selectExtractStatementParameters08 = new SelectExtractStatement("_sid0", selectExtractStatementSelect08, selectExtractStatementExtract08, "l", null, null);
        Assert.assertEquals((new TextQLParser(new StringReader(selectExtractStatement08))).selectExtractStatement(), selectExtractStatementParameters08);
        
        String selectExtractStatement10 = "SELECT v EXTRACT KEYWORDMATCH(u, \"keyZ\") FROM t";
        SelectPredicate selectExtractStatementSelect10 = new SelectFieldsPredicate(Arrays.asList("v"));
        ExtractPredicate selectExtractStatementExtract10 = new KeywordExtractPredicate(Arrays.asList("u"), "keyZ", null);
        Statement selectExtractStatementParameters10 = new SelectExtractStatement("_sid0", selectExtractStatementSelect10, selectExtractStatementExtract10, "t", null, null);
        Assert.assertEquals((new TextQLParser(new StringReader(selectExtractStatement10))).selectExtractStatement(), selectExtractStatementParameters10);
        
        String selectExtractStatement11 = "SELECT e EXTRACT KEYWORDMATCH([g4], \"key0\") FROM o";
        SelectPredicate selectExtractStatementSelect11 = new SelectFieldsPredicate(Arrays.asList("e"));
        ExtractPredicate selectExtractStatementExtract11 = new KeywordExtractPredicate(Arrays.asList("g4"), "key0", null);
        Statement selectExtractStatementParameters11 = new SelectExtractStatement("_sid0", selectExtractStatementSelect11, selectExtractStatementExtract11, "o", null, null);
        Assert.assertEquals((new TextQLParser(new StringReader(selectExtractStatement11))).selectExtractStatement(), selectExtractStatementParameters11);
        
        String selectExtractStatement12 = "SELECT f EXTRACT KEYWORDMATCH([g6,g7,h8,i9], \"key\") FROM p";
        SelectPredicate selectExtractStatementSelect12 = new SelectFieldsPredicate(Arrays.asList("f"));
        ExtractPredicate selectExtractStatementExtract12 = new KeywordExtractPredicate(Arrays.asList("g6","g7","h8","i9"), "key", null);
        Statement selectExtractStatementParameters12 = new SelectExtractStatement("_sid0", selectExtractStatementSelect12, selectExtractStatementExtract12, "p", null, null);
        Assert.assertEquals((new TextQLParser(new StringReader(selectExtractStatement12))).selectExtractStatement(), selectExtractStatementParameters12);
        
        String selectExtractStatement13 = "SELECT h, i, j EXTRACT KEYWORDMATCH([h6,h7,k8,k9], \"key5\") FROM q";
        SelectPredicate selectExtractStatementSelect13 = new SelectFieldsPredicate(Arrays.asList("h","i","j"));
        ExtractPredicate selectExtractStatementExtract13 = new KeywordExtractPredicate(Arrays.asList("h6","h7","k8","k9"), "key5", null);
        Statement selectExtractStatementParameters13 = new SelectExtractStatement("_sid0", selectExtractStatementSelect13, selectExtractStatementExtract13, "q", null, null);
        Assert.assertEquals((new TextQLParser(new StringReader(selectExtractStatement13))).selectExtractStatement(), selectExtractStatementParameters13);
        
        String selectExtractStatement14 = "EXTRACT KEYWORDMATCH([i6,j7,l8,m9], \"key5\") FROM q";
        ExtractPredicate selectExtractStatementExtract14 = new KeywordExtractPredicate(Arrays.asList("i6","j7","l8","m9"), "key5", null);
        Statement selectExtractStatementParameters14 = new SelectExtractStatement("_sid0", null, selectExtractStatementExtract14, "q", null, null);
        Assert.assertEquals((new TextQLParser(new StringReader(selectExtractStatement14))).selectExtractStatement(), selectExtractStatementParameters14);
        
        String selectExtractStatement15 = "EXTRACT KEYWORDMATCH(g0, \"key1\") FROM k";
        ExtractPredicate selectExtractStatementExtract15 = new KeywordExtractPredicate(Arrays.asList("g0"), "key1", null);
        Statement selectExtractStatementParameters15 = new SelectExtractStatement("_sid0", null, selectExtractStatementExtract15, "k", null, null);
        Assert.assertEquals((new TextQLParser(new StringReader(selectExtractStatement15))).selectExtractStatement(), selectExtractStatementParameters15);
        
        String selectExtractStatement16 = "EXTRACT KEYWORDMATCH(g1, \"key2\", phrase) FROM l";
        ExtractPredicate selectExtractStatementExtract16 = new KeywordExtractPredicate(Arrays.asList("g1"), "key2", "phrase");
        Statement selectExtractStatementParameters16 = new SelectExtractStatement("_sid0", null, selectExtractStatementExtract16, "l", null, null);
        Assert.assertEquals((new TextQLParser(new StringReader(selectExtractStatement16))).selectExtractStatement(), selectExtractStatementParameters16);
        
        String selectExtractStatement19 = "EXTRACT KEYWORDMATCH([g4], \"key0\") FROM o";
        ExtractPredicate selectExtractStatementExtract19 = new KeywordExtractPredicate(Arrays.asList("g4"), "key0", null);
        Statement selectExtractStatementParameters19 = new SelectExtractStatement("_sid0", null, selectExtractStatementExtract19, "o", null, null);
        Assert.assertEquals((new TextQLParser(new StringReader(selectExtractStatement19))).selectExtractStatement(), selectExtractStatementParameters19);
        
        String selectExtractStatement20 = "EXTRACT KEYWORDMATCH([g6,g7,h8,i9], \"key\") FROM p";
        ExtractPredicate selectExtractStatementExtract20 = new KeywordExtractPredicate(Arrays.asList("g6","g7","h8","i9"), "key", null);
        Statement selectExtractStatementParameters20 = new SelectExtractStatement("_sid0", null, selectExtractStatementExtract20, "p", null, null);
        Assert.assertEquals((new TextQLParser(new StringReader(selectExtractStatement20))).selectExtractStatement(), selectExtractStatementParameters20);
        
        String selectExtractStatement21 = "EXTRACT KEYWORDMATCH([h3,i2,j1,k0], \"key\\\"/\") FROM m LIMIT 4 OFFSET 25 ";
        ExtractPredicate selectExtractStatementExtract21 = new KeywordExtractPredicate(Arrays.asList("h3","i2","j1","k0"), "key\"/", null);
        Statement selectExtractStatementParameters21 = new SelectExtractStatement("_sid0", null, selectExtractStatementExtract21, "m", 4, 25);
        Assert.assertEquals((new TextQLParser(new StringReader(selectExtractStatement21))).selectExtractStatement(), selectExtractStatementParameters21);
        
        String selectExtractStatement22 = "SELECT FROM a";
        assertException(()->(new TextQLParser(new StringReader(selectExtractStatement22))).selectExtractStatement(), ParseException.class);
        
        String selectExtractStatement23 = "SELECT FROM a OFFSET 5 LIMIT 6";
        assertException(()->(new TextQLParser(new StringReader(selectExtractStatement23))).selectExtractStatement(), ParseException.class);
        
        String selectExtractStatement24 = "SELECT 25 FROM a";
        assertException(()->(new TextQLParser(new StringReader(selectExtractStatement24))).selectExtractStatement(), ParseException.class);
        
        String selectExtractStatement25 = "SELECT [a,b] FROM a";
        assertException(()->(new TextQLParser(new StringReader(selectExtractStatement25))).selectExtractStatement(), ParseException.class);
        
        String selectExtractStatement26 = "SELECT *,a FROM a";
        assertException(()->(new TextQLParser(new StringReader(selectExtractStatement26))).selectExtractStatement(), ParseException.class);
        
        String selectExtractStatement27 = "SELECT * FROM [a,b]";
        assertException(()->(new TextQLParser(new StringReader(selectExtractStatement27))).selectExtractStatement(), ParseException.class);
        
        String selectExtractStatement28 = "SELECT KEYWORDMATCH(g0, \"key1\") FROM a";
        assertException(()->(new TextQLParser(new StringReader(selectExtractStatement28))).selectExtractStatement(), ParseException.class);
        
        String selectExtractStatement29 = "SELECT EXTRACT KEYWORDMATCH(g0, \"key1\") FROM a";
        assertException(()->(new TextQLParser(new StringReader(selectExtractStatement29))).selectExtractStatement(), ParseException.class);
        
        String selectExtractStatement30 = "EXTRACT a FROM a";
        assertException(()->(new TextQLParser(new StringReader(selectExtractStatement30))).selectExtractStatement(), ParseException.class);
        
        String selectExtractStatement31 = "EXTRACT * FROM a";
        assertException(()->(new TextQLParser(new StringReader(selectExtractStatement31))).selectExtractStatement(), ParseException.class);
        
        String selectExtractStatement32 = "EXTRACT KEYWORDMATCH(g0, \"key1\") SELECT a FROM k";
        assertException(()->(new TextQLParser(new StringReader(selectExtractStatement32))).selectExtractStatement(), ParseException.class);
        
        String selectExtractStatement33 = "SELECT a";
        assertException(()->(new TextQLParser(new StringReader(selectExtractStatement33))).selectExtractStatement(), ParseException.class);
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
        Assert.assertEquals((new TextQLParser(new StringReader(keywordMatchPredicate00))).extractKeywordMatchPredicate(), keywordMatchParameters00);
        
        String keywordMatchPredicate01 = " KEYWORDMATCH(g1, \"key2\", conjunction) ";
        ExtractPredicate keywordMatchParameters01 = new KeywordExtractPredicate(Arrays.asList("g1"), "key2", "conjunction");
        Assert.assertEquals((new TextQLParser(new StringReader(keywordMatchPredicate01))).extractKeywordMatchPredicate(), keywordMatchParameters01);
        
        String keywordMatchPredicate02 = " KEYWORDMATCH(g2, \"key3\", phrase) ";
        ExtractPredicate keywordMatchParameters02 = new KeywordExtractPredicate(Arrays.asList("g2"), "key3", "phrase");
        Assert.assertEquals((new TextQLParser(new StringReader(keywordMatchPredicate02))).extractKeywordMatchPredicate(), keywordMatchParameters02);
        
        String keywordMatchPredicate03 = " KEYWORDMATCH(g3, \"key4\", substring) ";
        ExtractPredicate keywordMatchParameters03 = new KeywordExtractPredicate(Arrays.asList("g3"), "key4", "substring");
        Assert.assertEquals((new TextQLParser(new StringReader(keywordMatchPredicate03))).extractKeywordMatchPredicate(), keywordMatchParameters03);
        
        String keywordMatchPredicate04 = " KEYWORDMATCH([g4], \"key0\") ";
        ExtractPredicate keywordMatchParameters04 = new KeywordExtractPredicate(Arrays.asList("g4"), "key0", null);
        Assert.assertEquals((new TextQLParser(new StringReader(keywordMatchPredicate04))).extractKeywordMatchPredicate(), keywordMatchParameters04);
        
        String keywordMatchPredicate05 = " KEYWORDMATCH([g4,g5], \"key0\") ";
        ExtractPredicate keywordMatchParameters05 = new KeywordExtractPredicate(Arrays.asList("g4","g5"), "key0", null);
        Assert.assertEquals((new TextQLParser(new StringReader(keywordMatchPredicate05))).extractKeywordMatchPredicate(), keywordMatchParameters05);
        
        String keywordMatchPredicate06 = " KEYWORDMATCH([g6,g7,h8,i9], \"key\") ";
        ExtractPredicate keywordMatchParameters06 = new KeywordExtractPredicate(Arrays.asList("g6","g7","h8","i9"), "key", null);
        Assert.assertEquals((new TextQLParser(new StringReader(keywordMatchPredicate06))).extractKeywordMatchPredicate(), keywordMatchParameters06);

        String keywordMatchPredicate07 = " KEYWORDMATCH([g6,g7,h8,i9], \"key\", substring) ";
        ExtractPredicate keywordMatchParameters07 = new KeywordExtractPredicate(Arrays.asList("g6","g7","h8","i9"), "key", "substring");
        Assert.assertEquals((new TextQLParser(new StringReader(keywordMatchPredicate07))).extractKeywordMatchPredicate(), keywordMatchParameters07);
                
        String keywordMatchPredicate08 = " KEYWORDMATCH ([i6,j7,l8,m9, \"key5\") ";
        assertException(()->(new TextQLParser(new StringReader(keywordMatchPredicate08))).extractKeywordMatchPredicate(), ParseException.class);
        
        String keywordMatchPredicate09 = " KEYWORDMATCH (i6,j7,l8,m9, \"key5\") ";
        assertException(()->(new TextQLParser(new StringReader(keywordMatchPredicate09))).extractKeywordMatchPredicate(), ParseException.class);
        
        String keywordMatchPredicate10 = " KEYWORDMATCH (i6,j7,l8,m9], \"key5\") ";
        assertException(()->(new TextQLParser(new StringReader(keywordMatchPredicate10))).extractKeywordMatchPredicate(), ParseException.class);
        
        String keywordMatchPredicate11 = " KEYWORDMATCH ([i6,j7,l8,m9, \"key5\", conjunction) ";
        assertException(()->(new TextQLParser(new StringReader(keywordMatchPredicate11))).extractKeywordMatchPredicate(), ParseException.class);
        
        String keywordMatchPredicate12 = " KEYWORDMATCH (i6,j7,l8,m9, \"key5\", substring) ";
        assertException(()->(new TextQLParser(new StringReader(keywordMatchPredicate12))).extractKeywordMatchPredicate(), ParseException.class);
        
        String keywordMatchPredicate13 = " KEYWORDMATCH ([i6,j7,l8,m9, \"key5\", phrase) ";
        assertException(()->(new TextQLParser(new StringReader(keywordMatchPredicate13))).extractKeywordMatchPredicate(), ParseException.class);
        
        String keywordMatchPredicate14 = " KEYWORDMATCH ([], key5) ";
        assertException(()->(new TextQLParser(new StringReader(keywordMatchPredicate14))).extractKeywordMatchPredicate(), ParseException.class);
        
        String keywordMatchPredicate15 = " KEYWORDMATCH ([a], key5) ";
        assertException(()->(new TextQLParser(new StringReader(keywordMatchPredicate15))).extractKeywordMatchPredicate(), ParseException.class);
        
        String keywordMatchPredicate16 = " KEYWORDMATCH ([a]) ";
        assertException(()->(new TextQLParser(new StringReader(keywordMatchPredicate16))).extractKeywordMatchPredicate(), ParseException.class);
        
        String keywordMatchPredicate17 = " KEYWORDMATCH (\"key1\") ";
        assertException(()->(new TextQLParser(new StringReader(keywordMatchPredicate17))).extractKeywordMatchPredicate(), ParseException.class);
        
    }

    /**
     * Test the testCreateViewStatement method of the parser.
     * It should parse a create view statement and return the expected CreateViewStatement object.
     * @throws ParseException if an unexpected ParseException is thrown
     */ 
    @Test
    public void testCreateViewStatement() throws ParseException {
        String createViewStatement00 = " CREATE VIEW v0 AS SELECT * FROM a ";
        SelectPredicate createViewStatementSelectP00 = new SelectAllPredicate();
        Statement createViewStatementSelect00 = new SelectExtractStatement("_sid0", createViewStatementSelectP00, null, "a", null, null);
        Statement createViewStatementParameters00 = new CreateViewStatement("v0", createViewStatementSelect00);
        Assert.assertEquals((new TextQLParser(new StringReader(createViewStatement00))).createViewStatement(), createViewStatementParameters00);
        
        String createViewStatement01 = " CREATE VIEW v1 AS SELECT f8, fa, fc, df, ff FROM j LIMIT 1 OFFSET 8 ";
        SelectPredicate createViewStatementSelectP01 =  new SelectFieldsPredicate(Arrays.asList("f8","fa","fc","df","ff"));
        Statement createViewStatementSelect01 = new SelectExtractStatement("_sid0", createViewStatementSelectP01, null, "j", 1, 8);
        Statement createViewStatementParameters01 = new CreateViewStatement("v1", createViewStatementSelect01);
        Assert.assertEquals((new TextQLParser(new StringReader(createViewStatement01))).createViewStatement(), createViewStatementParameters01);
        
        String createViewStatement02 = " CREATE VIEW v2 AS SELECT e EXTRACT KEYWORDMATCH([g4,g5], \"key0\") FROM o ";
        SelectPredicate createViewStatementSelectP02 = new SelectFieldsPredicate(Arrays.asList("e"));
        ExtractPredicate createViewStatementExtract02 = new KeywordExtractPredicate(Arrays.asList("g4","g5"), "key0", null);
        Statement createViewStatementSelect02 = new SelectExtractStatement("_sid0", createViewStatementSelectP02, createViewStatementExtract02, "o", null, null);
        Statement createViewStatementParameters02 = new CreateViewStatement("v2", createViewStatementSelect02);
        Assert.assertEquals((new TextQLParser(new StringReader(createViewStatement02))).createViewStatement(), createViewStatementParameters02);
        
        String createViewStatement03 = " CREATE VIEW v2 AS EXTRACT KEYWORDMATCH([g4,g5], \"key0\", substring) FROM o ";
        ExtractPredicate createViewStatementExtract03 = new KeywordExtractPredicate(Arrays.asList("g4","g5"), "key0", "substring");
        Statement createViewStatementSelect03 = new SelectExtractStatement("_sid0", null, createViewStatementExtract03, "o", null, null);
        Statement createViewStatementParameters03 = new CreateViewStatement("v2", createViewStatementSelect03);
        Assert.assertEquals((new TextQLParser(new StringReader(createViewStatement03))).createViewStatement(), createViewStatementParameters03);
        
        String createViewStatement04 = " CREATE VIEW v3 AS CREATE VIEW v4 AS SELECT * FROM a ";
        assertException(()->(new TextQLParser(new StringReader(createViewStatement04))).createViewStatement(), ParseException.class);
        
        String createViewStatement05 = " CREATE VIEW v0 AS ";
        assertException(()->(new TextQLParser(new StringReader(createViewStatement05))).createViewStatement(), ParseException.class);
        
        String createViewStatement06 = " CREATE VIEW v0 ";
        assertException(()->(new TextQLParser(new StringReader(createViewStatement06))).createViewStatement(), ParseException.class);
        
        String createViewStatement08 = " CREATE v0 AS SELECT * FROM a ";
        assertException(()->(new TextQLParser(new StringReader(createViewStatement08))).createViewStatement(), ParseException.class);
        
        String createViewStatement09 = " VIEW v0 AS SELECT * FROM a ";
        assertException(()->(new TextQLParser(new StringReader(createViewStatement09))).createViewStatement(), ParseException.class);
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
    
}
