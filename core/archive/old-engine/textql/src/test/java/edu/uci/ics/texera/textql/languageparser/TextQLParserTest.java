package edu.uci.ics.texera.textql.languageparser;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.Test;

import edu.uci.ics.texera.textql.languageparser.ParseException;
import edu.uci.ics.texera.textql.languageparser.TextQLParser;
import edu.uci.ics.texera.textql.languageparser.TokenMgrError;
import edu.uci.ics.texera.textql.statements.CreateViewStatement;
import edu.uci.ics.texera.textql.statements.SelectStatement;
import edu.uci.ics.texera.textql.statements.Statement;
import edu.uci.ics.texera.textql.statements.predicates.ExtractPredicate;
import edu.uci.ics.texera.textql.statements.predicates.KeywordExtractPredicate;
import edu.uci.ics.texera.textql.statements.predicates.ProjectPredicate;
import edu.uci.ics.texera.textql.statements.predicates.ProjectAllFieldsPredicate;
import edu.uci.ics.texera.textql.statements.predicates.ProjectSomeFieldsPredicate;

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
     * Test the identifierListToListString method of the parser.
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
        Assert.assertEquals((new TextQLParser(string2InputStream(" j7i,/8s7s/ "))).identifierListToListString(), Arrays.asList("j7i"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" k8i,\"8s7s\" "))).identifierListToListString(), Arrays.asList("k8i"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" k8i,,k9j "))).identifierListToListString(), Arrays.asList("k8i"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" k8i,/8s7s/ "))).identifierListToListString(), Arrays.asList("k8i"));
        Assert.assertEquals((new TextQLParser(string2InputStream(" k8i, "))).identifierListToListString(), Arrays.asList("k8i"));
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
        String SelectStatement00 = "SELECT * FROM a;";
        ProjectPredicate SelectStatementSelect00 = new ProjectAllFieldsPredicate();
        Statement SelectStatementParameters00 = new SelectStatement("_sid0", SelectStatementSelect00, null, "a", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(SelectStatement00))).statement(), SelectStatementParameters00);
        
        String SelectStatement06 = "SELECT f8, fa, fc, df, ff FROM j;";
        ProjectPredicate SelectStatementSelect06 = new ProjectSomeFieldsPredicate(Arrays.asList("f8","fa","fc","df","ff"));
        Statement SelectStatementParameters06 = new SelectStatement("_sid0", SelectStatementSelect06, null, "j", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(SelectStatement06))).statement(), SelectStatementParameters06);
        
        String SelectStatement13 = "SELECT h, i, j, KEYWORDMATCH([h6,h7,k8,k9], \"key5\") FROM q;";
        ProjectPredicate SelectStatementSelect13 = new ProjectSomeFieldsPredicate(Arrays.asList("h","i","j"));
        ExtractPredicate SelectStatementExtract13 = new KeywordExtractPredicate(Arrays.asList("h6","h7","k8","k9"), "key5", null);
        Statement SelectStatementParameters13 = new SelectStatement("_sid0", SelectStatementSelect13, SelectStatementExtract13, "q", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(SelectStatement13))).statement(), SelectStatementParameters13);
        
        String SelectStatement14 = "SELECT KEYWORDMATCH([i6,j7,l8,m9], \"key5\") FROM q;";
        ExtractPredicate SelectStatementExtract14 = new KeywordExtractPredicate(Arrays.asList("i6","j7","l8","m9"), "key5", null);
        Statement SelectStatementParameters14 = new SelectStatement("_sid0", null, SelectStatementExtract14, "q", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(SelectStatement14))).statement(), SelectStatementParameters14);
        
        String SelectStatement21 = "SELECT KEYWORDMATCH([h3,i2,j1,k0], \"key\\\"/\") FROM m LIMIT 4 OFFSET 25 ;";
        ExtractPredicate SelectStatementExtract21 = new KeywordExtractPredicate(Arrays.asList("h3","i2","j1","k0"), "key\"/", null);
        Statement SelectStatementParameters21 = new SelectStatement("_sid0", null, SelectStatementExtract21, "m", 4, 25);
        Assert.assertEquals((new TextQLParser(string2InputStream(SelectStatement21))).statement(), SelectStatementParameters21);
        
        String createViewStatement00 = " CREATE VIEW v0 AS SELECT * FROM a; ";
        ProjectPredicate createViewStatementSelectP00 = new ProjectAllFieldsPredicate();
        Statement createViewStatementSelect00 = new SelectStatement("_sid0", createViewStatementSelectP00, null, "a", null, null);
        Statement createViewStatementParameters00 = new CreateViewStatement("v0", createViewStatementSelect00);
        Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement00))).statement(), createViewStatementParameters00);
        
        String createViewStatement01 = " CREATE VIEW v1 AS SELECT f8, fa, fc, df, ff FROM j LIMIT 1 OFFSET 8; ";
        ProjectPredicate createViewStatementSelectP01 = new ProjectSomeFieldsPredicate(Arrays.asList("f8","fa","fc","df","ff"));
        Statement createViewStatementSelect01 = new SelectStatement("_sid0", createViewStatementSelectP01, null, "j", 1, 8);
        Statement createViewStatementParameters01 = new CreateViewStatement("v1", createViewStatementSelect01);
        Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement01))).statement(), createViewStatementParameters01);
        
        String createViewStatement02 = " CREATE VIEW v2 AS SELECT e, KEYWORDMATCH([g4,g5], \"key0\") FROM o ;";
        ProjectPredicate createViewStatementSelectP02 = new ProjectSomeFieldsPredicate(Arrays.asList("e"));
        ExtractPredicate createViewStatementExtract02 = new KeywordExtractPredicate(Arrays.asList("g4","g5"), "key0", null);
        Statement createViewStatementSelect02 = new SelectStatement("_sid0", createViewStatementSelectP02, createViewStatementExtract02, "o", null, null);
        Statement createViewStatementParameters02 = new CreateViewStatement("v2", createViewStatementSelect02);
        Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement02))).statement(), createViewStatementParameters02);
        
        String createViewStatement03 = " CREATE VIEW v2 AS SELECT KEYWORDMATCH([g4,g5], \"key0\", substring) FROM o ;";
        ExtractPredicate createViewStatementExtract03 = new KeywordExtractPredicate(Arrays.asList("g4","g5"), "key0", "substring");
        Statement createViewStatementSelect03 = new SelectStatement("_sid0", null, createViewStatementExtract03, "o", null, null);
        Statement createViewStatementParameters03 = new CreateViewStatement("v2", createViewStatementSelect03);
        Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement03))).statement(), createViewStatementParameters03);

    }

    /**
     * Test the statementsMain method of the parser.
     * It should parse a list of statements and return the expected list
     * of Statement object and provide the Statements to the consumer.
     * @throws ParseException if an unexpected ParseException is thrown
     */ 
    @Test
    public void testStatementsMain() throws ParseException {
        //Declaration of multiple statements for testing
        String SelectStatement00 = "SELECT * FROM a;";
        String SelectStatement13 = "SELECT h, i, j, KEYWORDMATCH([h6,h7,k8,k9], \"key5\") FROM q LIMIT 5 OFFSET 6;";
        String SelectStatement14 = "SELECT KEYWORDMATCH(i6, \"key5\") FROM q;";
        String createViewStatement00 = " CREATE VIEW v0 AS SELECT * FROM a; ";
        String createViewStatement01 = " CREATE VIEW v1 AS SELECT f8, fa, fc, df, ff FROM j LIMIT 1 OFFSET 8; ";
        String createViewStatement02 = " CREATE VIEW v2 AS SELECT e, KEYWORDMATCH([g4,g5], \"key0\") FROM o ;";
        String createViewStatement03 = " CREATE VIEW v2 AS SELECT KEYWORDMATCH([g4,g5], \"key0\", substring) FROM o ;";
        
        //Example of statement objects used for testing
        ProjectPredicate SelectStatementSelect00 = new ProjectAllFieldsPredicate();
        //Statement SelectStatementParameters00 = new SelectStatement("_sid0", , null, "a", null, null);

        ProjectPredicate SelectStatementSelect13 = new ProjectSomeFieldsPredicate(Arrays.asList("h","i","j"));
        ExtractPredicate SelectStatementExtract13 = new KeywordExtractPredicate(Arrays.asList("h6","h7","k8","k9"), "key5", null);
        //Statement SelectStatementParameters13 = new SelectStatement("_sid0", , SelectStatementExtract13, "q", 5, 6);

        ExtractPredicate SelectStatementExtract14 = new KeywordExtractPredicate(Arrays.asList("i6"), "key5", null);
        //Statement SelectStatementParameters14 = new SelectStatement("_sid0", null, SelectStatementExtract14, "q", null, null);

        ProjectPredicate cfreateViewStatementSelect00 = new ProjectAllFieldsPredicate();
        //Statement createViewStatementSelect00 = new SelectStatement("_sid0", , null, "a", null, null);
        //Statement createViewStatementParameters00 = new CreateViewStatement("v0", createViewStatementSelect00);
        
        ProjectPredicate createViewStatementSelect01 = new ProjectSomeFieldsPredicate(Arrays.asList("f8","fa","fc","df","ff"));
        //Statement createViewStatementSelect01 = new SelectStatement("_sid0", , null, "j", 1, 8);
        //Statement createViewStatementParameters01 = new CreateViewStatement("v1", createViewStatementSelect01);

        ProjectPredicate createViewStatementSelect02 = new ProjectSomeFieldsPredicate(Arrays.asList("e"));
        ExtractPredicate createViewStatementExtract02 = new KeywordExtractPredicate(Arrays.asList("g4","g5"), "key0", null);
        //Statement createViewStatementSelect02 = new SelectStatement("_sid0", , createViewStatementExtract02, "o", null, null);
        //Statement createViewStatementParameters02 = new CreateViewStatement("v2", createViewStatementSelect02);
        
        ExtractPredicate createViewStatementExtract03 = new KeywordExtractPredicate(Arrays.asList("g4","g5"), "key0", "substring");
        //Statement createViewStatementSelect03 = new SelectStatement("_sid0", null, createViewStatementExtract03, "o", null, null);
        //Statement createViewStatementParameters03 = new CreateViewStatement("v2", createViewStatementSelect03);
        
        //Test combinations of statements
        String statements00 = SelectStatement00;
        Statement statements00Select = new SelectStatement("_sid0", SelectStatementSelect00, null, "a", null, null);
        List<Statement> statements00Result = Arrays.asList(statements00Select);
        Assert.assertEquals((new TextQLParser(string2InputStream(statements00))).mainStatementList(null), statements00Result);

        String statements01 = createViewStatement02;
        Statement statements01Select = new SelectStatement("_sid0", createViewStatementSelect02, createViewStatementExtract02, "o", null, null);
        Statement statements01CreateView = new CreateViewStatement("v2", statements01Select);
        List<Statement> statements01Result = Arrays.asList(statements01CreateView);
        Assert.assertEquals((new TextQLParser(string2InputStream(statements01))).mainStatementList(null), statements01Result);
        
        String statements02 = createViewStatement02 + SelectStatement00;
        Statement statements02Select00 = new SelectStatement("_sid0", createViewStatementSelect02, createViewStatementExtract02, "o", null, null);
        Statement statements02CreateView00 = new CreateViewStatement("v2", statements02Select00);
        Statement statements02Select01 = new SelectStatement("_sid1", SelectStatementSelect00, null, "a", null, null);
        List<Statement> statementsResult02 = Arrays.asList(statements02CreateView00, statements02Select01);
        List<Statement> statementsConsumed02 = new ArrayList<>();
        Assert.assertEquals((new TextQLParser(string2InputStream(statements02))).mainStatementList(null), statementsResult02);
        Assert.assertEquals((new TextQLParser(string2InputStream(statements02))).mainStatementList( s -> statementsConsumed02.add(s) ), statementsResult02);
        Assert.assertEquals(statementsConsumed02, statementsResult02);

        String statements03 = SelectStatement00 + createViewStatement00 + createViewStatement03;
        Statement statements03Select00 = new SelectStatement("_sid0", SelectStatementSelect00, null, "a", null, null);
        Statement statements03Select01 = new SelectStatement("_sid1", cfreateViewStatementSelect00, null, "a", null, null);
        Statement statements03CreateView01 = new CreateViewStatement("v0", statements03Select01);
        Statement statements03Select02 = new SelectStatement("_sid2", null, createViewStatementExtract03, "o", null, null);
        Statement statements03CreateView02 = new CreateViewStatement("v2", statements03Select02);
        List<Statement> statements03Result = Arrays.asList(statements03Select00, statements03CreateView01, statements03CreateView02);
        List<Statement> statements03Consumed = new ArrayList<>();
        Assert.assertEquals((new TextQLParser(string2InputStream(statements03))).mainStatementList(null), statements03Result);
        Assert.assertEquals((new TextQLParser(string2InputStream(statements03))).mainStatementList( s -> statements03Consumed.add(s) ), statements03Result);
        Assert.assertEquals(statements03Consumed, statements03Result);
        
        String statements04 = createViewStatement02 + SelectStatement14 + SelectStatement13;
        Statement statements04Select00 = new SelectStatement("_sid0", createViewStatementSelect02, createViewStatementExtract02, "o", null, null);
        Statement statements04CreateView00 = new CreateViewStatement("v2", statements04Select00);
        Statement statements04Select01 = new SelectStatement("_sid1", null, SelectStatementExtract14, "q", null, null);
        Statement statements04Select02 = new SelectStatement("_sid2", SelectStatementSelect13, SelectStatementExtract13, "q", 5, 6);
        List<Statement> statements04Result = Arrays.asList(statements04CreateView00, statements04Select01, statements04Select02);
        List<Statement> statements04Consumed = new ArrayList<>();
        Assert.assertEquals((new TextQLParser(string2InputStream(statements04))).mainStatementList(null), statements04Result);
        Assert.assertEquals((new TextQLParser(string2InputStream(statements04))).mainStatementList( s -> statements04Consumed.add(s) ), statements04Result);
        Assert.assertEquals(statements04Consumed, statements04Result);
        
        String statements05 = createViewStatement01 + SelectStatement13 + createViewStatement03;
        Statement statements05Select00 = new SelectStatement("_sid0", createViewStatementSelect01, null, "j", 1, 8);
        Statement statements05CreateView00 = new CreateViewStatement("v1", statements05Select00);
        Statement statements05Select01 = new SelectStatement("_sid1", SelectStatementSelect13, SelectStatementExtract13, "q", 5, 6);
        Statement statements05Select02 = new SelectStatement("_sid2", null, createViewStatementExtract03, "o", null, null);
        Statement statements05CreateView02 = new CreateViewStatement("v2", statements05Select02);
        List<Statement> statements05Result = Arrays.asList(statements05CreateView00, statements05Select01, statements05CreateView02);
        List<Statement> statements05Consumed = new ArrayList<>();
        Assert.assertEquals((new TextQLParser(string2InputStream(statements05))).mainStatementList(null), statements05Result);
        Assert.assertEquals((new TextQLParser(string2InputStream(statements05))).mainStatementList( s -> statements05Consumed.add(s) ), statements05Result);
        Assert.assertEquals(statements05Consumed, statements05Result);        
    } 

    /**
     * Test the selectStatement method of the parser.
     * It should parse a select statements and return the expected SelectStatement object.
     * @throws ParseException if an unexpected ParseException is thrown
     */ 
    @Test
    public void testSelectStatement() throws ParseException {
        String SelectStatement00 = "SELECT * FROM a";
        ProjectPredicate SelectStatementSelect00 = new ProjectAllFieldsPredicate();
        Statement SelectStatementParameters00 = new SelectStatement("_sid0", SelectStatementSelect00, null, "a", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(SelectStatement00))).selectStatement(), SelectStatementParameters00);
        
        String SelectStatement01 = "SELECT * FROM b LIMIT 5";
        ProjectPredicate SelectStatementSelect01 = new ProjectAllFieldsPredicate();
        Statement SelectStatementParameters01 = new SelectStatement("_sid0", SelectStatementSelect01, null, "b", 5, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(SelectStatement01))).selectStatement(), SelectStatementParameters01);
        
        String SelectStatement02 = "SELECT * FROM c LIMIT 1 OFFSET 8";
        ProjectPredicate SelectStatementSelect02 = new ProjectAllFieldsPredicate();
        Statement SelectStatementParameters02 = new SelectStatement("_sid0", SelectStatementSelect02, null, "c", 1, 8);
        Assert.assertEquals((new TextQLParser(string2InputStream(SelectStatement02))).selectStatement(), SelectStatementParameters02);
        
        String SelectStatement03 = "SELECT * FROM d OFFSET 6";
        ProjectPredicate SelectStatementSelect03 = new ProjectAllFieldsPredicate();
        Statement SelectStatementParameters03 = new SelectStatement("_sid0", SelectStatementSelect03, null, "d", null, 6);
        Assert.assertEquals((new TextQLParser(string2InputStream(SelectStatement03))).selectStatement(), SelectStatementParameters03);
        
        String SelectStatement04 = "SELECT f1 FROM e";
        ProjectPredicate SelectStatementSelect04 = new ProjectSomeFieldsPredicate(Arrays.asList("f1"));
        Statement SelectStatementParameters04 = new SelectStatement("_sid0", SelectStatementSelect04, null, "e", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(SelectStatement04))).selectStatement(), SelectStatementParameters04);
        
        String SelectStatement05 = "SELECT f1, f5 FROM i";
        ProjectPredicate SelectStatementSelect05 = new ProjectSomeFieldsPredicate(Arrays.asList("f1","f5"));
        Statement SelectStatementParameters05 = new SelectStatement("_sid0", SelectStatementSelect05, null, "i", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(SelectStatement05))).selectStatement(), SelectStatementParameters05);
        
        String SelectStatement06 = "SELECT f8, fa, fc, df, ff FROM j";
        ProjectPredicate SelectStatementSelect06 = new ProjectSomeFieldsPredicate(Arrays.asList("f8","fa","fc","df","ff"));
        Statement SelectStatementParameters06 = new SelectStatement("_sid0", SelectStatementSelect06, null, "j", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(SelectStatement06))).selectStatement(), SelectStatementParameters06);
        
        String SelectStatement07 = "SELECT a, KEYWORDMATCH(g0, \"key1\") FROM k";
        ProjectPredicate SelectStatementSelect07 = new ProjectSomeFieldsPredicate(Arrays.asList("a"));
        ExtractPredicate SelectStatementExtract07 = new KeywordExtractPredicate(Arrays.asList("g0"), "key1", null);
        Statement SelectStatementParameters07 = new SelectStatement("_sid0", SelectStatementSelect07, SelectStatementExtract07, "k", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(SelectStatement07))).selectStatement(), SelectStatementParameters07);
        
        String SelectStatement08 = "SELECT b, KEYWORDMATCH(g1, \"key2\", conjunction) FROM l";
        ProjectPredicate SelectStatementSelect08 = new ProjectSomeFieldsPredicate(Arrays.asList("b"));
        ExtractPredicate SelectStatementExtract08 = new KeywordExtractPredicate(Arrays.asList("g1"), "key2", "conjunction");
        Statement SelectStatementParameters08 = new SelectStatement("_sid0", SelectStatementSelect08, SelectStatementExtract08, "l", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(SelectStatement08))).selectStatement(), SelectStatementParameters08);
        
        String SelectStatement10 = "SELECT v, KEYWORDMATCH(u, \"keyZ\") FROM t";
        ProjectPredicate SelectStatementSelect10 = new ProjectSomeFieldsPredicate(Arrays.asList("v"));
        ExtractPredicate SelectStatementExtract10 = new KeywordExtractPredicate(Arrays.asList("u"), "keyZ", null);
        Statement SelectStatementParameters10 = new SelectStatement("_sid0", SelectStatementSelect10, SelectStatementExtract10, "t", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(SelectStatement10))).selectStatement(), SelectStatementParameters10);
        
        String SelectStatement11 = "SELECT e, KEYWORDMATCH([g4], \"key0\") FROM o";
        ProjectPredicate SelectStatementSelect11 = new ProjectSomeFieldsPredicate(Arrays.asList("e"));
        ExtractPredicate SelectStatementExtract11 = new KeywordExtractPredicate(Arrays.asList("g4"), "key0", null);
        Statement SelectStatementParameters11 = new SelectStatement("_sid0", SelectStatementSelect11, SelectStatementExtract11, "o", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(SelectStatement11))).selectStatement(), SelectStatementParameters11);
        
        String SelectStatement12 = "SELECT f, KEYWORDMATCH([g6,g7,h8,i9], \"key\") FROM p";
        ProjectPredicate SelectStatementSelect12 = new ProjectSomeFieldsPredicate(Arrays.asList("f"));
        ExtractPredicate SelectStatementExtract12 = new KeywordExtractPredicate(Arrays.asList("g6","g7","h8","i9"), "key", null);
        Statement SelectStatementParameters12 = new SelectStatement("_sid0", SelectStatementSelect12, SelectStatementExtract12, "p", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(SelectStatement12))).selectStatement(), SelectStatementParameters12);
        
        String SelectStatement13 = "SELECT h, i, j, KEYWORDMATCH([h6,h7,k8,k9], \"key5\") FROM q";
        ProjectPredicate SelectStatementSelect13 = new ProjectSomeFieldsPredicate(Arrays.asList("h","i","j"));
        ExtractPredicate SelectStatementExtract13 = new KeywordExtractPredicate(Arrays.asList("h6","h7","k8","k9"), "key5", null);
        Statement SelectStatementParameters13 = new SelectStatement("_sid0", SelectStatementSelect13, SelectStatementExtract13, "q", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(SelectStatement13))).selectStatement(), SelectStatementParameters13);
        
        String SelectStatement14 = "SELECT KEYWORDMATCH([i6,j7,l8,m9], \"key5\") FROM q";
        ExtractPredicate SelectStatementExtract14 = new KeywordExtractPredicate(Arrays.asList("i6","j7","l8","m9"), "key5", null);
        Statement SelectStatementParameters14 = new SelectStatement("_sid0", null, SelectStatementExtract14, "q", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(SelectStatement14))).selectStatement(), SelectStatementParameters14);
        
        String SelectStatement15 = "SELECT KEYWORDMATCH(g0, \"key1\") FROM k";
        ExtractPredicate SelectStatementExtract15 = new KeywordExtractPredicate(Arrays.asList("g0"), "key1", null);
        Statement SelectStatementParameters15 = new SelectStatement("_sid0", null, SelectStatementExtract15, "k", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(SelectStatement15))).selectStatement(), SelectStatementParameters15);
        
        String SelectStatement16 = "SELECT KEYWORDMATCH(g1, \"key2\", phrase) FROM l";
        ExtractPredicate SelectStatementExtract16 = new KeywordExtractPredicate(Arrays.asList("g1"), "key2", "phrase");
        Statement SelectStatementParameters16 = new SelectStatement("_sid0", null, SelectStatementExtract16, "l", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(SelectStatement16))).selectStatement(), SelectStatementParameters16);
        
        String SelectStatement19 = "SELECT KEYWORDMATCH([g4], \"key0\") FROM o";
        ExtractPredicate SelectStatementExtract19 = new KeywordExtractPredicate(Arrays.asList("g4"), "key0", null);
        Statement SelectStatementParameters19 = new SelectStatement("_sid0", null, SelectStatementExtract19, "o", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(SelectStatement19))).selectStatement(), SelectStatementParameters19);
        
        String SelectStatement20 = "SELECT KEYWORDMATCH([g6,g7,h8,i9], \"key\") FROM p";
        ExtractPredicate SelectStatementExtract20 = new KeywordExtractPredicate(Arrays.asList("g6","g7","h8","i9"), "key", null);
        Statement SelectStatementParameters20 = new SelectStatement("_sid0", null, SelectStatementExtract20, "p", null, null);
        Assert.assertEquals((new TextQLParser(string2InputStream(SelectStatement20))).selectStatement(), SelectStatementParameters20);
        
        String SelectStatement21 = "SELECT KEYWORDMATCH([h3,i2,j1,k0], \"key\\\"/\") FROM m LIMIT 4 OFFSET 25 ";
        ExtractPredicate SelectStatementExtract21 = new KeywordExtractPredicate(Arrays.asList("h3","i2","j1","k0"), "key\"/", null);
        Statement SelectStatementParameters21 = new SelectStatement("_sid0", null, SelectStatementExtract21, "m", 4, 25);
        Assert.assertEquals((new TextQLParser(string2InputStream(SelectStatement21))).selectStatement(), SelectStatementParameters21);
        
        String SelectStatement22 = "SELECT FROM a";
        assertException(()->(new TextQLParser(string2InputStream(SelectStatement22))).selectStatement(), ParseException.class);
        
        String SelectStatement23 = "SELECT FROM a OFFSET 5 LIMIT 6";
        assertException(()->(new TextQLParser(string2InputStream(SelectStatement23))).selectStatement(), ParseException.class);
        
        String SelectStatement24 = "SELECT 25 FROM a";
        assertException(()->(new TextQLParser(string2InputStream(SelectStatement24))).selectStatement(), ParseException.class);
        
        String SelectStatement25 = "SELECT [a,b] FROM a";
        assertException(()->(new TextQLParser(string2InputStream(SelectStatement25))).selectStatement(), ParseException.class);
        
        String SelectStatement26 = "SELECT *, a FROM a";
        assertException(()->(new TextQLParser(string2InputStream(SelectStatement26))).selectStatement(), ParseException.class);
        
        String SelectStatement27 = "SELECT * FROM [a,b]";
        assertException(()->(new TextQLParser(string2InputStream(SelectStatement27))).selectStatement(), ParseException.class);
        
        String SelectStatement28 = "SELECT KEYWORDMATCH(g0, \"key1\"), a FROM a";
        assertException(()->(new TextQLParser(string2InputStream(SelectStatement28))).selectStatement(), ParseException.class);
        
        String SelectStatement29 = "SELECT KEYWORDMATCH(g0, \"key1\") SELECT a FROM k";
        assertException(()->(new TextQLParser(string2InputStream(SelectStatement29))).selectStatement(), ParseException.class);
        
        String SelectStatement30 = "SELECT a";
        assertException(()->(new TextQLParser(string2InputStream(SelectStatement30))).selectStatement(), ParseException.class);
    }

    /**
     * Test the extractKeywordMatchPredicate method of the parser.
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
     * Test the createViewStatement method of the parser.
     * It should parse a create view statement and return the expected CreateViewStatement object.
     * @throws ParseException if an unexpected ParseException is thrown
     */ 
    @Test
    public void testCreateViewStatement() throws ParseException {
        String createViewStatement00 = " CREATE VIEW v0 AS SELECT * FROM a ";
        ProjectPredicate createViewStatementSelectP00 = new ProjectAllFieldsPredicate();
        Statement createViewStatementSelect00 = new SelectStatement("_sid0", createViewStatementSelectP00, null, "a", null, null);
        Statement createViewStatementParameters00 = new CreateViewStatement("v0", createViewStatementSelect00);
        Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement00))).createViewStatement(), createViewStatementParameters00);
        
        String createViewStatement01 = " CREATE VIEW v1 AS SELECT f8, fa, fc, df, ff FROM j LIMIT 1 OFFSET 8 ";
        ProjectPredicate createViewStatementSelectP01 =  new ProjectSomeFieldsPredicate(Arrays.asList("f8","fa","fc","df","ff"));
        Statement createViewStatementSelect01 = new SelectStatement("_sid0", createViewStatementSelectP01, null, "j", 1, 8);
        Statement createViewStatementParameters01 = new CreateViewStatement("v1", createViewStatementSelect01);
        Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement01))).createViewStatement(), createViewStatementParameters01);
        
        String createViewStatement02 = " CREATE VIEW v2 AS SELECT e, KEYWORDMATCH([g4,g5], \"key0\") FROM o ";
        ProjectPredicate createViewStatementSelectP02 = new ProjectSomeFieldsPredicate(Arrays.asList("e"));
        ExtractPredicate createViewStatementExtract02 = new KeywordExtractPredicate(Arrays.asList("g4","g5"), "key0", null);
        Statement createViewStatementSelect02 = new SelectStatement("_sid0", createViewStatementSelectP02, createViewStatementExtract02, "o", null, null);
        Statement createViewStatementParameters02 = new CreateViewStatement("v2", createViewStatementSelect02);
        Assert.assertEquals((new TextQLParser(string2InputStream(createViewStatement02))).createViewStatement(), createViewStatementParameters02);
        
        String createViewStatement03 = " CREATE VIEW v2 AS SELECT KEYWORDMATCH([g4,g5], \"key0\", substring) FROM o ";
        ExtractPredicate createViewStatementExtract03 = new KeywordExtractPredicate(Arrays.asList("g4","g5"), "key0", "substring");
        Statement createViewStatementSelect03 = new SelectStatement("_sid0", null, createViewStatementExtract03, "o", null, null);
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
     * Create an InputStream that contains an given String.
     * @param s the string to be available in the resulting InputStream
     * @return an InputStream containing the string s
     */ 
    private InputStream string2InputStream(String s){
        try {
            //create a piped input stream to write to and a piped output stream to return as result
            PipedOutputStream pos = new PipedOutputStream();
            PipedInputStream pis = new PipedInputStream(pos);
            //print the string to the stream
            PrintStream ppos = new PrintStream(pos);
            ppos.print(s);
            ppos.close();
            //return the generated InputStream
            return pis;
        } catch (IOException e) {
            //return null if an IOException is thrown
            return null;
        }
    }
    
}