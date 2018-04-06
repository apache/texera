package edu.uci.ics.texera.dataflow.regexsplit;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.constants.test.TestConstantsChinese;
import edu.uci.ics.texera.api.constants.test.TestConstantsRegexSplit;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.TestUtils;
import edu.uci.ics.texera.dataflow.source.scan.ScanBasedSourceOperator;
import edu.uci.ics.texera.dataflow.source.scan.ScanSourcePredicate;
import edu.uci.ics.texera.storage.DataWriter;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

/**
 * @author Zuozhi Wang
 * @author Qinhua Huang
 *
 */
public class RegexSplitOperatorTest {
    
    public static final String REGEX_TABLE = "regex_split_test";
    public static final String CHINESE_TABLE = "regex_split_test_chinese";
    public static final String RESULT_ATTR = "RESULT";
    
    @BeforeClass
    public static void setUp() throws TexeraException {
        RelationManager relationManager = RelationManager.getInstance();
        
        RelationManager.getInstance().deleteTable(REGEX_TABLE);
        relationManager = RelationManager.getInstance();
        relationManager.createTable(REGEX_TABLE, TestUtils.getDefaultTestIndex().resolve(REGEX_TABLE), 
                TestConstantsRegexSplit.SCHEMA_PEOPLE, LuceneAnalyzerConstants.standardAnalyzerString());
        DataWriter regexDataWriter = relationManager.getTableDataWriter(REGEX_TABLE);
        regexDataWriter.open();
        for (Tuple tuple : TestConstantsRegexSplit.constructSamplePeopleTuples()) {
            regexDataWriter.insertTuple(tuple);
        }
        regexDataWriter.close();
        
        // create the people table and write tuples in Chinese
        relationManager.createTable(CHINESE_TABLE, TestUtils.getDefaultTestIndex().resolve(CHINESE_TABLE), 
                TestConstantsChinese.SCHEMA_PEOPLE, LuceneAnalyzerConstants.chineseAnalyzerString());
        DataWriter chineseDataWriter = relationManager.getTableDataWriter(CHINESE_TABLE);
        chineseDataWriter.open();
        for (Tuple tuple : TestConstantsChinese.getSamplePeopleTuples()) {
            chineseDataWriter.insertTuple(tuple);
        }
        chineseDataWriter.close();
    }
    
    @AfterClass
    public static void cleanUp() throws TexeraException {
        RelationManager.getInstance().deleteTable(CHINESE_TABLE);
        RelationManager.getInstance().deleteTable(REGEX_TABLE);
    }
    
    public static List<Tuple> computeRegexSplitResultsOneToMany( String tableName, String splitAttrName,
            String splitRegex, RegexSplitPredicate.SplitType splitType ) throws TexeraException {
        
        ScanBasedSourceOperator scanSource = new ScanBasedSourceOperator(new ScanSourcePredicate(tableName));
        RegexSplitOperator regexSplit = new RegexSplitOperator(
                new RegexSplitPredicate(splitRegex, splitAttrName, RegexOutputType.ONE_TO_MANY, splitType, RESULT_ATTR));
        regexSplit.setInputOperator(scanSource);
        
        List<Tuple> results = new ArrayList<>();
        
        regexSplit.open();
        Tuple tuple;
        while((tuple = regexSplit.getNextTuple()) != null) {
            results.add(tuple);
        }
        regexSplit.close();
        return results;
    }
    
    public static List<Tuple> computeRegexSplitResultsOnetoOne( String tableName, String splitAttrName,
            String splitRegex, RegexSplitPredicate.SplitType splitType ) throws TexeraException {
        
        ScanBasedSourceOperator scanSource = new ScanBasedSourceOperator(new ScanSourcePredicate(tableName));
        RegexSplitOperator regexSplit = new RegexSplitOperator(
                new RegexSplitPredicate(splitRegex, splitAttrName, RegexOutputType.ONE_TO_ONE, splitType, RESULT_ATTR));
        regexSplit.setInputOperator(scanSource);
        
        List<Tuple> results = new ArrayList<>();
        
        regexSplit.open();
        Tuple tuple;
        while((tuple = regexSplit.getNextTuple()) != null) {
            results.add(tuple);
        }
        regexSplit.close();
        return results;
    }
    
    @SuppressWarnings("unchecked")
    public static List<String> getTupleSpanListString(Tuple tuple, String attribute) {
        List<String> listStr = new ArrayList<>();
        for (Span span : (List<Span>) tuple.getField(attribute).getValue()) {
            listStr.add(span.getValue());
        }
        return listStr;
    }
    
    /*
     *  When splitting a field that is not TextField nor StringField, the 
     *  operator will throw an exception.
     */
    @Test(expected = DataflowException.class)
    public void test1() throws TexeraException {
        String splitRegex = "19";
        String splitAttrName = TestConstantsChinese.DATE_OF_BIRTH;
        computeRegexSplitResultsOneToMany(CHINESE_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.GROUP_RIGHT);
    }
    
    /*
     * If no pattern is found in the field, the operator will not split the field.
     * Instead, it will return the original field as the result.
     */
    @Test
    public void test2() throws TexeraException {
        String splitRegex = "hi";
        String splitAttrName = TestConstantsRegexSplit.DESCRIPTION;
        
        List<String> splitResult = new ArrayList<>();
        splitResult.add("banana");
        splitResult.add("mississippi");
        
        List<Tuple> results = computeRegexSplitResultsOneToMany(REGEX_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.STANDALONE);
        
        List<String> splitStrings = results.stream()
                .map(tuple -> tuple.getField(RESULT_ATTR).getValue().toString())
                .collect(Collectors.toList());
        Assert.assertEquals(splitResult, splitStrings);
    }
    /*
     * In the STANDALONE mode, when a string contains multiple overlapping matching patterns, 
     * the operator will only return the longest one as the result.
     */
    @Test
    public void test3() throws TexeraException {
        String splitRegex = "b.*a";
        String splitAttrName = TestConstantsRegexSplit.DESCRIPTION;
        
        List<String> splitResult = new ArrayList<>();
        splitResult.add("banana");
        splitResult.add("mississippi");
        
        List<Tuple> results = computeRegexSplitResultsOneToMany(REGEX_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.STANDALONE);
        
        List<String> splitStrings = results.stream()
                .map(tuple -> tuple.getField(RESULT_ATTR).getValue().toString())
                .collect(Collectors.toList());
        Assert.assertEquals(splitResult, splitStrings);
    }
    
    /*
     * In the GROUP_LEFT mode, the operator will group the matching patterns to the left.
     */
    @Test
    public void test4() throws TexeraException {
        String splitRegex = "a.*n";
        String splitAttrName = TestConstantsRegexSplit.DESCRIPTION;
        
        List<String> splitResult = new ArrayList<>();
        splitResult.add("banan");
        splitResult.add("a");
        splitResult.add("mississippi");
        
        List<Tuple> results = computeRegexSplitResultsOneToMany(REGEX_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.GROUP_LEFT);
        
        List<String> splitStrings = results.stream()
                .map(tuple -> tuple.getField(RESULT_ATTR).getValue().toString())
                .collect(Collectors.toList());
        Assert.assertEquals(splitResult, splitStrings);
    }
    
    /*
     * In the GROUP_RIGHT mode, the operator will group the matching patterns to the right.
     */
    @Test
    public void test5() throws TexeraException {
        String splitRegex = "a.*n";
        String splitAttrName = TestConstantsRegexSplit.DESCRIPTION;
        
        List<String> splitResult = new ArrayList<>();
        splitResult.add("b");
        splitResult.add("anana");
        splitResult.add("mississippi");
        
        List<Tuple> results = computeRegexSplitResultsOneToMany(REGEX_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.GROUP_RIGHT);
        
        List<String> splitStrings = results.stream()
                .map(tuple -> tuple.getField(RESULT_ATTR).getValue().toString())
                .collect(Collectors.toList());
        Assert.assertEquals(splitResult, splitStrings);
    }
    
    /*
     * The word "banana" matches "ana" twice, but since the two matches overlap, the
     * operator will only return the first match. It's a behavior of the Pattern.matcher(). 
     */
    @Test
    public void test6() throws TexeraException {
        String splitRegex = "ana";
        String splitAttrName = TestConstantsRegexSplit.DESCRIPTION;
        
        List<String> splitResult = new ArrayList<>();
        splitResult.add("b");
        splitResult.add("ana");
        splitResult.add("na");
        splitResult.add("mississippi");
        
        List<Tuple> results = computeRegexSplitResultsOneToMany(REGEX_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.STANDALONE);
        
        List<String> splitStrings = results.stream()
                .map(tuple -> tuple.getField(RESULT_ATTR).getValue().toString())
                .collect(Collectors.toList());
        Assert.assertEquals(splitResult, splitStrings);
    }
    
    /*
     * Test in OneToOne mode. 
     */
    @Test
    public void test9() throws TexeraException {
        String splitRegex = "ana";
        String splitAttrName = TestConstantsRegexSplit.DESCRIPTION;
        
        List<String> splitResult1 = new ArrayList<>();
        splitResult1.add("b");
        splitResult1.add("ana");
        splitResult1.add("na");
        List<String> splitResult2 = new ArrayList<>();
        splitResult2.add("mississippi");
        
        List<List<String>> splitResults = new ArrayList<>();
        splitResults.add(splitResult1);
        splitResults.add(splitResult2);
        
        List<Tuple> results = computeRegexSplitResultsOnetoOne(REGEX_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.STANDALONE);
        
        int i = 0;
        for (Tuple tuple : results) {
            Assert.assertEquals(getTupleSpanListString(tuple, RESULT_ATTR), splitResults.get(i) );
            i++;
        }
    }
    
    /*
     * Chinese test: STANDALONE, string field.
     */
    @Test
    public void testChinese1() throws TexeraException {
        String splitRegex = "克";
        String splitAttrName = TestConstantsChinese.LAST_NAME;
        
        List<String> splitResult = new ArrayList<>();
        splitResult.add("长孙");
        splitResult.add("洛");
        splitResult.add("克");
        splitResult.add("贝尔");
        splitResult.add("建筑");
        
        List<Tuple> results = computeRegexSplitResultsOneToMany(CHINESE_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.STANDALONE);
        
        List<String> splitStrings = results.stream()
                .map(tuple -> tuple.getField(RESULT_ATTR).getValue().toString())
                .collect(Collectors.toList());

        Assert.assertEquals(splitResult, splitStrings);
    }
    
    /*
     * Chinese test: GROUP_RIGHT, text field.
     */
    @Test
    public void testChinese2() throws TexeraException {
        String splitRegex = "学";
        String splitAttrName = TestConstantsChinese.DESCRIPTION;
        
        List<String> splitResult = new ArrayList<>();
        splitResult.add("北京大");
        splitResult.add("学电气工程");
        splitResult.add("学院");
        splitResult.add("北京大");
        splitResult.add("学计算机");
        splitResult.add("学院");
        splitResult.add("伟大的建筑是历史的坐标，具有传承的价值。");
        
        List<Tuple> results = computeRegexSplitResultsOneToMany(CHINESE_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.GROUP_RIGHT);
        
        List<String> splitStrings = results.stream()
                .map(tuple -> tuple.getField(RESULT_ATTR).getValue().toString())
                .collect(Collectors.toList());

        Assert.assertEquals(splitResult, splitStrings);
    }
    
    /*
     * Chinese test: GROUP_LEFT, text field.
     */
    @Test
    public void testChinese3() throws TexeraException {
        String splitRegex = "学";
        String splitAttrName = TestConstantsChinese.DESCRIPTION;
        
        List<String> splitResult = new ArrayList<>();
        splitResult.add("北京大学");
        splitResult.add("电气工程学");
        splitResult.add("院");
        splitResult.add("北京大学");
        splitResult.add("计算机学");
        splitResult.add("院");
        splitResult.add("伟大的建筑是历史的坐标，具有传承的价值。");
        
        List<Tuple> results = computeRegexSplitResultsOneToMany(CHINESE_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.GROUP_LEFT);
        
        List<String> splitStrings = results.stream()
                .map(tuple -> tuple.getField(RESULT_ATTR).getValue().toString())
                .collect(Collectors.toList());

        Assert.assertEquals(splitResult, splitStrings);
    }
    
    /*
     * Chinese test: STANDALONE, text field.
     */
    @Test
    public void testChinese4() throws TexeraException {
        String splitRegex = "学";
        String splitAttrName = TestConstantsChinese.DESCRIPTION;
        
        List<String> splitResult = new ArrayList<>();
        splitResult.add("北京大");
        splitResult.add("学");
        splitResult.add("电气工程");
        splitResult.add("学");
        splitResult.add("院");
        splitResult.add("北京大");
        splitResult.add("学");
        splitResult.add("计算机");
        splitResult.add("学");
        splitResult.add("院");
        splitResult.add("伟大的建筑是历史的坐标，具有传承的价值。");
        
        List<Tuple> results = computeRegexSplitResultsOneToMany(CHINESE_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.STANDALONE);
        
        List<String> splitStrings = results.stream()
                .map(tuple -> tuple.getField(RESULT_ATTR).getValue().toString())
                .collect(Collectors.toList());

        Assert.assertEquals(splitResult, splitStrings);
    }
    
    /*
     * Chinese test: STANDALONE, text field, matching the whole field.
     */
    @Test
    public void testChinese5() throws TexeraException {
        String splitRegex = "北京大学电气工程学院";
        String splitAttrName = TestConstantsChinese.DESCRIPTION;
        
        List<String> splitResult = new ArrayList<>();
        splitResult.add("北京大学电气工程学院");
        splitResult.add("北京大学计算机学院");
        splitResult.add("伟大的建筑是历史的坐标，具有传承的价值。");
        
        List<Tuple> results = computeRegexSplitResultsOneToMany(CHINESE_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.STANDALONE);
        
        List<String> splitStrings = results.stream()
                .map(tuple -> tuple.getField(TestConstantsChinese.DESCRIPTION).getValue().toString())
                .collect(Collectors.toList());

        Assert.assertEquals(splitResult, splitStrings);
    }

    /*
     * Chinese test: STANDALONE, text field, no matching.
     */
    @Test
    public void testChinese6() throws TexeraException {
        String splitRegex = "美利坚合众国";
        String splitAttrName = TestConstantsChinese.DESCRIPTION;
        
        List<String> splitResult = new ArrayList<>();
        splitResult.add("北京大学电气工程学院");
        splitResult.add("北京大学计算机学院");
        splitResult.add("伟大的建筑是历史的坐标，具有传承的价值。");
        
        List<Tuple> results = computeRegexSplitResultsOneToMany(CHINESE_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.STANDALONE);
        
        List<String> splitStrings = results.stream()
                .map(tuple -> tuple.getField(TestConstantsChinese.DESCRIPTION).getValue().toString())
                .collect(Collectors.toList());

        Assert.assertEquals(splitResult, splitStrings);
    }
    
    /*
     * ID test: To test if each new tuple has a new ID.
     */
    @Test
    public void test7() throws TexeraException {
        String splitRegex = "ana";
        String splitAttrName = TestConstantsRegexSplit.DESCRIPTION;
        
        List<Tuple> results = computeRegexSplitResultsOneToMany(REGEX_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.STANDALONE);
        
        for (Tuple tuple : results) {
            tuple.getField(SchemaConstants._ID);
        }
    }
    
    /*
     * ID test: To test if each newly-split tuple's ID has conflict with the old tuple.
     */
    @Test
    public void test8() throws TexeraException {
        String splitRegex = "ana";
        String splitAttrName = TestConstantsRegexSplit.DESCRIPTION;
        
        List<Tuple> results = computeRegexSplitResultsOneToMany(REGEX_TABLE, splitAttrName, splitRegex, 
                RegexSplitPredicate.SplitType.STANDALONE);
        
        ScanBasedSourceOperator scanSource = new ScanBasedSourceOperator(new ScanSourcePredicate(REGEX_TABLE));
        
        Tuple tupleTable;
        scanSource.open();
        while ((tupleTable = scanSource.getNextTuple()) != null) {
            for (Tuple tuple : results) {
                Assert.assertFalse(tuple.getField(SchemaConstants._ID).equals(tupleTable.getField(SchemaConstants._ID)));
            }
        }
        scanSource.close();
    }
}
