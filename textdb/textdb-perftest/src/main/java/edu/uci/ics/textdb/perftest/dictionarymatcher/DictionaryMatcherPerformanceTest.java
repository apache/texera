package edu.uci.ics.textdb.perftest.dictionarymatcher;

import java.io.File;
import java.io.FileWriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import edu.uci.ics.textdb.api.common.Tuple;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.dataflow.common.Dictionary;
import edu.uci.ics.textdb.dataflow.common.DictionaryPredicate;
import edu.uci.ics.textdb.dataflow.dictionarymatcher.DictionaryMatcherSourceOperator;
import edu.uci.ics.textdb.perftest.medline.MedlineIndexWriter;
import edu.uci.ics.textdb.perftest.utils.PerfTestUtils;

/**
 * @author Hailey Pan
 * 
 *         This is the performance test of dictionary matcher
 */

public class DictionaryMatcherPerformanceTest {

    private static String HEADER = "Date, Record #, Dictionary, Words/Phrase Count, Time(sec), Total Results, Commit Number";

    private static String commaDelimiter = ",";
    private static String newLine = "\n";
    private static double matchTime = 0.0;
    private static int resultCount = 0;

    private static String currentTime = "";

    // result files
    private static String conjunctionCsv = "dictionary-conjunction.csv";
    private static String scanCsv = "dictionary-scan.csv";
    private static String phraseCsv = "dictionary-phrase.csv";

    /*
     * queryFileName contains line(s) of phrases/words which are used to form a
     * dictionary for matching; the file must be placed in
     * ./perftest-files/queries/.
     * 
     * This function will match the dictionary against all indices in
     * ./index/standard/
     * 
     * Test results for each operator are recorded in corresponding csv files:
     *   ./perftest-files/results/dictionary-conjunction.csv
     *   ./perftest-files/results/dictionary-phrase.csv
     *   ./perftest-files/results/dictionary-scan.csv.
     * 
     */
    public static void runTest(String queryFileName) throws Exception {

        // Reads queries from query file into a list
        ArrayList<String> dictionary = PerfTestUtils.readQueries(PerfTestUtils.getQueryPath(queryFileName));

        // Gets the current time
        currentTime = PerfTestUtils.formatTime(System.currentTimeMillis());
        File indexFiles = new File(PerfTestUtils.standardIndexFolder);

        for (File file : indexFiles.listFiles()) {
            if (file.getName().startsWith(".")) {
                continue;
            }
            String tableName = file.getName().replace(".txt", "");

            csvWriter(conjunctionCsv, file.getName(), queryFileName, dictionary,
                    DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED, tableName);
            csvWriter(phraseCsv, file.getName(), queryFileName, dictionary,
                    DataConstants.KeywordMatchingType.PHRASE_INDEXBASED, tableName);
            csvWriter(scanCsv, file.getName(), queryFileName, dictionary,
                    DataConstants.KeywordMatchingType.SUBSTRING_SCANBASED, tableName);
        }
    }

    /*
     * 
     * This function writes test results to the given result file.
     * 
     * CSV file example: Date Record # Dictionary Words/Phrase Count Time(sec)
     * Total Results, Commit Number 09-09-2016 00:50:40 abstract_100
     * sample_queries.txt 11 0.4480 24
     * 
     * Commit number is designed for performance dashboard. It will be appended
     * to the result file only when the performance test is run by
     * /textdb-scripts/dashboard/build.py
     * 
     */
    public static void csvWriter(String resultFile, String recordNum, String queryFileName,
            ArrayList<String> dictionary, KeywordMatchingType opType, String tableName) throws Exception {

        PerfTestUtils.createFile(PerfTestUtils.getResultPath(resultFile), HEADER);
        FileWriter fileWriter = new FileWriter(PerfTestUtils.getResultPath(resultFile), true);
        fileWriter.append(newLine);
        fileWriter.append(currentTime + commaDelimiter);
        fileWriter.append(recordNum + commaDelimiter);
        fileWriter.append(queryFileName + commaDelimiter);
        fileWriter.append(Integer.toString(dictionary.size()) + commaDelimiter);
        match(dictionary, opType, new StandardAnalyzer(), tableName);
        fileWriter.append(String.format("%.4f", matchTime) + commaDelimiter);
        fileWriter.append(Integer.toString(resultCount));
        fileWriter.flush();
        fileWriter.close();
    }

    /**
     * This function does match for a dictionary
     */
    public static void match(ArrayList<String> queryList, KeywordMatchingType opType, Analyzer luceneAnalyzer,
            String tableName) throws Exception {
        List<String> attributeNames = Arrays.asList(MedlineIndexWriter.ABSTRACT);

        Dictionary dictionary = new Dictionary(queryList);
        DictionaryPredicate dictionaryPredicate = new DictionaryPredicate(dictionary, attributeNames, luceneAnalyzer,
                opType);
        DictionaryMatcherSourceOperator dictionaryMatcher = new DictionaryMatcherSourceOperator(dictionaryPredicate,
                tableName);

        long startMatchTime = System.currentTimeMillis();
        dictionaryMatcher.open();
        Tuple nextTuple = null;
        int counter = 0;
        while ((nextTuple = dictionaryMatcher.getNextTuple()) != null) {
            ListField<Span> spanListField = nextTuple.getField(SchemaConstants.SPAN_LIST);
            List<Span> spanList = spanListField.getValue();
            counter += spanList.size();
        }
        dictionaryMatcher.close();
        long endMatchTime = System.currentTimeMillis();
        matchTime = (endMatchTime - startMatchTime) / 1000.0;
        resultCount = counter;
    }

}
