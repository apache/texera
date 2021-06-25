package edu.uci.ics.texera.perftest.dictionarymatcher;

import java.io.BufferedWriter;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.dictionarymatcher.Dictionary;
import edu.uci.ics.texera.dataflow.dictionarymatcher.DictionaryMatcherSourceOperator;
import edu.uci.ics.texera.dataflow.dictionarymatcher.DictionarySourcePredicate;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordMatchingType;
import edu.uci.ics.texera.perftest.medline.MedlineIndexWriter;
import edu.uci.ics.texera.perftest.utils.PerfTestUtils;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

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
                    KeywordMatchingType.CONJUNCTION_INDEXBASED, tableName);
            csvWriter(phraseCsv, file.getName(), queryFileName, dictionary,
                    KeywordMatchingType.PHRASE_INDEXBASED, tableName);
            csvWriter(scanCsv, file.getName(), queryFileName, dictionary,
                    KeywordMatchingType.SUBSTRING_SCANBASED, tableName);
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
     * /scripts/dashboard/build.py
     * 
     */
    public static void csvWriter(String resultFile, String recordNum, String queryFileName,
            ArrayList<String> dictionary, KeywordMatchingType opType, String tableName) throws Exception {

        PerfTestUtils.createFile(PerfTestUtils.getResultPath(resultFile), HEADER);
        BufferedWriter fileWriter = Files.newBufferedWriter
                (PerfTestUtils.getResultPath(resultFile), StandardOpenOption.APPEND);
        fileWriter.append(newLine);
        fileWriter.append(currentTime + commaDelimiter);
        fileWriter.append(recordNum + commaDelimiter);
        fileWriter.append(queryFileName + commaDelimiter);
        fileWriter.append(Integer.toString(dictionary.size()) + commaDelimiter);
        match(dictionary, opType, LuceneAnalyzerConstants.standardAnalyzerString(), tableName);
        fileWriter.append(String.format("%.4f", matchTime) + commaDelimiter);
        fileWriter.append(Integer.toString(resultCount));
        fileWriter.flush();
        fileWriter.close();
    }

    /**
     * This function does match for a dictionary
     */
    public static void match(ArrayList<String> queryList, KeywordMatchingType opType, String luceneAnalyzerStr,
            String tableName) throws Exception {
        List<String> attributeNames = Arrays.asList(MedlineIndexWriter.ABSTRACT);

        Dictionary dictionary = new Dictionary(queryList);
        DictionarySourcePredicate dictionarySourcePredicate = new DictionarySourcePredicate(dictionary, attributeNames, luceneAnalyzerStr,
                opType, tableName, SchemaConstants.SPAN_LIST);
        DictionaryMatcherSourceOperator dictionaryMatcher = new DictionaryMatcherSourceOperator(dictionarySourcePredicate);

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
