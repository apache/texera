
package edu.uci.ics.texera.perftest.keywordmatcher;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordMatcherSourceOperator;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordMatchingType;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordSourcePredicate;
import edu.uci.ics.texera.perftest.medline.MedlineIndexWriter;
import edu.uci.ics.texera.perftest.utils.PerfTestUtils;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

/**
 * @author Hailey Pan
 * 
 * This is the performance test of KeywordMatcher
 */

public class KeywordMatcherPerformanceTest {

    private static String HEADER = "Date,Record #,Min Time, Max Time, Average Time, Std, Average Results, Commit Number";
    private static String delimiter = ",";
    private static String newLine = "\n";

    private static List<Double> timeResults = null;
    private static int totalResultCount = 0;
    private static String currentTime = "";

    // result files
    private static String conjunctionCsv = "keyword-conjunction.csv";
    private static String scanCsv = "keyword-scan.csv";
    private static String phraseCsv = "keyword-phrase.csv";

    /*
     * queryFileName contains line(s) of queries; the file must be placed in
     * ./perftest-files/queries/
     * 
     * This function will match the queries against all indices in
     * ./index/standard/
     * 
     * Test results for each operator include minimum runtime, maximum runtime,
     * average runtime, the standard deviation and the average number of results
     * are recorded in corresponding csv files:
     * ./perftest-files/results/keyword-conjunction.csv
     * ./perftest-files/results/keyword-phrase.csv ./data-files/results/keyword-scan.csv
     * 
     * 
     */
    public static void runTest(String queryFileName) throws Exception {

        // Reads queries from query file into a list
        ArrayList<String> queries = PerfTestUtils.readQueries(PerfTestUtils.getQueryPath(queryFileName));

        // Gets the current time
        currentTime = PerfTestUtils.formatTime(System.currentTimeMillis());

        File indexFiles = new File(PerfTestUtils.standardIndexFolder);

        // Does match against each index in ./index/
        for (File file : indexFiles.listFiles()) {
            if (file.getName().startsWith(".")) {
                continue;
            }
            String tableName = file.getName().replace(".txt", "");
                    
            csvWriter(conjunctionCsv, file.getName(), queries, KeywordMatchingType.CONJUNCTION_INDEXBASED, tableName);
            csvWriter(phraseCsv, file.getName(), queries, KeywordMatchingType.PHRASE_INDEXBASED, tableName);
            csvWriter(scanCsv, file.getName(), queries, KeywordMatchingType.SUBSTRING_SCANBASED, tableName);

        }

    }

    /*
     * This function writes test results to the given csv file.
     * 
     * Example
     * 
     * Date,                Record #,     Min Time, Max Time, Average Time, Std,    Average Results, Commit Number
     * 09-09-2016 00:54:18, abstract_100, 0.017,    1.373,    0.2371,       0.4464, 2.18
     * 
     * Commit number is designed for performance dashboard. It will be appended
     * to the result file only when the performance test is run by
     * /scripts/dashboard/build.py
     * 
     */
    public static void csvWriter(String resultFile, String recordNum, ArrayList<String> queries,
            KeywordMatchingType opType, String tableName) throws Exception {
        double avgTime = 0.0;
        PerfTestUtils.createFile(PerfTestUtils.getResultPath(resultFile), HEADER);
        BufferedWriter fileWriter = Files.newBufferedWriter(
                PerfTestUtils.getResultPath(resultFile), StandardOpenOption.APPEND);
        fileWriter.append(newLine);
        fileWriter.append(currentTime + delimiter);
        fileWriter.append(recordNum + delimiter);
        resetStats();
        match(queries, opType, LuceneAnalyzerConstants.standardAnalyzerString(), tableName);
        avgTime = PerfTestUtils.calculateAverage(timeResults);
        fileWriter.append(Collections.min(timeResults) + delimiter + Collections.max(timeResults) + delimiter + avgTime
                + delimiter + PerfTestUtils.calculateSTD(timeResults, avgTime) + delimiter
                + String.format("%.2f", totalResultCount * 1.0 / queries.size()));
        fileWriter.flush();
        fileWriter.close();
    }

    /**
     * reset timeResults and totalResultCount
     */
    public static void resetStats() {
        timeResults = new ArrayList<Double>();
        totalResultCount = 0;
    }

    /*
     * This function does match for a list of queries
     */
    public static void match(ArrayList<String> queryList, KeywordMatchingType opType, String luceneAnalyzerStr,
            String tableName) throws TexeraException, IOException {

        String[] attributeNames = new String[] { MedlineIndexWriter.ABSTRACT };

        for (String query : queryList) {
            KeywordSourcePredicate predicate = new KeywordSourcePredicate(
                    query,
                    Arrays.asList(attributeNames),
                    luceneAnalyzerStr, 
                    opType, 
                    tableName,
                    SchemaConstants.SPAN_LIST);

            KeywordMatcherSourceOperator keywordSource = new KeywordMatcherSourceOperator(predicate);

            long startMatchTime = System.currentTimeMillis();
            keywordSource.open();
            int counter = 0;
            Tuple nextTuple = null;
            while ((nextTuple = keywordSource.getNextTuple()) != null) {
                ListField<Span> spanListField = nextTuple.getField(SchemaConstants.SPAN_LIST);
                List<Span> spanList = spanListField.getValue();
                counter += spanList.size();
            }
            keywordSource.close();
            long endMatchTime = System.currentTimeMillis();
            double matchTime = (endMatchTime - startMatchTime) / 1000.0;

            timeResults.add(Double.parseDouble(String.format("%.4f", matchTime)));
            totalResultCount += counter;

        }
    }

}
