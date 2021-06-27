package edu.uci.ics.texera.perftest.regexmatcher;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.regexmatcher.RegexMatcherSourceOperator;
import edu.uci.ics.texera.dataflow.regexmatcher.RegexSourcePredicate;

import edu.uci.ics.texera.perftest.medline.MedlineIndexWriter;
import edu.uci.ics.texera.perftest.utils.PerfTestUtils;

/*
 * 
 * @author Zuozhi Wang
 * @author Hailey Pan
 * 
 */
public class RegexMatcherPerformanceTest {

    public static int resultNumber;
    private static String HEADER = "Date, dataset, Average Time, Average Results, Commit Number";
    private static String delimiter = ",";
    private static double totalMatchingTime = 0.0;
    private static int totalRegexResultCount = 0;
    private static String csvFile  = "regex.csv";

    /*
     * regexQueries is a list of regex queries.
     * 
     * This function will match the queries against all indices in
     * ./index/trigram/
     * 
     * Test results includes the average runtime of all queries, the average
     * number of results. These results are written to
     * ./perftest-files/results/regex.csv.
     * 
     * CSV file example: 
     * Date,                dataset,      Average Time, Average Results, Commit Number
     * 09-09-2016 00:54:29, abstract_100, 0.2798,       69.80
     * 
     * Commit number is designed for performance dashboard. It will be appended
     * to the result file only when the performance test is run by
     * /scripts/dashboard/build.py
     * 
     */
    public static void runTest(List<String> regexQueries)
            throws TexeraException, IOException {         
        // Gets the current time for naming the cvs file
        String currentTime = PerfTestUtils.formatTime(System.currentTimeMillis());

        // Writes results to the csv file
        File indexFiles = new File(PerfTestUtils.trigramIndexFolder);
   
        for (File file : indexFiles.listFiles()) {
            if (file.getName().startsWith(".")) {
                continue;
            }
            System.out.println(file.getName());

            PerfTestUtils.createFile(PerfTestUtils.getResultPath(csvFile), HEADER);
            BufferedWriter fileWriter = Files.newBufferedWriter
                    (PerfTestUtils.getResultPath(csvFile), StandardOpenOption.APPEND);
            matchRegex(regexQueries, file.getName());
            fileWriter.append("\n");
            fileWriter.append(currentTime + delimiter);
            fileWriter.append(file.getName() + delimiter);
            fileWriter.append(String.format("%.4f", totalMatchingTime / regexQueries.size()));
            fileWriter.append(delimiter);
            fileWriter.append(String.format("%.2f", totalRegexResultCount * 1.0 / regexQueries.size()));
            fileWriter.flush();
            fileWriter.close();
        }
   
    }

    /*
     *         This function does match for a list of regex queries
     */
    public static void matchRegex(List<String> regexes, String tableName) throws TexeraException, IOException {

        List<String> attributeNames = Arrays.asList(MedlineIndexWriter.ABSTRACT);
        
        for(String regex: regexes){
	        // analyzer should generate grams all in lower case to build a lower
	        // case index.
	        RegexSourcePredicate predicate = new RegexSourcePredicate(regex, attributeNames, tableName, SchemaConstants.SPAN_LIST);
	        RegexMatcherSourceOperator regexSource = new RegexMatcherSourceOperator(predicate);
	
	        long startMatchTime = System.currentTimeMillis();
	        regexSource.open();
	        int counter = 0;
	        Tuple nextTuple = null;
	        while ((nextTuple = regexSource.getNextTuple()) != null) {
	            ListField<Span> spanListField = nextTuple.getField(SchemaConstants.SPAN_LIST);
	            List<Span> spanList = spanListField.getValue();
	            counter += spanList.size();
	        }
	        regexSource.close();
	        long endMatchTime = System.currentTimeMillis();
	        double matchTime = (endMatchTime - startMatchTime) / 1000.0;
	        totalMatchingTime += matchTime;
	        totalRegexResultCount += counter;
        }
    }

}