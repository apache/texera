package edu.uci.ics.texera.perftest.nlpextractor;

import java.io.BufferedWriter;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.dataflow.ISourceOperator;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.nlp.entity.NlpEntityOperator;
import edu.uci.ics.texera.dataflow.nlp.entity.NlpEntityPredicate;
import edu.uci.ics.texera.dataflow.nlp.entity.NlpEntityType;
import edu.uci.ics.texera.dataflow.source.scan.ScanBasedSourceOperator;
import edu.uci.ics.texera.dataflow.source.scan.ScanSourcePredicate;
import edu.uci.ics.texera.perftest.medline.MedlineIndexWriter;
import edu.uci.ics.texera.perftest.utils.*;

/**
 * @author Hailey Pan
 * @author Zhenfeng Qi
 * 
 *         This is the performance test of NLP extractor matcher
 */
public class NlpExtractorPerformanceTest {

    private static String HEADER = "Date,Record #, Avg Time, Avg Result #, Commit #";
    private static String delimiter = ",";
    private static String newLine = "\n";
    private static int numOfNlpType = 5;
    private static String csvFile = "nlp.csv";
    private static double totalMatchingTime = 0.0;
    private static int totalResults = 0;
    
    
    /*
     * 
     * 
     * This function will match the queries against all indices in
     * ./index/standard/
     * 
     * Test results includes the average runtime of all nlp token types, the
     * average number of results. These results are recorded in
     * ./perftest-files/results/nlp.csv
     * 
     * CSV file example:
     * Date,                Record #,     Avg Time, Avg Result #, Commit #
     * 09-09-2016 00:54:40, abstract_100, 29.5494,  252.36
     * 
     * Commit number is designed for performance dashboard. It will be appended
     * to the result file only when the performance test is run by
     * /scripts/dashboard/build.py
     * 
     */
    public static void runTest() throws Exception {
    	
    	// Gets the current time  
        String currentTime = PerfTestUtils.formatTime(System.currentTimeMillis());
        
        File indexFiles = new File(PerfTestUtils.standardIndexFolder);
        
        for (File file : indexFiles.listFiles()) {
            if (file.getName().startsWith(".")) {
                continue;
            }
            String tableName = file.getName().replace(".txt", "");

            PerfTestUtils.createFile(PerfTestUtils.getResultPath(csvFile), HEADER);
            BufferedWriter fileWriter = Files.newBufferedWriter
                    (PerfTestUtils.getResultPath(csvFile), StandardOpenOption.APPEND);
            fileWriter.append(newLine);
            fileWriter.append(currentTime + delimiter);
            fileWriter.append(file.getName() + delimiter);
            matchNLP(tableName, NlpEntityType.NE_ALL);
            matchNLP(tableName, NlpEntityType.ADJECTIVE);
            matchNLP(tableName, NlpEntityType.ADVERB);
            matchNLP(tableName, NlpEntityType.NOUN);
            matchNLP(tableName, NlpEntityType.VERB);
            fileWriter.append(String.format("%.4f", totalMatchingTime / numOfNlpType));
            fileWriter.append(delimiter);
            fileWriter.append(String.format("%.2f", totalResults * 0.1 / numOfNlpType ));
            fileWriter.flush();
            fileWriter.close();
        }
    
 
    }

    /*
     * This function does match based on tokenType
     */
    public static void matchNLP(String tableName, NlpEntityType tokenType) throws Exception {

        List<String> attributeNames = Arrays.asList(MedlineIndexWriter.ABSTRACT);

        ISourceOperator sourceOperator = new ScanBasedSourceOperator(new ScanSourcePredicate(tableName));

        NlpEntityPredicate nlpEntityPredicate = new NlpEntityPredicate(tokenType, attributeNames, SchemaConstants.SPAN_LIST);
        NlpEntityOperator nlpEntityOperator = new NlpEntityOperator(nlpEntityPredicate);
        nlpEntityOperator.setInputOperator(sourceOperator);

        long startMatchTime = System.currentTimeMillis();
        nlpEntityOperator.open();
        Tuple nextTuple = null;
        int counter = 0;
        while ((nextTuple = nlpEntityOperator.getNextTuple()) != null) {
            ListField<Span> spanListField = nextTuple.getField(SchemaConstants.SPAN_LIST);
            List<Span> spanList = spanListField.getValue();
            counter += spanList.size();

        }
        nlpEntityOperator.close();
        long endMatchTime = System.currentTimeMillis();
        double matchTime = (endMatchTime - startMatchTime) / 1000.0;

        totalMatchingTime += matchTime;
        totalResults += counter;

    }

}
