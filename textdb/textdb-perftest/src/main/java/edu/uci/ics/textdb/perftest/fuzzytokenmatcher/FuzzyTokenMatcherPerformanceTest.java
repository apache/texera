package edu.uci.ics.textdb.perftest.fuzzytokenmatcher;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.dataflow.common.FuzzyTokenPredicate;
import edu.uci.ics.textdb.dataflow.fuzzytokenmatcher.FuzzyTokenMatcher;
import edu.uci.ics.textdb.perftest.medline.MedlineIndexWriter;
import edu.uci.ics.textdb.perftest.utils.PerfTestUtils;
import edu.uci.ics.textdb.storage.DataStore;

/**
 * @author Qing Tang
 * 
 *         This is the performance test of fuzzy token operator.
 */

public class FuzzyTokenMatcherPerformanceTest {

	private static String HEADER = "Record #, Threshold, isSpanInfoAdded, Min, Max, Average, Std, Average Results\n";
	private static String trueHeader = "true,";
	private static String newLine = "\n";

	private static List<Double> timeResults = null;
	private static int totalResultCount = 0;
	private static boolean bool = true;
	private static String csvFileFolder = "fuzzytoken/";

	/**
	 * @param queryFileName:
	 *            this file contains line(s) of queries; the file must be placed
	 *            in ./data-files/queries/
	 * @param iterationNumber:
	 *            the number of times the test expected to be run
	 * @param thresholds:
	 *            a list of thresholds
	 * @return
	 * 
	 * 		This function will match the queries against all indices in
	 *         ./index/standard/
	 * 
	 *         Test results includes minimum runtime, maximum runtime, average
	 *         runtime, the standard deviation and the average results for each
	 *         index, each threshold and each test cycle. They are written in a
	 *         csv file that is named by current time and located at
	 *         ./data-files/results/fuzzytoken/.
	 * 
	 */
	public static void runTest(String queryFileName, int iterationNumber, List<Double> thresholds)
			throws StorageException, DataFlowException, IOException {

		// Reads queries from query file into a list
		ArrayList<String> queries = PerfTestUtils.readQueries(PerfTestUtils.getQueryPath(queryFileName));
		FileWriter fileWriter = null;
		
		// Checks whether "fuzzytoken" folder exists in
		// ./data-files/results/. If not create one.
		if (!new File(PerfTestUtils.resultFolder, "fuzzytoken").exists()) {
			File resultFile = new File(PerfTestUtils.resultFolder + csvFileFolder);
			resultFile.mkdir();
		}

		// Gets the current time for naming the cvs file
		String currentTime = PerfTestUtils.formatTime(System.currentTimeMillis());
		String csvFile = csvFileFolder + currentTime + ".csv";
		fileWriter = new FileWriter(PerfTestUtils.getResultPath(csvFile));
		
		// Iterates through the times of test
		// Writes results to the csv file
		for (int i = 1; i <= iterationNumber; i++) {
			fileWriter.append("Cycle" + i);
			fileWriter.append(newLine);
			fileWriter.append(HEADER);

			File indexFiles = new File(PerfTestUtils.standardIndexFolder);
			double avgTime = 0;
			for (double threshold : thresholds) {
				for (File file : indexFiles.listFiles()) {
					if (file.getName().startsWith(".")) {
						continue;
					}
					DataStore dataStore = new DataStore(PerfTestUtils.getIndexPath(file.getName()),
							MedlineIndexWriter.SCHEMA_MEDLINE);

					fileWriter.append(file.getName() + ",");
					fileWriter.append(Double.toString(threshold) + ",");
					fileWriter.append(trueHeader);
					resetStats();
					match(queries, threshold, new StandardAnalyzer(), dataStore, bool);
					avgTime = PerfTestUtils.calculateAverage(timeResults);
					fileWriter.append(Collections.min(timeResults) + "," + Collections.max(timeResults) + "," + avgTime
							+ "," + PerfTestUtils.calculateSTD(timeResults, avgTime) + ","
							+ totalResultCount / queries.size());
					fileWriter.append(newLine);

				}
				fileWriter.append(newLine);
			}
		}
		fileWriter.flush();
		fileWriter.close();
	}

	/**
	 * reset timeResults and totalReusltCount
	 */
	public static void resetStats() {
		timeResults = new ArrayList<Double>();
		totalResultCount = 0;
	}

	/**
	 * @param queryList: queries
	 * @param threshold
	 * @param luceneAnalyzer
	 * @param DataStore
	 * @return
	 * 
	 * This function does match for a list of queries
	 */
	public static void match(ArrayList<String> queryList, double threshold, Analyzer luceneAnalyzer,
			DataStore dataStore, boolean bool) throws DataFlowException, IOException {

		Attribute[] attributeList = new Attribute[] { MedlineIndexWriter.ABSTRACT_ATTR };

		for (String query : queryList) {
			FuzzyTokenPredicate predicate = new FuzzyTokenPredicate(query, dataStore, Arrays.asList(attributeList),
					luceneAnalyzer, threshold, true);
			FuzzyTokenMatcher fuzzyTokenMatcher = new FuzzyTokenMatcher(predicate);

			long startMatchTime = System.currentTimeMillis();
			fuzzyTokenMatcher.open();
			int counter = 0;
			ITuple nextTuple = null;
			while ((nextTuple = fuzzyTokenMatcher.getNextTuple()) != null) {
				List<Span> spanList = ((ListField<Span>) nextTuple.getField(SchemaConstants.SPAN_LIST)).getValue();
				counter += spanList.size();
			}
			long endMatchTime = System.currentTimeMillis();
			double matchTime = (endMatchTime - startMatchTime) / 1000.0;

			timeResults.add(Double.parseDouble(String.format("%.4f", matchTime)));
			totalResultCount += counter;
		}

	}

}
