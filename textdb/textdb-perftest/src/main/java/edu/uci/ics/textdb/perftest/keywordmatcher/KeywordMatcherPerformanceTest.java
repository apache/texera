
package edu.uci.ics.textdb.perftest.keywordmatcher;

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
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcher;
import edu.uci.ics.textdb.perftest.medline.MedlineIndexWriter;
import edu.uci.ics.textdb.perftest.utils.PerfTestUtils;
import edu.uci.ics.textdb.storage.DataStore;

/**
 * @author Hailey Pan
 * 
 *         This is the performance test of KeywordMatcher
 */

public class KeywordMatcherPerformanceTest {

	private static String HEADER = "Record #,Operator , Min Time, Max Time, Average Time, Std, Average Results\n";
	private static String basicHeader = "Conjunctive,";
	private static String phraseHeader = "Phrase,";
	private static String newLine = "\n";

	private static List<Double> timeResults = null;
	private static int totalResultCount = 0;
	private static String csvFileFolder = "keyword/";

	/**
	 * @param queryFileName:
	 *            this file contains line(s) of queries; the file must be placed
	 *            in ./data-files/queries/
	 * @param iterationNumber:
	 *            the number of times the test expected to be run
	 * @return
	 * 
	 * 		This function will match the queries against all indices in
	 *         ./index/standard/
	 * 
	 *         Test results include minimum runtime, maximum runtime, average
	 *         runtime, the standard deviation and the average results for each
	 *         index, each operator and each test cycle. They are recorded in a
	 *         csv file that is named by current time and located at
	 *         ./data-files/results/keyword/.
	 * 
	 */
	public static void runTest(String queryFileName, int iterationNumber)
			throws StorageException, DataFlowException, IOException {

		// Reads queries from query file into a list
		ArrayList<String> queries = PerfTestUtils.readQueries(PerfTestUtils.getQueryPath(queryFileName));

		// Checks whether "keyword" folder exists in
		// ./data-files/results/
		if (!new File(PerfTestUtils.resultFolder, "keyword").exists()) {
			File resultFile = new File(PerfTestUtils.resultFolder + csvFileFolder);
			resultFile.mkdir();
		}

		// Gets the current time for naming the cvs file
		String currentTime = PerfTestUtils.formatTime(System.currentTimeMillis());
		String csvFile = csvFileFolder + currentTime + ".csv";
		FileWriter fileWriter = new FileWriter(PerfTestUtils.getResultPath(csvFile));

		// Iterates through the times of test
		// Writes results to the csv file
		File indexFiles = new File(PerfTestUtils.standardIndexFolder);
		double avgTime = 0;
		for (int i = 1; i <= iterationNumber; i++) {
			fileWriter.append("Cycle" + i);
			fileWriter.append(newLine);
			fileWriter.append(HEADER);

			// Does match against each index in ./index/
			for (File file : indexFiles.listFiles()) {
				if (file.getName().startsWith(".")) {
					continue;
				}
				DataStore dataStore = new DataStore(PerfTestUtils.getIndexPath(file.getName()),
						MedlineIndexWriter.SCHEMA_MEDLINE);

				fileWriter.append(file.getName() + ",");
				fileWriter.append(basicHeader);
				resetStats();
				match(queries, DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED, new StandardAnalyzer(),
						dataStore);
				avgTime = PerfTestUtils.calculateAverage(timeResults);
				fileWriter.append(Collections.min(timeResults) + "," + Collections.max(timeResults) + "," + avgTime
						+ "," + PerfTestUtils.calculateSTD(timeResults, avgTime) + ","
						+ String.format("%.2f", totalResultCount * 1.0 / queries.size()));
				fileWriter.append(newLine);

				fileWriter.append(file.getName() + ",");
				fileWriter.append(phraseHeader);
				resetStats();
				match(queries, DataConstants.KeywordMatchingType.PHRASE_INDEXBASED, new StandardAnalyzer(), dataStore);
				avgTime = PerfTestUtils.calculateAverage(timeResults);
				fileWriter.append(Collections.min(timeResults) + "," + Collections.max(timeResults) + "," + avgTime
						+ "," + PerfTestUtils.calculateSTD(timeResults, avgTime) + ","
						+ String.format("%.2f", totalResultCount * 1.0 / queries.size()));
				fileWriter.append(newLine);

			}
			fileWriter.append(newLine);
		}

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

	/**
	 * @param queryList:
	 *            queries
	 * @param Optype:
	 *            operator type
	 * @param luceneAnalyzer
	 * @param DataStore
	 * @return
	 * 
	 * 		This function does match for a list of queries
	 */
	public static void match(ArrayList<String> queryList, KeywordMatchingType opType, Analyzer luceneAnalyzer,
			DataStore dataStore) throws DataFlowException, IOException {

		Attribute[] attributeList = new Attribute[] { MedlineIndexWriter.ABSTRACT_ATTR };

		for (String query : queryList) {
			IPredicate predicate = new KeywordPredicate(query, Arrays.asList(attributeList), luceneAnalyzer, opType);
			KeywordMatcher keywordMatcher = new KeywordMatcher(predicate, dataStore);

			long startMatchTime = System.currentTimeMillis();
			keywordMatcher.open();
			int counter = 0;
			ITuple nextTuple = null;
			while ((nextTuple = keywordMatcher.getNextTuple()) != null) {
				List<Span> spanList = ((ListField<Span>) nextTuple.getField(SchemaConstants.SPAN_LIST)).getValue();
				counter += spanList.size();
			}
			keywordMatcher.close();
			long endMatchTime = System.currentTimeMillis();
			double matchTime = (endMatchTime - startMatchTime) / 1000.0;

			timeResults.add(Double.parseDouble(String.format("%.4f", matchTime)));
			totalResultCount += counter;

		}
	}

}
