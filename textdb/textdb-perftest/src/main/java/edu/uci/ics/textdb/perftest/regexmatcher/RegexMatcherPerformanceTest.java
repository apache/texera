package edu.uci.ics.textdb.perftest.regexmatcher;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.dataflow.common.RegexPredicate;
import edu.uci.ics.textdb.dataflow.regexmatch.RegexMatcher;
import edu.uci.ics.textdb.perftest.medline.MedlineIndexWriter;
import edu.uci.ics.textdb.perftest.utils.PerfTestUtils;
import edu.uci.ics.textdb.storage.DataStore;

/*
 * 
 * @author Zuozhi Wang
 */
public class RegexMatcherPerformanceTest {

	public static int resultNumber;
	private static String HEADER = "regex, dataset, time, Total Results\n";

	private static double regexMatchTime = 0.0;
	private static int regexResultCount = 0;
	private static String csvFileFolder = "regex/";

	/**
	 * @param regexQueries:
	 *            a list of regex queries.
	 * @param iterationNumber:
	 *            the number of times the test expected to be run
	 * @return
	 * 
	 * This function will match the queries against all indices in ./index/trigram/
	 * 
	 * Test results includes runtime, the number of results for each
	 * query, each index and each test iteration. They are written in a csv
	 * file that is named by current time and located at ./data-files/results/regex/.
	 * 
	 */
	public static void runTest(List<String> regexQueries, int iterationNumber)
			throws StorageException, DataFlowException, IOException {

		FileWriter fileWriter = null;

		// Checks whether "regex" folder exists in
		// ./data-files/results/
		if (!new File(PerfTestUtils.resultFolder, "regex").exists()) {
			File resultFile = new File(PerfTestUtils.resultFolder + csvFileFolder);
			resultFile.mkdir();
		}

		// Gets the current time for naming the cvs file
		String currentTime = PerfTestUtils.formatTime(System.currentTimeMillis());
		String csvFile = csvFileFolder + currentTime + ".csv";
		fileWriter = new FileWriter(PerfTestUtils.getResultPath(csvFile));

		// Iterates through the times of test
		// Writes results to the csv file
		File indexFiles = new File(PerfTestUtils.trigramIndexFolder);
		for (int i = 1; i <= iterationNumber; i++) {
			fileWriter.append("Cycle" + i);
			fileWriter.append("\n");
			fileWriter.append(HEADER);
			for (String regex : regexQueries) {

				for (File file : indexFiles.listFiles()) {
					if (file.getName().startsWith(".")) {
						continue;
					}
					DataStore dataStore = new DataStore(PerfTestUtils.getTrigramIndexPath(file.getName()),
							MedlineIndexWriter.SCHEMA_MEDLINE);

					matchRegex(regex, dataStore);

					fileWriter.append(regex);
					fileWriter.append(",");
					fileWriter.append(file.getName());
					fileWriter.append(",");
					fileWriter.append(String.format("%.4f", regexMatchTime));
					fileWriter.append(",");
					fileWriter.append(Integer.toString(regexResultCount));
					fileWriter.append("\n");
				}
				
			}
			fileWriter.append("\n");
			
		}
		fileWriter.flush();
		fileWriter.close();
	}

	/**
	 * @param regex
	 * @param dataStore
	 * @return
	 * 
	 * 		This function does match for a regex query
	 */
	public static void matchRegex(String regex, DataStore dataStore) throws DataFlowException, IOException {

		Attribute[] attributeList = new Attribute[] { MedlineIndexWriter.ABSTRACT_ATTR };
		// analyzer should generate grams all in lower case to build a lower
		// case index.
		Analyzer luceneAnalyzer = DataConstants.getTrigramAnalyzer();
		RegexPredicate regexPredicate = new RegexPredicate(regex, dataStore, Arrays.asList(attributeList), luceneAnalyzer);

		RegexMatcher regexMatcher = new RegexMatcher(regexPredicate, true);

		long startMatchTime = System.currentTimeMillis();
		regexMatcher.open();
		int counter = 0;
		ITuple nextTuple = null;
		while ((nextTuple = regexMatcher.getNextTuple()) != null) {
			List<Span> spanList = ((ListField<Span>) nextTuple.getField(SchemaConstants.SPAN_LIST)).getValue();
			counter += spanList.size();
		}
		regexMatcher.close();
		long endMatchTime = System.currentTimeMillis();
		double matchTime = (endMatchTime - startMatchTime) / 1000.0;
		regexMatchTime = matchTime;
		regexResultCount = counter;
	}

}