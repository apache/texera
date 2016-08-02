package edu.uci.ics.textdb.perftest.runme;

/**
 * @author Hailey Pan 
 */

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.perftest.keywordmatcher.*;
import edu.uci.ics.textdb.perftest.nlpextractor.NlpExtractorPerformanceTest;
import edu.uci.ics.textdb.perftest.regexmatcher.RegexMatcherPerformanceTest;
import edu.uci.ics.textdb.perftest.dictionarymatcher.*;
import edu.uci.ics.textdb.perftest.fuzzytokenmatcher.*;

public class RunPerftests {

	/**
	 * Run all performance tests. 
	 * Make necessary changes for arguments, 
	 * such as query file name, threshold list, iteration number and
	 * regexQueries
	 *
	 */
	public static void main(String[] args) {
		try {
			List<Double> thresholds = Arrays.asList(0.8,0.65,0.5,0.35);
			List<String> regexQueries = Arrays.asList("mosquitos?", "v[ir]{2}[us]{2}",
					"market(ing)?", "medic(ine|al|ation|are|aid)?",
					"[A-Z][aeiou|AEIOU][A-Za-z]*"
					);
			
			KeywordMatcherPerformanceTest.runTest("sample_queries.txt", 1);
			DictionaryMatcherPerformanceTest.runTest("sample_queries.txt", 1);
			FuzzyTokenMatcherPerformanceTest.runTest("sample_queries.txt", 1, thresholds);
			RegexMatcherPerformanceTest.runTest(regexQueries, 2);
			NlpExtractorPerformanceTest.runTest(1);

		} catch (StorageException | DataFlowException | IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}