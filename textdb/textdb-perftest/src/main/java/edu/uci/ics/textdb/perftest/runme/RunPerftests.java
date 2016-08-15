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
import edu.uci.ics.textdb.perftest.utils.PerfTestUtils;
import edu.uci.ics.textdb.perftest.dictionarymatcher.*;
import edu.uci.ics.textdb.perftest.fuzzytokenmatcher.*;

//testing testing testing testing


public class RunPerftests {

	/**
	 *  Run all performance tests. 
	 *  file folder path (where data set stored)
	 *  Passed in below arguments:
	 * 	result folder path (where performance test results stored)
	 * 	standard index folder path (where standard index stored)
	 * 	trigram index folder path (where trigram index stored)
	 * 	queries folder path (where query files stored)
	 * 
	 * If above arguments are not passed in, default paths will be used (refer to PerfTestUtils.java)
	 * If some of the arguments are not applicable, define them as empty string. 
	 * 
	 * Make necessary changes for arguments, 
	 * such as query file name, threshold list, iteration number and
	 * regexQueries
	 *
	 */
	public static void main(String[] args) {
		if(args.length!=0){
			PerfTestUtils.setResultFolder(args[0]);
			PerfTestUtils.setStandardIndexFolder(args[1]);
			PerfTestUtils.setTrigramIndexFolder(args[2]);
			PerfTestUtils.setQueryFolder(args[3]);
		}
		
		try {
			List<Double> thresholds = Arrays.asList(0.8,0.65,0.5,0.35);
			List<String> regexQueries = Arrays.asList("mosquitos?", "v[ir]{2}[us]{2}",
					"market(ing)?", "medic(ine|al|ation|are|aid)?",
					"[A-Z][aeiou|AEIOU][A-Za-z]*"
					);
			
			 
			RegexMatcherPerformanceTest.runTest(regexQueries, 2);
			NlpExtractorPerformanceTest.runTest(1);
			
			
			
			
		} catch (StorageException | DataFlowException | IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
			
		}
	}
}