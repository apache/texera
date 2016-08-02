package edu.uci.ics.textdb.perftest.nlpextractor;

import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.dataflow.nlpextrator.NlpExtractor;
import edu.uci.ics.textdb.dataflow.nlpextrator.NlpExtractor.NlpTokenType;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.perftest.medline.MedlineIndexWriter;
import edu.uci.ics.textdb.perftest.utils.*;
import edu.uci.ics.textdb.storage.DataReaderPredicate;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.reader.DataReader;

/**
 * @author Hailey Pan
 * @author Zhenfeng Qi
 * 
 *         This is the performance test of NLP extractor matcher
 */
public class NlpExtractorPerformanceTest {

	private static String csvFileFolder = "nlp/";
	private static String HEADER = "Record #, TokenType, Time, Result #\n";
	private static String newLine = "\n";
	private static FileWriter fileWriter = null;

	/**
	 * @param iterationNumber:
	 *            the number of times the test expected to be ran
	 * @return
	 * 
	 * This function will match the queries against all indices in ./index/standard/
	 * 
	 * Test results includes runtime, the number of results for each nlp
	 * token type and each test cycle. They are written in a csv file
	 * that is named by current time and located at ./data-files/results/nlp/.
	 * 
	 */
	public static void runTest(int iterationNumber) throws Exception {

		// Checks whether "nlp" folder exists in
		// ./data-files/results/
		if (!new File(PerfTestUtils.resultFolder, "nlp").exists()) {
			File resultFile = new File(PerfTestUtils.resultFolder + csvFileFolder);
			resultFile.mkdir();
		}
		
		// Gets the current time for naming the cvs file
		String currentTime = PerfTestUtils.formatTime(System.currentTimeMillis());
		String csvFile = csvFileFolder + currentTime + ".csv";
		fileWriter = new FileWriter(PerfTestUtils.getResultPath(csvFile));

		// Iterates through the times of test
		// Writes results to the csv file
		File indexFiles = new File(PerfTestUtils.standardIndexFolder);
		for (int i = 1; i <= iterationNumber; i++) {
			fileWriter.append("Cycle" + i);
			fileWriter.append(newLine);
			fileWriter.append(HEADER);
			for (File file : indexFiles.listFiles()) {
				if (file.getName().startsWith(".")) {
					continue;
				}
				DataStore dataStore = new DataStore(PerfTestUtils.getIndexPath(file.getName()),
						MedlineIndexWriter.SCHEMA_MEDLINE);

				fileWriter.append(file.getName().replace(".txt", "") + ",");
				fileWriter.append("NE_ALL,");
				matchNLP(dataStore, NlpExtractor.NlpTokenType.NE_ALL, new StandardAnalyzer());

				fileWriter.append(file.getName().replace(".txt", "") + ",");
				fileWriter.append("Adjective,");
				matchNLP(dataStore, NlpExtractor.NlpTokenType.Adjective, new StandardAnalyzer());

				fileWriter.append(file.getName().replace(".txt", "") + ",");
				fileWriter.append("Adverb,");
				matchNLP(dataStore, NlpExtractor.NlpTokenType.Adverb, new StandardAnalyzer());

				fileWriter.append(file.getName().replace(".txt", "") + ",");
				fileWriter.append("Noun,");
				matchNLP(dataStore, NlpExtractor.NlpTokenType.Noun, new StandardAnalyzer());

				fileWriter.append(file.getName().replace(".txt", "") + ",");
				fileWriter.append("Verb,");
				matchNLP(dataStore, NlpExtractor.NlpTokenType.Verb, new StandardAnalyzer());

			}
		}
		fileWriter.flush();
		fileWriter.close();
	}

	/**
	 * @param DataStore
	 * @param tokenType
	 * @param analyzer
	 * @return
	 * 
	 * 		This function does match based on tokenType
	 */
	public static void matchNLP(IDataStore dataStore, NlpTokenType tokenType, Analyzer analyzer) throws Exception {

		List<Attribute> attributeList = Arrays.asList(MedlineIndexWriter.ABSTRACT_ATTR);

		QueryParser queryParser = new QueryParser(MedlineIndexWriter.ABSTRACT_ATTR.getFieldName(), analyzer);
		Query query = queryParser.parse(DataConstants.SCAN_QUERY);

		DataReaderPredicate dataReaderPredicate = new DataReaderPredicate(query, DataConstants.SCAN_QUERY,
				dataStore, attributeList, analyzer);
		IDataReader dataReader = new DataReader(dataReaderPredicate);
		ISourceOperator sourceOperator = new ScanBasedSourceOperator(dataReader);

		NlpExtractor nlpExtractor = new NlpExtractor(sourceOperator, attributeList, tokenType);

		long startMatchTime = System.currentTimeMillis();
		nlpExtractor.open();
		ITuple nextTuple = null;
		int counter = 0;
		while ((nextTuple = nlpExtractor.getNextTuple()) != null) {
			List<Span> spanList = ((ListField<Span>) nextTuple.getField(SchemaConstants.SPAN_LIST)).getValue();
			counter += spanList.size();

		}
		nlpExtractor.close();
		long endMatchTime = System.currentTimeMillis();
		double matchTime = (endMatchTime - startMatchTime) / 1000.0;

		fileWriter.append(String.format("%.4f", matchTime) + ",");
		fileWriter.append(counter + ",");
		fileWriter.append(newLine);

	}

}
