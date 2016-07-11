package edu.uci.ics.textdb.perftest.regexmatcher;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.ngram.NGramTokenizerFactory;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.dataflow.common.RegexPredicate;
import edu.uci.ics.textdb.dataflow.regexmatch.RegexMatcher;
import edu.uci.ics.textdb.perftest.medline.MedlineReader;
import edu.uci.ics.textdb.storage.DataStore;

/*
 * This is a sample performance test 
 * using Medline data and helper functions.
 * 
 * @author Zuozhi Wang
 */
public class RegexMatcherPerformanceTest {
	
	public static void main(String[] args) throws StorageException, IOException, DataFlowException {
		samplePerformanceTest("./data-files/ipubmed_abs_present.json", "./index");	
	}

	public static void samplePerformanceTest(String filePath, String indexPath) 
			throws StorageException, IOException, DataFlowException {
		
		Analyzer luceneAnalyzer = CustomAnalyzer.builder()
				.withTokenizer(NGramTokenizerFactory.class, new String[]{"minGramSize", "3", "maxGramSize", "3"})
				.build();
		
		long startIndexTime = System.currentTimeMillis(); 
		
		DataStore dataStore = new DataStore(indexPath, MedlineReader.SCHEMA_MEDLINE);

//		MedlineIndexWriter.writeMedlineToIndex(filePath, dataStore, luceneAnalyzer);
		
		long endIndexTime = System.currentTimeMillis();
		double indexTime = (endIndexTime - startIndexTime)/1000.0;
		System.out.printf("index time: %.4f seconds\n", indexTime);
		
		
		String regex = "\\bmedic(ine|al|ation|are|aid)?\\b";

		Attribute[] attributeList = new Attribute[]{ MedlineReader.ABSTRACT_ATTR };

		RegexPredicate regexPredicate = new RegexPredicate(
				regex, Arrays.asList(attributeList), 
				luceneAnalyzer, dataStore);
		
		RegexMatcher regexMatcher = new RegexMatcher(regexPredicate, true);
		
		regexMatcher.setRegexEngineToJava();
//		regexMatcher.setRegexEngineToRE2J();
		System.out.println(regexMatcher.getLueneQueryString());
		System.out.println(regexMatcher.getRegexEngineString());
		
		long startLuceneQueryTime = System.currentTimeMillis();
		
		regexMatcher.open();
		
		long endLuceneQueryTime = System.currentTimeMillis();
		double luceneQueryTime = (endLuceneQueryTime - startLuceneQueryTime)/1000.0;
		System.out.printf("lucene Query time: %.4f seconds\n", luceneQueryTime);
		
		
		long startMatchTime = System.currentTimeMillis();

		int counter = 0;
		while ((regexMatcher.getNextTuple()) != null) {
			counter++;
		}
		
		long endMatchTime = System.currentTimeMillis();
		double matchTime = (endMatchTime - startMatchTime)/1000.0;
		System.out.printf("match time: %.4f seconds\n", matchTime);
		
		System.out.printf("total: %d results\n", counter);
	}
	
}
