package edu.uci.ics.textdb.perftest.regexmatcher;

import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.LowerCaseFilterFactory;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.ngram.NGramTokenizerFactory;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.dataflow.common.RegexPredicate;
import edu.uci.ics.textdb.dataflow.regexmatch.RegexMatcher;
import edu.uci.ics.textdb.engine.Engine;
import edu.uci.ics.textdb.perftest.medline.MedlineIndexWriter;
import edu.uci.ics.textdb.storage.DataStore;

/*
 * This is a sample performance test 
 * using Medline data and helper functions.
 * 
 * @author Zuozhi Wang
 */
public class RegexMatcherPerformanceTest {
	

	public static void main(String[] args) throws Exception {
		samplePerformanceTest("./data-files/abstract_10K.txt", "./index/trigram/sample_regex_index/");	
	}

	public static void samplePerformanceTest(String filePath, String indexPath) 
			throws Exception {
		
		// analyzer should generate grams all in lower case to build a lower case index. 
		Analyzer luceneAnalyzer = CustomAnalyzer.builder()
				.withTokenizer(NGramTokenizerFactory.class, new String[]{"minGramSize", "3", "maxGramSize", "3"})
				.addTokenFilter(LowerCaseFilterFactory.class)
				.build();
		
		long startIndexTime = System.currentTimeMillis(); 
		
		DataStore dataStore = new DataStore(indexPath, MedlineIndexWriter.SCHEMA_MEDLINE);

		Engine writeIndexEngine = Engine.getEngine();

		writeIndexEngine.evaluate(MedlineIndexWriter.getMedlineIndexPlan(filePath, dataStore, luceneAnalyzer));

		long endIndexTime = System.currentTimeMillis();
		double indexTime = (endIndexTime - startIndexTime)/1000.0;
		System.out.printf("index time: %.4f seconds\n", indexTime);
		
		
		String regex = "\\bmedic(ine|al|ation|are|aid)?\\b";

		Attribute[] attributeList = new Attribute[]{ MedlineIndexWriter.ABSTRACT_ATTR };

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
		ITuple result = null;
		while ((result = regexMatcher.getNextTuple()) != null) {
//			System.out.println(Utils.getTupleString(result));
			List<Span> spanList = ((ListField<Span>) result.getField(SchemaConstants.SPAN_LIST)).getValue();
            counter += spanList.size();
		}
		
		long endMatchTime = System.currentTimeMillis();
		double matchTime = (endMatchTime - startMatchTime)/1000.0;
		System.out.printf("match time: %.4f seconds\n", matchTime);
		
		System.out.printf("total: %d results\n", counter);
	}
	
}
