package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.ngram.NGramTokenizerFactory;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.dataflow.common.RegexPredicate;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.writer.DataWriter;


/**
 * @author zuozhi
 * @author shuying
 * Helper class to quickly create unit test
 */
public class RegexMatcherTestHelper {

	RegexMatcher regexMatcher;
	IDataWriter dataWriter;
	IDataStore dataStore;
	
	List<ITuple> results;
    Analyzer luceneAnalyzer;
	
	public RegexMatcherTestHelper(Schema schema, List<ITuple> data) throws Exception {
		dataStore = new DataStore(DataConstants.INDEX_DIR, schema);
		luceneAnalyzer = CustomAnalyzer.builder()
				.withTokenizer(NGramTokenizerFactory.class, new String[]{"minGramSize", "3", "maxGramSize", "3"})
				.build();
        dataWriter = new DataWriter(dataStore, luceneAnalyzer);
		dataWriter.clearData();
		dataWriter.writeData(data);	
		results = new ArrayList<ITuple>();
	}
	
	public List<ITuple> getResults() {
		return results;
	}
	
	public Schema getSpanSchema() {
		return regexMatcher.getSpanSchema();
	}
	
	public void runTest(String regex, Attribute attribute) throws Exception {
		runTest(regex, attribute, true);
	}

	public void runTest(String regex, Attribute attribute, boolean useTranslator) throws Exception {
		results.clear();
		RegexPredicate regexPredicate = new RegexPredicate(
				regex, dataStore, Arrays.asList(new Attribute[]{attribute}), 
				luceneAnalyzer);

		regexMatcher = new RegexMatcher(regexPredicate, useTranslator);
		regexMatcher.open();
		ITuple nextTuple = null;
		while ((nextTuple = regexMatcher.getNextTuple()) != null) {
			results.add(nextTuple);
		}
		regexMatcher.close();
	}
	
	public void cleanUp() throws Exception {
		dataWriter.clearData();
	}

}
