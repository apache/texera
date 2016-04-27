package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.constants.LuceneConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.dataflow.common.RegexPredicate;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.LuceneDataStore;
import edu.uci.ics.textdb.storage.reader.LuceneDataReader;
import edu.uci.ics.textdb.storage.writer.LuceneDataWriter;


/**
 * @author zuozhi
 * 	
 * Helper class to quickly create unit test
 */
public class RegexMatcherTestHelper {

	private RegexMatcher regexMatcher;
	private IDataWriter dataWriter;
	private IDataReader dataReader;
	private IDataStore dataStore;
	
	private List<ITuple> results;
    private Analyzer analyzer;
    private Query query;
	
	public RegexMatcherTestHelper(Schema schema, List<ITuple> data) throws Exception {
		dataStore = new LuceneDataStore(LuceneConstants.INDEX_DIR, schema);
		analyzer = new  StandardAnalyzer();
        dataWriter = new LuceneDataWriter(dataStore, analyzer);
		dataWriter.clearData();
		dataWriter.writeData(data);	
		results = new ArrayList<ITuple>();
	}
	
	
	public void runTest(String regex, String fieldName) throws Exception {
		results.clear();
		QueryParser queryParser = new QueryParser(
                TestConstants.FIRST_NAME, analyzer);
        query = queryParser.parse(LuceneConstants.SCAN_QUERY);
        dataReader = new LuceneDataReader(dataStore, query);

		IPredicate predicate = new RegexPredicate(regex, fieldName);
		ISourceOperator sourceOperator = new ScanBasedSourceOperator(dataReader);


		regexMatcher = new RegexMatcher(predicate, sourceOperator);
		regexMatcher.open();
		ITuple nextTuple = null;
		while ((nextTuple = regexMatcher.getNextTuple()) != null) {
			results.add(nextTuple);
		}
		regexMatcher.close();
	}
	
	
	public boolean matchExpectedResults(List<ITuple> expected) {
		return TestUtils.equalTo(results, expected);
	}
	
	
	public void cleanUp() throws Exception {
		dataWriter.clearData();
	}

}
