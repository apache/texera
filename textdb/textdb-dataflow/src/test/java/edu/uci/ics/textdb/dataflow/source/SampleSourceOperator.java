package edu.uci.ics.textdb.dataflow.source;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.common.StringField;
import edu.uci.ics.textdb.dataflow.common.SampleTuple;

import java.io.IOException;
import java.nio.file.Paths;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

/**
 * Created by chenli on 3/31/16.
 */
public class SampleSourceOperator implements ISourceOperator {
	private IndexSearcher indexSearcher;
    private ScoreDoc[] scoreDocs;
    private int cursor;

    public SampleSourceOperator() throws Exception{
    	init();
    }

    public void init() throws Exception {
    	try {
			IndexReader indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(Constants.INDEX_DIR)));
			indexSearcher = new IndexSearcher(indexReader);
			Analyzer analyzer = new StandardAnalyzer();
			QueryParser queryParser = new QueryParser(Constants.FIRST_NAME, analyzer);
			Query query = queryParser.parse(Constants.WILD_CARD_QUERY);
			
			TopDocs topDocs = indexSearcher.search(query, Integer.MAX_VALUE);
			scoreDocs = topDocs.scoreDocs;
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		} catch (ParseException e) {
			e.printStackTrace();
			throw e;
		}
		
	}

	@Override
    public void open() {
        cursor = 0;
    }

    @Override
    public ITuple getNextTuple() throws Exception{
//        if (cursor < 0 || cursor >= SAMPLE_TUPLES.size()) {
//            return null;
//        }
//        return SAMPLE_TUPLES.get(cursor++);
    	try {
			if(cursor >= scoreDocs.length){
				return null;
			}
			Document document = indexSearcher.doc(scoreDocs[cursor++].doc);
			IField firstName = new StringField(document.get(Constants.FIRST_NAME));
			IField lastName = new StringField(document.get(Constants.LAST_NAME));
			
			SampleTuple sampleTuple = new SampleTuple(Constants.SAMPLE_SCHEMA, firstName, lastName);
			return sampleTuple;
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
    	
    }

    @Override
    public void close() {
    	
    }

}
