package edu.uci.ics.textdb.sandbox.team1lucenehotelexample;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;
import org.junit.Before;
import org.junit.Test;

public class LuceneDemoTest {

	private Indexer indexer;
	private Searcher searcher;
	@Before
	public void setUp() throws IOException {
		indexer = new Indexer();
	}

	@Test
	public void testIndexing() throws IOException {
		System.out.println("Building Indexes");
		indexer.rebuildIndexes();
		System.out.println("Finished Indexing");
	}

	@Test
	public void testIndexingAndSearching() throws IOException, ParseException {
		System.out.println("Building Indexes");
		indexer.rebuildIndexes();
		System.out.println("Finished Indexing");

		searcher = new Searcher();
		// max docs retrieved will be 100
		int maxResults = 100;

		// *:* performs a full scan
		// Modify this to try different queries
		String queryString = "paris";
		System.out.println("\nperformSearch");

		IndexSearcher Se = searcher.getSearcher();

		
		TopDocs topDocs = searcher.performSearch(queryString, maxResults);
		SpanTermQuery spanTerm = searcher.makeSpanTermQuery(queryString);
		
		SpanWeight W = spanTerm.createWeight(Se, false);
		if(W.equals(null))
			System.out.println("yes");
		List<LeafReaderContext> ctx = searcher.getIndexReader().leaves();
		System.out.println("size: "+ctx.size());
		Spans S = W.getSpans(ctx.get(0), SpanWeight.Postings.POSITIONS);
		
		if(S == null)
			System.out.println("yes");
		S.nextDoc();
		
		S.nextDoc();

	
		System.out.println("DOC ID:" + S.docID());
		
		int start = S.nextStartPosition();
		
		
		
		System.out.println("Start :" + start);
		
		
		System.out.println("End :" + S.endPosition());
		
		
		System.out.println("Results found: " + topDocs.totalHits);
//		ScoreDoc[] hits = topDocs.scoreDocs;
//		for (int i = 0; i < hits.length; i++) {
//			Document doc = searcher.getDocument(hits[i].doc);
//			System.out.println("Id: " + doc.get(ID_FIELD) + ", Name: " + doc.get(NAME_FIELD) + " " + ", Content: "
//			        + doc.get("content"));
//
//		}

		System.out.println("performSearch done");

	}
}
