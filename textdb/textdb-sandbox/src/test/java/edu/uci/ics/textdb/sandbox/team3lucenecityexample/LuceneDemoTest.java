package edu.uci.ics.textdb.sandbox.team3lucenecityexample;

import static edu.uci.ics.textdb.sandbox.team3lucenecityexample.LuceneIndexConstants.CONTENT_FIELD;
import static edu.uci.ics.textdb.sandbox.team3lucenecityexample.LuceneIndexConstants.ID_FIELD;
import static edu.uci.ics.textdb.sandbox.team3lucenecityexample.LuceneIndexConstants.NAME_FIELD;
import static edu.uci.ics.textdb.sandbox.team3lucenecityexample.LuceneIndexConstants.COUNTRY_FIELD;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
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
		String queryString = "*:*";
		System.out.println("\nperformSearch");

		// TopDocs contains a reference to the documents
		TopDocs topDocs = searcher.performSearch(queryString, maxResults);

		System.out.println("Results found: " + topDocs.totalHits);
		ScoreDoc[] hits = topDocs.scoreDocs;
		for (int i = 0; i < hits.length; i++) {
			Document doc = searcher.getDocument(hits[i].doc);
			System.out.println("Id: " + doc.get(ID_FIELD) + ", Name: " + doc.get(NAME_FIELD) + " " + ", Country: "
			        + doc.get(COUNTRY_FIELD));


		}

		System.out.println("performSearch done");

	}
}
