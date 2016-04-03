package edu.uci.ics.textdb.lucenedemo;

import java.util.Iterator;


import org.apache.lucene.document.Field;
import org.apache.lucene.document.Document;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;

import org.apache.lucene.document.Document;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.IndexSearcher;

/**
 *
 * @author Rajesh
 */
public class DemoMain {

	/** Creates a new instance of Main */
	public DemoMain() {
	}

	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) {

		try {
			// build a lucene index
//			System.out.println("buildIndex");
//			Indexer indexer = new Indexer();
//			indexer.rebuildIndexes();
//			System.out.println("Indexing done");

			// perform search on user queries
			// and retrieve the top 10 result

			System.out.println(" performSearch ");

			Searcher se = new Searcher();
			TopDocs topDocs = se.performSearch("id:1*", 100);

			System.out.println("Results found: " + topDocs.totalHits);
			ScoreDoc[] hits = topDocs.scoreDocs;
			for (int i = 0; i < hits.length; i++) {
				Document doc = se.getDocument(hits[i].doc);
				System.out.println("Name: " + doc.get("name") + " "+ ", City: " + doc.get("city") + " (" + hits[i].score + ")");

			}

			System.out.println("performSearch done");
		}

		catch (Exception e) {
			System.out.println("Exception caught.\n");
		}
	}

}
