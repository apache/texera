package edu.uci.ics.textdb.lucenedemo;

import java.util.Scanner;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.ScoreDoc;

/**
 *
 * @author Rajesh
 */
public class DemoMain {

	/** Creates a new instance of DemoMain */
	public DemoMain() {
	}

	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) {

		try {
			
			/* 
			 * Builds a lucene index
			 * 
			 */
			
//			 System.out.println("buildIndex");
//			 Indexer indexer = new Indexer();
//			 indexer.rebuildIndexes();
//			 System.out.println("Indexing done");

			/* perform search on user queries
			* and retrieve the top 50 results
			*/
			
			Scanner reader = new Scanner(System.in); // Reading from System.in
			System.out.println("Enter a search string: ");
			String s = reader.next(); 
										
			reader.close();
			System.out.println(" performSearch ");
			
			/*
			 * Performs Search
			 * 
			 */
			Searcher se = new Searcher();
			TopDocs topDocs = se.performSearch(s, 50);

			System.out.println("Results found: " + topDocs.totalHits);
			ScoreDoc[] hits = topDocs.scoreDocs;
			for (int i = 0; i < hits.length; i++) {
				Document doc = se.getDocument(hits[i].doc);
				System.out.println(
						"Id: " + doc.get("id") + ", Name: " + doc.get("name") + " " + ", City: " + doc.get("city"));

			}

			System.out.println("performSearch done");

		}

		catch (Exception e) {
			System.out.println("Exception caught.\n");
		}
	}

}
