package edu.uci.ics.textdb.sandbox.team8lucenenewsexample;

import static edu.uci.ics.textdb.sandbox.team8lucenenewsexample.LuceneIndexConstants.ID_FIELD;
import static edu.uci.ics.textdb.sandbox.team8lucenenewsexample.LuceneIndexConstants.TEXT_FIELD;
import static edu.uci.ics.textdb.sandbox.team8lucenenewsexample.LuceneIndexConstants.TITLE_FIELD;

import java.io.IOException;
import java.util.Scanner;

import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

/**
 * Created by Sam on 16/4/10.
 */
public class Main {


    public static void main( String[] args ) throws IOException, ParseException {

        Indexer indexer = new Indexer();
        indexer.rebuildIndexes();

        Searcher searcher= new Searcher();

        // max docs retrieved will be 100
        int maxResults = 20;

        Scanner reader = new Scanner(System.in);  // Reading from System.in
        System.out.println("Enter a query: ");
        String queryString = reader.nextLine();


        System.out.println("\nperformSearch");

        // TopDocs contains a reference to the documents
        TopDocs topDocs = searcher.performSearch(queryString, maxResults);

        System.out.println("Results found: " + topDocs.totalHits);

        ScoreDoc[] hits = topDocs.scoreDocs;
        for (int i = 0; i < hits.length; i++) {
            Document doc = searcher.getDocument(hits[i].doc);
            System.out.println("Id: " + doc.get(ID_FIELD) + ", Title: " + doc.get(TITLE_FIELD) + " " + ", Text: "
                    + doc.get(TEXT_FIELD));

        }

        System.out.println("performSearch done");
        
        reader.close();


    }

}
