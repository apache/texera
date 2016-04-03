package edu.uci.ics.textdb.lucenedemo;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

/** Simple command-line based search demo. */
public class Searcher {

	 private IndexSearcher searcher = null;
	    private QueryParser parser = null;
	    
	    /** Creates a new instance of SearchEngine */
	    public Searcher() throws IOException {
	    	File file = new File("index");
	        searcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get("index"))));
	        parser = new QueryParser("content", new StandardAnalyzer());
	    }
	    
	    public TopDocs performSearch(String queryString, int n)
	    throws IOException, ParseException {
	        Query query = parser.parse(queryString);        
	        return searcher.search(query, n);
	    }

	    public Document getDocument(int docId)
	    throws IOException {
	        return searcher.doc(docId);
	    }

 
}

