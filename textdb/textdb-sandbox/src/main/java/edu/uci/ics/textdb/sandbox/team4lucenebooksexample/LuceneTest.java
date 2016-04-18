package edu.uci.ics.textdb.sandbox.team4lucenebooksexample;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
public class LuceneTest {

    public static void main(String[] args) {
        Indexer id = new Indexer();
        Searcher s;
        try {
            id.rebuildIndexes();
            s = new Searcher();
            TopDocs topDocs = s.performSearch("potter", 10);
            for(int i=0;i<topDocs.scoreDocs.length;i++){
            	Document doc = s.getDocument(topDocs.scoreDocs[i].doc);
            	System.out.println(doc.get("name"));
            	System.out.println(doc.get("author"));

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}