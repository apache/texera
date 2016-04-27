package edu.uci.ics.textdb.sandbox.team3.team3lucenecityexample;

import static edu.uci.ics.textdb.sandbox.team3.team3lucenecityexample.LuceneIndexConstants.CONTENT_FIELD;
import static edu.uci.ics.textdb.sandbox.team3.team3lucenecityexample.LuceneIndexConstants.INDEX_DIR;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

/** Simple command-line based search demo. */
public class Searcher {

    private IndexSearcher searcher = null;
    private QueryParser parser = null;

    /** Creates a new instance of SearchEngine */
    public Searcher() throws IOException {
        searcher = new IndexSearcher(DirectoryReader.open(FSDirectory
                .open(Paths.get(INDEX_DIR))));
        parser = new QueryParser(CONTENT_FIELD, new StandardAnalyzer());
    }

    public TopDocs performSearch(String queryString, int n) throws IOException,
            ParseException {
        Query query = parser.parse(queryString);
        return searcher.search(query, n);
    }

    public Document getDocument(int docId) throws IOException {
        return searcher.doc(docId);
    }

}
