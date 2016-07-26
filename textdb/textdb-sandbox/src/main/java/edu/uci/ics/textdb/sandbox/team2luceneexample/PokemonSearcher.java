package edu.uci.ics.textdb.sandbox.team2luceneexample;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

/**
 * Created by kishorenarendran on 08/04/16.
 */
public class PokemonSearcher {
    private IndexSearcher indexSearcher;
    private QueryParser queryParser;

    public PokemonSearcher(String fieldName) throws IOException {
        indexSearcher = new IndexSearcher(DirectoryReader.open(
                FSDirectory.open(Paths.get(LuceneConstants.INDEX))));
        queryParser = new QueryParser(fieldName, new StandardAnalyzer());
    }

    public Document[] performSearch(String queryString, int n) throws IOException,
            ParseException {
        Query query = queryParser.parse(queryString);
        TopDocs topDocs = indexSearcher.search(query, n);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        Document[] documents = new Document[scoreDocs.length];
        for(int i = 0; i < documents.length; i++) {
            documents[i] = getDocument(scoreDocs[i].doc);
        }
        return documents;
    }

    public Document getDocument(int docId) throws IOException {
        return indexSearcher.doc(docId);
    }
}