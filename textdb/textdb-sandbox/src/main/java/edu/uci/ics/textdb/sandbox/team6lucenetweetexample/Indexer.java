package edu.uci.ics.textdb.sandbox.team6lucenetweetexample;

import static edu.uci.ics.textdb.sandbox.team6lucenetweetexample.LuceneIndexConstants.CONTENT_FIELD;
import static edu.uci.ics.textdb.sandbox.team6lucenetweetexample.LuceneIndexConstants.DATE_FIELD;
import static edu.uci.ics.textdb.sandbox.team6lucenetweetexample.LuceneIndexConstants.ID_FIELD;
import static edu.uci.ics.textdb.sandbox.team6lucenetweetexample.LuceneIndexConstants.INDEX_DIR;
import static edu.uci.ics.textdb.sandbox.team6lucenetweetexample.LuceneIndexConstants.TEXT_FIELD;
import static edu.uci.ics.textdb.sandbox.team6lucenetweetexample.LuceneIndexConstants.USER_FIELD;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;

/**
 * Index all text files under a directory.
 * <p>
 * 
 */
public class Indexer {

    public Indexer() {
    }

    /** Index all text files under a directory. */
    private IndexWriter indexWriter = null;

    public IndexWriter getIndexWriter() throws IOException {
        if (indexWriter == null) {
            FSDirectory indexDir = FSDirectory.open(Paths.get(INDEX_DIR));
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            indexWriter = new IndexWriter(indexDir, iwc);
        }
        return indexWriter;
    }

    public void closeIndexWriter() throws IOException {
        if (indexWriter != null) {
            indexWriter.close();
        }
    }

    public void indexTweet(Tweet tweet) throws IOException {

        System.out.println("Indexing tweets: " + tweet.toString());
        IndexWriter writer = getIndexWriter();
        Document doc = new Document();
        doc.add(new StringField(ID_FIELD, tweet.getId(), Field.Store.YES));
        doc.add(new StringField(DATE_FIELD, tweet.getDate(), Field.Store.YES));
        doc.add(new StringField(USER_FIELD, tweet.getUser(), Field.Store.YES));
        doc.add(new StringField(TEXT_FIELD, tweet.getText(), Field.Store.YES));
        String fullSearchableText = tweet.getDate() + " " + tweet.getUser()
                + " " + tweet.getText();
        doc.add(new TextField(CONTENT_FIELD, fullSearchableText, Field.Store.NO));
        writer.addDocument(doc);
    }

    public void rebuildIndexes() throws IOException {
        getIndexWriter();
        indexWriter.deleteAll();
        // Index all Accommodation entries
        for (Tweet tweet : Data.getTweets()) {
            indexTweet(tweet);
        }

        // Closing the Index
        closeIndexWriter();
    }

}
