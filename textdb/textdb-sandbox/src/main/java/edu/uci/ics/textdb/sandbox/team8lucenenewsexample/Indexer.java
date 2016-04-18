package edu.uci.ics.textdb.sandbox.team8lucenenewsexample;


import static edu.uci.ics.textdb.sandbox.team8lucenenewsexample.LuceneIndexConstants.*;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;

/**
 * Created by Sam on 16/4/10.
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

    public void indexNews(News news) throws IOException {

        System.out.println("Indexing News: " + news.toString());
        IndexWriter writer = getIndexWriter();
        Document doc = new Document();
        doc.add(new StringField(ID_FIELD, news.getId(), Field.Store.YES));
        doc.add(new StringField(TITLE_FIELD, news.getTitle(), Field.Store.YES));
        doc.add(new StringField(CATEGORY_FIELD, news.getCategory(), Field.Store.YES));
        doc.add(new StringField(TEXT_FIELD, news.getText(), Field.Store.YES));

        String fullSearchableText = news.getTitle()+" "+news.getCategory()+" "+news.getText();

        doc.add(new TextField(CONTENT_FIELD, fullSearchableText, Field.Store.NO));
        writer.addDocument(doc);
    }

    public void rebuildIndexes() throws IOException {
        getIndexWriter();
        indexWriter.deleteAll();


        //load data from DATA
        News[] newses=Data.getNewes();

        // Index all Accommodation entries
        for (News news : Data.getNewes()) {
            indexNews(news);
        }

        // Closing the Index
        closeIndexWriter();
    }
}
