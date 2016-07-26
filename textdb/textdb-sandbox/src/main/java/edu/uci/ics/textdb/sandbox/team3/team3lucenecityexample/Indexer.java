package edu.uci.ics.textdb.sandbox.team3.team3lucenecityexample;

import static edu.uci.ics.textdb.sandbox.team3.team3lucenecityexample.LuceneIndexConstants.CONTENT_FIELD;
import static edu.uci.ics.textdb.sandbox.team3.team3lucenecityexample.LuceneIndexConstants.COUNTRY_FIELD;
import static edu.uci.ics.textdb.sandbox.team3.team3lucenecityexample.LuceneIndexConstants.ID_FIELD;
import static edu.uci.ics.textdb.sandbox.team3.team3lucenecityexample.LuceneIndexConstants.INDEX_DIR;
import static edu.uci.ics.textdb.sandbox.team3.team3lucenecityexample.LuceneIndexConstants.NAME_FIELD;

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

    public void indexCity(City city) throws IOException {

        System.out.println("Indexing city: " + city);
        IndexWriter writer = getIndexWriter();
        Document doc = new Document();
        doc.add(new StringField(ID_FIELD, city.getId(), Field.Store.YES));
        doc.add(new StringField(NAME_FIELD, city.getName(), Field.Store.YES));
        doc.add(new StringField(COUNTRY_FIELD, city.getCountry(), Field.Store.YES));
        String fullSearchableText = city.getName() + " " + city.getCountry()
                + " " + city.getDescription();
        doc.add(new TextField(CONTENT_FIELD, fullSearchableText, Field.Store.NO));
        writer.addDocument(doc);
    }

    public void rebuildIndexes() throws IOException {
        getIndexWriter();
        indexWriter.deleteAll();
        // Index all Accommodation entries
        City[] cities = Data.getCities();
        for (City city : cities) {
            indexCity(city);
        }

        // Closing the Index
        closeIndexWriter();
    }

}
