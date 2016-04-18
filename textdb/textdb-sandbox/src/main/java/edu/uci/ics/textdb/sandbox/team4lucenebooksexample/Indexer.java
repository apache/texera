package edu.uci.ics.textdb.sandbox.team4lucenebooksexample;

import static edu.uci.ics.textdb.sandbox.team4lucenebooksexample.LuceneIndexConstants.AUTHOR_FIELD;
import static edu.uci.ics.textdb.sandbox.team4lucenebooksexample.LuceneIndexConstants.CONTENT_FIELD;
import static edu.uci.ics.textdb.sandbox.team4lucenebooksexample.LuceneIndexConstants.ID_FIELD;
import static edu.uci.ics.textdb.sandbox.team4lucenebooksexample.LuceneIndexConstants.INDEX_DIR;
import static edu.uci.ics.textdb.sandbox.team4lucenebooksexample.LuceneIndexConstants.NAME_FIELD;

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

    public void indexBooks(Book book) throws IOException {

        System.out.println("Indexing Book: " + book);
        IndexWriter writer = getIndexWriter();
        Document doc = new Document();
        doc.add(new StringField(ID_FIELD, book.getId(), Field.Store.YES));
        doc.add(new StringField(NAME_FIELD, book.getName(), Field.Store.YES));
        doc.add(new StringField(AUTHOR_FIELD, book.getAuthor(), Field.Store.YES));
        String fullSearchableText = book.getName() + " " + book.getAuthor()
                + " " + book.getDescription();
        doc.add(new TextField(CONTENT_FIELD, fullSearchableText, Field.Store.NO));
        writer.addDocument(doc);
    }

    public void rebuildIndexes() throws IOException {
        getIndexWriter();
        indexWriter.deleteAll();
        // Index all book entries
        Book[] Books = Data.getBooks();
        for (Book book : Books) {
            indexBooks(book);
        }

        // Closing the Index
        closeIndexWriter();
    }

}
