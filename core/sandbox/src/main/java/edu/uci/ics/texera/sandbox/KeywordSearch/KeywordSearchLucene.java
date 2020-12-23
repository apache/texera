package edu.uci.ics.texera.sandbox.KeywordSearch;

import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.MMapDirectory;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Sample non-blocking keyword search operator implementation using Lucene mmap API.
 * Lucene MemoryIndex API is preferred compared to MMap API because its much higher performance.
 * See https://github.com/Texera/texera/pull/932
 */
public class KeywordSearchLucene {

    public static String keyword = "keyword";
    public static String attribute = "attribute";
    public static SimpleAnalyzer analyzer = new SimpleAnalyzer();
    public static Query query;
    public static Path tempDirectory;
    public static MMapDirectory mMapDirectory;
    public static IndexWriter indexWriter;

    static {
        try {
            query = new QueryParser(attribute, analyzer).parse(keyword);
            tempDirectory = Files.createTempDirectory("texera-keyword-temp");
            mMapDirectory = new MMapDirectory(tempDirectory);
            indexWriter = new IndexWriter(mMapDirectory, new IndexWriterConfig(analyzer));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean findKeywordMmap(String fieldValue) throws Exception {
        Document doc = new Document();
        doc.add(new Field(attribute, fieldValue, TextField.TYPE_STORED));
        indexWriter.addDocument(doc);

        IndexReader indexReader = DirectoryReader.open(indexWriter);
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);
        TopDocs topDocs = indexSearcher.search(query, 1);
        boolean isMatch = topDocs.totalHits > 0;
        indexWriter.deleteAll();

        return isMatch;
    }

    public void close() throws Exception {
        Files.deleteIfExists(tempDirectory);
    }
}
