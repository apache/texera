package edu.uci.ics.texera.workflow.operators.keywordSearch;

import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;

import org.apache.lucene.document.Document;
import org.apache.lucene.store.MMapDirectory;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.Date;

public class Performance {
    private static String indexPath = "/Users/rohan/Downloads/tweetsIndex";
    SimpleAnalyzer analyzer = new SimpleAnalyzer();

    public Performance() throws IOException {
    }

    public void createIndex(String filepath, Directory indexDirectory) throws IOException {
        Path path = Paths.get(filepath);
        File file = path.toFile();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);

        IndexWriter indexWriter = new IndexWriter(indexDirectory, indexWriterConfig);

        Document document = new Document();
        FileReader fileReader = new FileReader(file);
        document.add(new TextField("contents", fileReader));
        document.add(new StringField("path", file.getPath(), Field.Store.YES));
        document.add(new StringField("filename", file.getName(), Field.Store.YES));

        indexWriter.addDocument(document);
        indexWriter.close();
    }

    public void searchQuery(String inField, String queryString, Directory indexDirectory) throws ParseException, IOException {
        Query query = new QueryParser(inField, analyzer).parse(queryString);

        IndexReader indexReader = DirectoryReader.open(indexDirectory);

        IndexSearcher searcher = new IndexSearcher(indexReader);
        TopDocs topDocs = searcher.search(query, 10);

        for (ScoreDoc sd : topDocs.scoreDocs) {
            Document d = searcher.doc(sd.doc);
            System.out.println("Path : "+ d.get("path") + ", Score : " + sd.score);
        }

        indexReader.close();
    }

    public void mMapDirectory(String filepath) throws IOException, ParseException {
        Path path = Paths.get(filepath);
        File file = path.toFile();

        BufferedReader br = new BufferedReader(new FileReader(file));

        Query query = new QueryParser("TEXT", analyzer).parse("Just");

        MMapDirectory mapDirectory = (MMapDirectory) MMapDirectory.open(Paths.get(indexPath));
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
        IndexWriter indexWriter = new IndexWriter(mapDirectory, indexWriterConfig);

        int hits = 0;
        String st;
        while ((st = br.readLine() ) != null) {
            Document document = new Document();
            document.add(new TextField("TEXT", st, Field.Store.NO));
            indexWriter.addDocument(document);
        }
        IndexReader indexReader = DirectoryReader.open(mapDirectory);

        IndexSearcher searcher = new IndexSearcher(indexReader);
        TopDocs topDocs = searcher.search(query, 1);
        System.out.println(topDocs.totalHits);
        System.out.println(st);

        if (topDocs.totalHits.value > hits) {
            System.out.println(st);
            hits += 1;
        }
        indexReader.close();
        indexWriter.close();
        br.close();
    }

    public void memoryIndex(String filepath) throws IOException, ParseException {
        Path path = Paths.get(filepath);
        File file = path.toFile();

        BufferedReader br = new BufferedReader(new FileReader(file));

        MemoryIndex index = new MemoryIndex();

        String st;
        while ((st = br.readLine() ) != null) {
            index.addField("TEXT", st, analyzer);
            QueryParser parser = new QueryParser("TEXT", analyzer);
            float score = index.search(parser.parse("Donald AND Trump AND Biden AND Joe AND elections"));
            if (score > 0.0f) {
                System.out.println(st);
            }
            index = new MemoryIndex();
        }
    }

    public static void main(String[] args) throws IOException, ParseException {
        String dataPath = "/Users/rohan/Downloads/subsetTweets.csv";
//        String dataPath = "/Users/rohan/Downloads/tweet_1week.csv";

        MMapDirectory mapDirectory = (MMapDirectory) MMapDirectory.open(Paths.get(indexPath));

        Performance p = new Performance();

        Date date1 = new Date();
        System.out.println("Start MMapDirectory " + new Timestamp(date1.getTime()));
        p.mMapDirectory(dataPath);
        Date date2 = new Date();
        System.out.println("End MMapDirectory " + new Timestamp(date2.getTime()));
        long totalTime = date2.getTime() - date1.getTime();
        System.out.println("Total building time: " + totalTime);

//        Date date1 = new Date();
//        System.out.println("Start MemoryIndex " + new Timestamp(date1.getTime()));
//        p.memoryIndex(dataPath);
//        Date date2 = new Date();
//        System.out.println("End MemoryIndex " + new Timestamp(date2.getTime()));
//        long totalTime = date2.getTime() - date1.getTime();
//        System.out.println("Total building time: " + totalTime);

//        Date date1 = new Date();
//        System.out.println("Start building index " + new Timestamp(date1.getTime()));
//        p.createIndex(dataPath, mapDirectory);
//        Date date2 = new Date();
//        long totalTime = date2.getTime() - date1.getTime();
//        System.out.println("End building index " + new Timestamp(date2.getTime()));
//        System.out.println("Total building time: " + totalTime);
//
//        Date date3 = new Date();
//        System.out.println("Start searching index " + new Timestamp(date3.getTime()));
//        p.searchQuery("TEXT", "Biden", mapDirectory);
//        Date date4 = new Date();
//        long totalTime2 = date4.getTime() - date3.getTime();
//        System.out.println("Total searching index " + new Timestamp(date4.getTime()));
//        System.out.println("Total searching time: " + totalTime2);
    }
}
