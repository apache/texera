package edu.uci.ics.textdb.lucenedemo;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;


/** Index all text files under a directory.
 * <p>
 
 */
public class Indexer {
  
  public Indexer() {}

  /** Index all text files under a directory. */
  private IndexWriter indexWriter = null;
  
  public IndexWriter getIndexWriter(boolean create) throws IOException {
      if (indexWriter == null) {
    	  
    	 new File("index");
          FSDirectory indexDir = FSDirectory.open(Paths.get("index"));
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
  
  // Indexing a document
  
  public void indexHotel(Hotel hotel) throws IOException {

      System.out.println("Indexing hotel: " + hotel);
      IndexWriter writer = getIndexWriter(false);
      Document doc = new Document();
      doc.add(new StringField("id", hotel.getId(), Field.Store.YES));
      doc.add(new StringField("name", hotel.getName(), Field.Store.YES));
      doc.add(new StringField("city", hotel.getCity(), Field.Store.YES));
      String fullSearchableText = hotel.getName() + " " + hotel.getCity() + " " + hotel.getDescription();
      doc.add(new TextField("content", fullSearchableText, Field.Store.NO));
      writer.addDocument(doc);
  }   
  
  public void rebuildIndexes() throws IOException {
        //
        // Erase existing index
        //
        getIndexWriter(true);
        //
        // Index all Accommodation entries
        //
        Hotel[] hotels = Data.getHotels();
        for(Hotel hotel : hotels) {
            indexHotel(hotel);              
        }
        //
        // Closing the Index
        //
        closeIndexWriter();
   }    
 
  
}
