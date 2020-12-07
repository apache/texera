package edu.uci.ics.texera.workflow.operators.keywordSearch

import java.io.{BufferedWriter, File, FileReader, FileWriter, IOException, StringReader}

import edu.uci.ics.texera.workflow.common.operators.filter.FilterOpExec
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.analysis.core.SimpleAnalyzer
import org.apache.lucene.index.memory.MemoryIndex
import org.apache.lucene.store.MMapDirectory
import java.nio.file.FileSystems
import java.nio.file.Paths
import java.sql.Timestamp
import java.util.Date

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.{Document, Field, StringField, TextField}
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.search.Query
import org.apache.lucene.search.IndexSearcher

class KeywordSearchOpExec(val opDesc: KeywordSearchOpDesc, var counter: Int) extends FilterOpExec {
  var kw: String = opDesc.keyword
  this.setFilterFunc(this.findKeywordMMap)

  val analyzer = new SimpleAnalyzer()
  val parser = new QueryParser(opDesc.columnName, analyzer)

  def findKeyword(tuple: Tuple): Boolean = {
//    if (counter == 0) {
//      val date1 = new Date
//      System.out.println("Start " + new Timestamp(date1.getTime))
//    }
//    counter += 1
//    if (counter >= 450000) {
//      val date2 = new Date
//      System.out.println("End " + new Timestamp(date2.getTime))
//    }
    try {
      val tupleValue = tuple.getField(opDesc.columnName).toString
      val index = new MemoryIndex()
      index.addField(opDesc.columnName, tupleValue, analyzer)
      val score = index.search(parser.parse(kw))

      if (score > 0.0f) true
      else false
    }
    catch {
      case e: NullPointerException => false
    }
  }

  def findKeywordMMap(tuple: Tuple): Boolean = {
    def createIndex(): MMapDirectory = {
//      val indexPath = "/Users/rohan/Downloads/tweetsIndex"
//      val dataPath = "/Users/rohan/Downloads/subsetTweets.csv"
//      val dataPath = "/Users/rohan/Downloads/tweet_1week.csv"

      val tupleValue = tuple.getField(opDesc.columnName).toString

      import java.io.File
      val tempFile = File.createTempFile("tempFile", ".txt")
      val dataPath = tempFile.getAbsolutePath

      val bw = new BufferedWriter(new FileWriter(tempFile))
      bw.write(tupleValue)
      bw.close()

      val path = Paths.get(dataPath)
      val file = path.toFile

      val indexWriterConfig = new IndexWriterConfig(analyzer);

      val date1 = new Date
      val time = new Timestamp(date1.getTime)
      val indexPath = "/Users/rohan/Downloads/tweetsIndex/" + time.toString;

      val indexDirectory = new MMapDirectory(Paths.get(indexPath));

      val indexWriter = new IndexWriter(indexDirectory, indexWriterConfig);

      val document = new Document();

      val fileReader = new FileReader(file)

      document.add(new TextField("TEXT", fileReader));

      indexWriter.addDocument(document)
      fileReader.close()
      indexWriter.close
      tempFile.deleteOnExit

      indexDirectory
    }

    val indexDirectory = createIndex()

    val query: Query = new QueryParser(opDesc.columnName, analyzer).parse(kw)

    val indexReader = DirectoryReader.open(indexDirectory)

    val searcher = new IndexSearcher(indexReader)

    val topDocs = searcher.search(query, 1)

    indexReader.close()


    if (topDocs.totalHits.value > 0) {
      true
    }
    else {
      false
    }
  }
}