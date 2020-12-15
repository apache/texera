package edu.uci.ics.texera.workflow.operators.keywordSearch

import java.io.{BufferedWriter, File, FileReader, FileWriter, IOException, StringReader}

import edu.uci.ics.texera.workflow.common.operators.filter.FilterOpExec
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.analysis.core.SimpleAnalyzer
import org.apache.lucene.index.memory.MemoryIndex
import org.apache.lucene.store.MMapDirectory
import java.nio.file.{FileSystems, Files, Paths}
import java.sql.Timestamp
import java.util.Date

import org.apache.commons.io.FileUtils
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.{Document, Field, StringField, TextField}
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.search.Query
import org.apache.lucene.search.IndexSearcher
import java.nio.file.Files

class KeywordSearchOpExec(var counter: Int, val opDesc: KeywordSearchOpDesc) extends FilterOpExec {
  @transient lazy val mmapDir: MMapDirectory = new MMapDirectory(
    Files.createTempDirectory("texera-keyword-" + counter)
  )
  @transient lazy val analyzer = new SimpleAnalyzer();
  @transient lazy val indexWriter: IndexWriter =
    new IndexWriter(mmapDir, new IndexWriterConfig(analyzer))
  @transient lazy val queryParser: QueryParser =
    new QueryParser(opDesc.columnName, analyzer)
  @transient lazy val query: Query = queryParser.parse(opDesc.keyword)

  this.setFilterFunc(this.findKeyword)

  def findKeyword(tuple: Tuple): Boolean = {
    try {
      @transient lazy val memoryIndex: MemoryIndex = new MemoryIndex();
      val tupleValue = tuple.getField(opDesc.columnName).toString
      memoryIndex.addField(opDesc.columnName, tupleValue, analyzer)

      if (memoryIndex.search(query) > 0.0f) {
        true
      } else {
        false
      }
    } catch {
      case e: NullPointerException => false
    }
  }

  def findKeywordMMap(tuple: Tuple): Boolean = {
    val doc = new Document()
    doc.add(
      new Field(
        opDesc.columnName,
        tuple.getField(opDesc.columnName, classOf[String]),
        TextField.TYPE_STORED
      )
    )
    indexWriter.addDocument(doc)
    doc.clear()

    val indexReader = DirectoryReader.open(indexWriter)
    val searcher = new IndexSearcher(indexReader)

    val topDocs = searcher.search(query, 1)

    indexWriter.deleteAll()

    if (topDocs.totalHits.value > 0) {
      true
    } else {
      false
    }
  }
}
