package edu.uci.ics.amber.operator.keywordSearch

import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.operator.filter.FilterOpExec
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.memory.MemoryIndex
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.Query

class KeywordSearchOpExec(descString: String) extends FilterOpExec {
  private val desc: KeywordSearchOpDesc =
    objectMapper.readValue(descString, classOf[KeywordSearchOpDesc])

  // We chose StandardAnalyzer because it provides more comprehensive tokenization, retaining numeric tokens and handling a broader range of characters.
  // This ensures that search functionality can include standalone numbers (e.g., "3") and complex queries while offering robust performance for most use cases.
  @transient private lazy val analyzer = new StandardAnalyzer()
  @transient lazy val query: Query = new QueryParser(desc.attribute, analyzer).parse(desc.keyword)
  @transient private lazy val memoryIndex: MemoryIndex = new MemoryIndex()

  this.setFilterFunc(findKeyword)

  private def findKeyword(tuple: Tuple): Boolean = {
    Option[Any](tuple.getField(desc.attribute)).map(_.toString).exists { fieldValue =>
      memoryIndex.addField(desc.attribute, fieldValue, analyzer)
      val isMatch = memoryIndex.search(query) > 0.0f
      memoryIndex.reset()
      isMatch
    }
  }

}
