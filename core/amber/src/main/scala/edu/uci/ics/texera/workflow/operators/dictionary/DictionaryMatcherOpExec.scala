package edu.uci.ics.texera.workflow.operators.dictionary

import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.workflow.common.operators.map.MapOpExec
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute

import java.io.StringReader
import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Set}
import scala.jdk.CollectionConverters.CollectionHasAsScala

class DictionaryMatcherOpExec(
    attributeName: String,
    dictionary: String,
    matchingType: MatchingType
) extends MapOpExec {

  // this is needed for the matching types Phrase and Conjunction
  var tokenizedDictionaryEntries: ListBuffer[mutable.Set[String]] = _
  // this is needed for the simple Scan matching type
  var dictionaryEntries: List[String] = _
  var luceneAnalyzer: Analyzer = _

  /**
    * first prepare the dictionary by splitting the values using a comma delimiter then tokenize the split values
    */
  override def open(): Unit = {
    // create the dictionary by splitting the values first
    dictionaryEntries = dictionary.split(",").toList.map(_.toLowerCase)
    if (matchingType == MatchingType.CONJUNCTION_INDEXBASED) {
      // then tokenize each entry
      this.luceneAnalyzer = new EnglishAnalyzer
      tokenizedDictionaryEntries = ListBuffer[mutable.Set[String]]()
      tokenizeDictionary()
    }
  }

  override def close(): Unit = {
    tokenizedDictionaryEntries = null
    dictionaryEntries = null
    luceneAnalyzer = null
  }

  /**
    * use LuceneAnalyzer to tokenize the dictionary
    */
  private def tokenizeDictionary(): Unit = {
    for (text <- dictionaryEntries) {
      tokenizedDictionaryEntries += tokenize(text)
    }
  }

  /**
    * Checks if a given tuple matches any entry in a dictionary according to specified matching criteria.
    * The tuple is first converted to a lower-cased string, and the matching is performed based on the {@link MatchingType}.
    *
    * @param tuple The tuple to match against the dictionary entries.
    * @return Boolean indicating whether the tuple matches any dictionary entry according to the specified matching type.
    */
  private def isTupleInDictionary(tuple: Tuple): Boolean = {
    val text = tuple.getField(attributeName).asInstanceOf[String].toLowerCase

    // Return false if the text is empty, as it cannot match any dictionary entry
    if (text.isEmpty) return false

    matchingType match {
      case MatchingType.SCANBASED =>
        // Directly check if the dictionary contains the text
        dictionaryEntries.contains(text)

      case MatchingType.SUBSTRING =>
        // Check if any dictionary entry contains the text as a substring
        dictionaryEntries.exists(entry => entry.contains(text))

      case MatchingType.CONJUNCTION_INDEXBASED =>
        // Tokenize the text and check if any tokenized dictionary entry is a subset of the tokenized text
        val tokenizedText = tokenize(text)
        tokenizedText.nonEmpty && tokenizedDictionaryEntries.exists(entry =>
          entry.subsetOf(tokenizedText)
        )
    }
  }

  /**
    * tokenize a given text to a list of tokens after removing stopwords
    *
    * @param text String
    * @return
    */
  def tokenize(text: String): mutable.Set[String] = {
    val tokenizedSet = mutable.Set[String]()
    val tokenStream = luceneAnalyzer.tokenStream(null, new StringReader(text))
    val offsetAttribute = tokenStream.addAttribute(classOf[OffsetAttribute])
    tokenStream.reset()
    while ({
      tokenStream.incrementToken
    }) {
      val charStart = offsetAttribute.startOffset
      val charEnd = offsetAttribute.endOffset
      val termStr = text.substring(charStart, charEnd).toLowerCase
      if (
        !EnglishAnalyzer.ENGLISH_STOP_WORDS_SET.contains(termStr) && !Utils.URL_STOP_WORDS_SET
          .contains(termStr)
      )
        tokenizedSet += termStr
    }
    tokenStream.close()
    tokenizedSet
  }

  /**
    * match an input tuple and give it a label if it is present in the dictionary
    *
    * @param tuple Tuple
    * @return
    */
  private def matchTuple(tuple: Tuple): TupleLike = {
    var matched: Boolean = false
    if (tuple.getField(attributeName) != null) {
      matched = isTupleInDictionary(tuple)
    }
    TupleLike(tuple.getFields.asScala ++ List(matched))
  }

  setMapFunc(matchTuple)
}
