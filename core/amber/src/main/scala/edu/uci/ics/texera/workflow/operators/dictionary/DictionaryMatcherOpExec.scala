package edu.uci.ics.texera.workflow.operators.dictionary

import edu.uci.ics.amber.engine.common.Utils
import edu.uci.ics.amber.engine.common.model.tuple.{Tuple, TupleLike}
import edu.uci.ics.texera.workflow.common.operators.map.MapOpExec
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute

import java.io.StringReader
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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
    * Determines whether a given tuple matches any dictionary entry based on defined matching criteria.
    * The tuple's specified field is converted to a lowercase string for comparison.
    *
    * @param tuple The tuple whose field is to be checked against dictionary entries.
    * @return true if the tuple matches a dictionary entry according to the matching criteria; false otherwise.
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
    * Tokenizes a given text into a set of unique tokens, excluding stopwords.
    *
    * @param text The input text to tokenize.
    * @return A mutable set of tokens derived from the input text, excluding stopwords.
    */
  private def tokenize(text: String): mutable.Set[String] = {
    val tokens = mutable.Set[String]()
    val tokenStream = luceneAnalyzer.tokenStream("", new StringReader(text))
    try {
      val charTermAttribute = tokenStream.addAttribute(classOf[CharTermAttribute])
      tokenStream.reset()
      while (tokenStream.incrementToken()) {
        val term = charTermAttribute.toString.toLowerCase
        if (
          !EnglishAnalyzer.ENGLISH_STOP_WORDS_SET.contains(term) && !Utils.URL_STOP_WORDS_SET
            .contains(term)
        ) {
          tokens += term
        }
      }
    } finally {
      tokenStream.close() // Ensure the token stream is always closed
    }
    tokens
  }

  /**
    * Labels an input tuple as matched if it is present in the dictionary.
    *
    * @param tuple The input tuple to be checked against the dictionary.
    * @return A TupleLike object containing the original fields of the tuple and a boolean indicating the match status.
    */
  private def labelTupleIfMatched(tuple: Tuple): TupleLike = {
    val isMatched =
      Option(tuple.getField[Any](attributeName)).exists(_ => isTupleInDictionary(tuple))
    TupleLike(tuple.getFields ++ Seq(isMatched))
  }

  setMapFunc(labelTupleIfMatched)
}
