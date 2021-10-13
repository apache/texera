package edu.uci.ics.texera.workflow.operators.dictionary

import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.workflow.common.operators.map.MapOpExec
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, OperatorSchemaInfo}
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute

import java.io.StringReader
import scala.collection.mutable.{ListBuffer, Set}

class DictionaryMatcherOpExec(
    opDesc: DictionaryMatcherOpDesc,
    operatorSchemaInfo: OperatorSchemaInfo
) extends MapOpExec {

  // this is needed for the matching types Phrase and Conjunction
  var tokenizedDictionaryEntries: ListBuffer[Set[String]] = _
  // this is needed for the simple Scan matching type
  var dictionaryEntries: List[String] = _
  var luceneAnalyzer: Analyzer = _

  /**
    * first prepare the dictionary by splitting the values using a comma delimiter then tokenize the splitted values
    */
  override def open(): Unit = {
    // create the dictionary by splitting the values first
    dictionaryEntries = this.opDesc.dictionary.split(",").toList.map(_.toLowerCase)
    if (opDesc.matchingType == MatchingType.CONJUNCTION_INDEXBASED) {
      // then tokenize each entry
      this.luceneAnalyzer = new EnglishAnalyzer
      tokenizedDictionaryEntries = ListBuffer[Set[String]]()
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
  def tokenizeDictionary(): Unit = {
    for (text <- dictionaryEntries) {
      tokenizedDictionaryEntries += tokenize(text)
    }
  }

  /**
    * decide if a tuple matches a dictionary after it is tokenized based on {@link MatchingType}
    *
    * @param tuple
    * @return
    */
  def isTupleInDictionary(tuple: Tuple): Boolean = {
    val text = tuple.getField(this.opDesc.attribute).asInstanceOf[String].toLowerCase()
    // depending on the matching type, iterate the dictionary to match the tuple
    if (!text.isEmpty) {
      this.opDesc.matchingType match {
        case MatchingType.SCANBASED =>
          if (
            dictionaryEntries.contains(
              text
            )
          ) {
            return true
          }
        case MatchingType.SUBSTRING =>
          for (entry <- dictionaryEntries) {
            if (
              entry.contains(
                text
              )
            ) {
              return true
            }
          }

        case MatchingType.CONJUNCTION_INDEXBASED =>
          // first, tokenize the tuple
          val tokenizedTuple = tokenize(text)
          if (!tokenizedTuple.isEmpty) {
            for (entry <- tokenizedDictionaryEntries) {
              if (entry.subsetOf(tokenizedTuple)) {
                return true
              }
            }
          }
      }

    }
    false
  }

  /**
    * tokenize a given text to a list of tokens after removing stopwords
    *
    * @param text
    * @return
    */
  def tokenize(text: String): Set[String] = {
    val tokenizedSet = Set[String]()
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
    * to process a tuple and give it a label if it is present in the dictionary,
    *
    * @param tuple
    * @return
    */
  def processTuple(tuple: Tuple): Tuple = {
    var matched: Boolean = false
    if (tuple.getField(this.opDesc.attribute) != null) {
      matched = isTupleInDictionary(tuple)
    }
    Tuple
      .newBuilder(operatorSchemaInfo.outputSchema)
      .add(tuple)
      .add(opDesc.resultAttribute, AttributeType.BOOLEAN, matched)
      .build()

  }

  setMapFunc(processTuple)
}
