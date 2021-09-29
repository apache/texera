package edu.uci.ics.texera.workflow.operators.dictionary

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, OperatorSchemaInfo}
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute
import scala.collection.mutable.Set
import java.io.StringReader
import scala.collection.Iterator
import scala.collection.mutable.ListBuffer
import scala.util.Either

class DictionaryMatcherOpExec(
    opDesc: DictionaryMatcherOpDesc,
    operatorSchemaInfo: OperatorSchemaInfo
) extends OperatorExecutor {

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
    // then tokenize each entry
    this.luceneAnalyzer = new EnglishAnalyzer
    tokenizedDictionaryEntries = ListBuffer[Set[String]]()
    tokenizeDictionary();
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
    * @param tuple
    * @return
    */
  def isTupleInDictionary(tuple: Tuple): Boolean = {
    // first, tokenize the tuple
    val tokenizedTuple = tokenize(tuple.getField(this.opDesc.attribute))
    // then depending on the matching type, iterate the dictionary to match the tuple
    if (!tokenizedTuple.isEmpty) {
      this.opDesc.matchingType match {
        // any subset matches regardless of their positions
        case MatchingType.CONJUNCTION_INDEXBASED =>
          for (entry <- tokenizedDictionaryEntries) {
            if (entry.subsetOf(tokenizedTuple) || tokenizedTuple.subsetOf(entry)) {
              return true
            }
          }

        /**
          * here, we convert the data structure to a list to maintain the order to match a continuous phrase using the function {@link ListBuffer.containsSlice()}
          */
        case MatchingType.PHRASE_INDEXBASED =>
          for (entry <- tokenizedDictionaryEntries) {
            if (
              entry.toList.containsSlice(tokenizedTuple.toList) || tokenizedTuple.toList
                .containsSlice(entry.toList)
            ) {
              return true
            }
          }
        case MatchingType.SCANBASED =>
          for (entry <- dictionaryEntries) {
            if (
              entry.contains(
                tuple.getField(this.opDesc.attribute).asInstanceOf[String].toLowerCase()
              )
            ) {
              return true
            }
          }
      }

    }
    false
  }

  /**
    * tokenize a given text to a list of tokens after removing stopwords
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
      if (!EnglishAnalyzer.ENGLISH_STOP_WORDS_SET.contains(termStr))
        tokenizedSet += termStr
    }
    tokenStream.close()
    tokenizedSet
  }

  /**
    * to process a tuple and give it a label if it is present in the dictionary,
    * @param tuple
    * @param input
    * @return
    */
  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: LinkIdentity
  ): Iterator[Tuple] =
    tuple match {
      case Left(t) =>
        var matched: Boolean = false
        if (t.getField(this.opDesc.attribute) != null) {
          matched = isTupleInDictionary(t)
        }
        val result = Tuple
          .newBuilder(operatorSchemaInfo.outputSchema)
          .add(t)
          .add(opDesc.resultAttribute, AttributeType.BOOLEAN, matched)
          .build()
        Iterator(result)
      case Right(_) => Iterator()
    }
}
