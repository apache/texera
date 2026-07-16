/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.operator.dictionary

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{
  InputPort,
  OutputPort,
  PhysicalOp,
  SchemaPropagationFunc
}
import org.apache.texera.amber.operator.StandaloneCodeGenerator
import org.apache.texera.amber.operator.map.MapOpDesc
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.util.JSONUtils.objectMapper

/**
  * Dictionary matcher operator matches a tuple if the specified column is in the given dictionary.
  * It outputs an extra column to label the tuple if it is matched or not
  * This is the description of the operator
  */
@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "Attribute": { "enum": ["string"] }
  }
}
""")
class DictionaryMatcherOpDesc extends MapOpDesc with StandaloneCodeGenerator {
  @JsonProperty(value = "Dictionary", required = true)
  @JsonPropertyDescription("dictionary values separated by a comma") var dictionary: String = _

  @JsonProperty(value = "Attribute", required = true)
  @JsonPropertyDescription("column name to match")
  @AutofillAttributeName var attribute: String = _

  @JsonProperty(value = "result attribute", required = true, defaultValue = "matched")
  @JsonPropertyDescription("column name of the matching result") var resultAttribute: String = _

  @JsonProperty(value = "Matching type", required = true) var matchingType: MatchingType = _

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {

    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "org.apache.texera.amber.operator.dictionary.DictionaryMatcherOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(inputSchemas => {
          if (resultAttribute == null || resultAttribute.trim.isEmpty) return null
          Map(
            operatorInfo.outputPorts.head.id -> inputSchemas.values.head
              .add(new Attribute(resultAttribute, AttributeType.BOOLEAN))
          )
        })
      )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Dictionary matcher",
      "Matches tuples if they appear in a given dictionary",
      OperatorGroupConstants.SEARCH_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort()),
      supportReconfiguration = true
    )

  override def generateStandaloneCode(): String = {
    // JVM splits the dictionary on "," and lowercases each entry — no trim,
    // so leading/trailing whitespace around a comma becomes part of the entry.
    val rawDict = Option(dictionary).getOrElse("")
    val entries = rawDict.split(",").toList.map(_.toLowerCase)
    val resultCol = Option(resultAttribute)
      .filter(_.trim.nonEmpty)
      .getOrElse("matched")
    val attrPy = toPyDoubleQuotedLiteral(Option(attribute).getOrElse(""))
    val resultPy = toPyDoubleQuotedLiteral(resultCol)
    val mt = Option(matchingType).getOrElse(MatchingType.SCANBASED)

    val entriesLiteral = entries.map(toPyDoubleQuotedLiteral).mkString("[", ", ", "]")

    mt match {
      case MatchingType.SCANBASED =>
        // Exact case-insensitive equality between the (lowercased) cell value
        // and any dictionary entry. Null or empty text never matches, matching
        // JVM behavior.
        s"""out1df = in1df.copy()
           |out1df[$resultPy] = out1df[$attrPy].apply(
           |    lambda v, _entries=$entriesLiteral: (
           |        False if pd.isna(v)
           |        else (str(v).lower() != "" and str(v).lower() in _entries)
           |    )
           |)""".stripMargin

      case MatchingType.SUBSTRING =>
        // JVM checks dictionaryEntries.exists(entry => entry.contains(text)) —
        // the cell value (lowercased) is a substring of some dictionary entry,
        // NOT the other way around.
        s"""out1df = in1df.copy()
           |out1df[$resultPy] = out1df[$attrPy].apply(
           |    lambda v, _entries=$entriesLiteral: (
           |        False if pd.isna(v)
           |        else (str(v).lower() != "" and any(str(v).lower() in _e for _e in _entries))
           |    )
           |)""".stripMargin

      case MatchingType.CONJUNCTION_INDEXBASED =>
        // JVM tokenizes via Lucene EnglishAnalyzer (StandardTokenizer + lowercase
        // + English stop words + possessive filter + Porter2 stemmer) and matches
        // when an entry's token set is a subset of the text's token set. Best-
        // effort reproduction: regex \w+ tokens, lowercase, drop the same stop
        // word lists, but NO stemming — morphological variants ("book" vs "books")
        // will diverge from JVM behavior.
        val tokenSetsLiteral = entries
          .map(tokenizeForConjunction)
          .map(toks => toks.toList.sorted.map(toPyDoubleQuotedLiteral).mkString("frozenset({", ", ", "})"))
          .mkString("[", ", ", "]")
        val stopWordsLiteral = DictionaryMatcherOpDesc.STOP_WORDS.toList.sorted
          .map(toPyDoubleQuotedLiteral)
          .mkString("frozenset({", ", ", "})")
        s"""import re as _texera_dm_re
           |_TEXERA_DM_STOPWORDS = $stopWordsLiteral
           |def _texera_dm_tokenize(text):
           |    return frozenset(t for t in _texera_dm_re.findall(r"\\w+", text.lower()) if t not in _TEXERA_DM_STOPWORDS)
           |out1df = in1df.copy()
           |out1df[$resultPy] = out1df[$attrPy].apply(
           |    lambda v, _entries=$tokenSetsLiteral: (
           |        False if pd.isna(v)
           |        else (lambda _t: bool(_t) and any(_e.issubset(_t) for _e in _entries))(_texera_dm_tokenize(str(v)))
           |    )
           |)""".stripMargin
    }
  }

  private def tokenizeForConjunction(text: String): Set[String] = {
    val wordRe = "\\w+".r
    wordRe
      .findAllIn(text.toLowerCase)
      .toSet
      .filterNot(DictionaryMatcherOpDesc.STOP_WORDS.contains)
  }

  private def toPyDoubleQuotedLiteral(s: String): String =
    "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
}

private object DictionaryMatcherOpDesc {
  // Mirrors Lucene's EnglishAnalyzer.ENGLISH_STOP_WORDS_SET plus the URL stop
  // words filtered by DictionaryMatcherOpExec.
  val STOP_WORDS: Set[String] = Set(
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if",
    "in", "into", "is", "it", "no", "not", "of", "on", "or", "such",
    "that", "the", "their", "then", "there", "these", "they", "this",
    "to", "was", "will", "with",
    "http", "https", "org", "net", "com", "store", "www", "html"
  )
}
