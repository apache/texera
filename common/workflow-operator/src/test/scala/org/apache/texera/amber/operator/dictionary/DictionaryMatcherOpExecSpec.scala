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

import org.apache.texera.amber.core.tuple._
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class DictionaryMatcherOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val tupleSchema: Schema = Schema()
    .add(new Attribute("field1", AttributeType.STRING))
    .add(new Attribute("field2", AttributeType.INTEGER))
    .add(new Attribute("field3", AttributeType.BOOLEAN))

  val tuple: Tuple = Tuple
    .builder(tupleSchema)
    .add(new Attribute("field1", AttributeType.STRING), "nice a a person")
    .add(new Attribute("field2", AttributeType.INTEGER), 1)
    .add(
      new Attribute("field3", AttributeType.BOOLEAN),
      true
    )
    .build()

  var opExec: DictionaryMatcherOpExec = _
  val opDesc: DictionaryMatcherOpDesc = new DictionaryMatcherOpDesc()
  var outputSchema: Schema = _
  val dictionaryScan = "nice a a person"
  val dictionarySubstring = "nice a a person and good"
  val dictionaryConjunction = "a person is nice"

  before {
    opDesc.attribute = "field1"
    opDesc.dictionary = dictionaryScan
    opDesc.resultAttribute = "matched"
    opDesc.matchingType = MatchingType.SCANBASED
    outputSchema = opDesc.getExternalOutputSchemas(Map(PortIdentity() -> tupleSchema)).values.head
  }

  it should "open" in {
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    assert(opExec.dictionaryEntries != null)
  }

  /**
    * Test cases that all Matching Types should match the query
    */
  it should "match a tuple if present in the given dictionary entry when matching type is SCANBASED" in {
    opDesc.matchingType = MatchingType.SCANBASED
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val processedTuple = opExec.processTuple(tuple, 0).next()
    assert(
      processedTuple.asInstanceOf[SchemaEnforceable].enforceSchema(outputSchema).getField("matched")
    )
    opExec.close()
  }

  it should "match a tuple if present in the given dictionary entry when matching type is SUBSTRING" in {
    opDesc.matchingType = MatchingType.SUBSTRING
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val processedTuple = opExec.processTuple(tuple, 0).next()
    assert(
      processedTuple.asInstanceOf[SchemaEnforceable].enforceSchema(outputSchema).getField("matched")
    )
    opExec.close()
  }

  it should "match a tuple if present in the given dictionary entry when matching type is CONJUNCTION_INDEXBASED" in {
    opDesc.matchingType = MatchingType.CONJUNCTION_INDEXBASED
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val processedTuple = opExec.processTuple(tuple, 0).next()
    assert(
      processedTuple.asInstanceOf[SchemaEnforceable].enforceSchema(outputSchema).getField("matched")
    )
    opExec.close()
  }

  /**
    * Test cases that SCANBASED and SUBSTRING Matching Types should fail to match a query
    */
  it should "not match a tuple if not present in the given dictionary entry when matching type is SCANBASED and not exact match" in {
    opDesc.dictionary = dictionaryConjunction
    opDesc.matchingType = MatchingType.SCANBASED
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val processedTuple = opExec.processTuple(tuple, 0).next()
    assert(
      !processedTuple
        .asInstanceOf[SchemaEnforceable]
        .enforceSchema(outputSchema)
        .getField[Boolean]("matched")
    )
    opExec.close()
  }

  it should "not match a tuple if the given dictionary entry doesn't contain all the tuple when the matching type is SUBSTRING" in {
    opDesc.dictionary = dictionaryConjunction
    opDesc.matchingType = MatchingType.SUBSTRING
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val processedTuple = opExec.processTuple(tuple, 0).next()
    assert(
      !processedTuple
        .asInstanceOf[SchemaEnforceable]
        .enforceSchema(outputSchema)
        .getField[Boolean]("matched")
    )
    opExec.close()
  }

  it should "match a tuple if present in the given dictionary entry when matching type is CONJUNCTION_INDEXBASED even with different order" in {
    opDesc.dictionary = dictionaryConjunction
    opDesc.matchingType = MatchingType.CONJUNCTION_INDEXBASED
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val processedTuple = opExec.processTuple(tuple, 0).next()
    assert(
      processedTuple
        .asInstanceOf[SchemaEnforceable]
        .enforceSchema(outputSchema)
        .getField[Boolean]("matched")
    )
    opExec.close()
  }

  /**
    * Test cases that only SUBSTRING Matching Type should match the query
    */
  it should "not match a tuple if not present in the given dictionary entry when matching type is SCANBASED when the entry contains more text" in {
    opDesc.dictionary = dictionarySubstring
    opDesc.matchingType = MatchingType.SCANBASED
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val processedTuple = opExec.processTuple(tuple, 0).next()
    assert(
      !processedTuple
        .asInstanceOf[SchemaEnforceable]
        .enforceSchema(outputSchema)
        .getField[Boolean]("matched")
    )
    opExec.close()
  }

  it should "not match a tuple if not present in the given dictionary entry when matching type is CONJUNCTION_INDEXBASED when the entry contains more text" in {
    opDesc.dictionary = dictionarySubstring
    opDesc.matchingType = MatchingType.CONJUNCTION_INDEXBASED
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val processedTuple = opExec.processTuple(tuple, 0).next()
    assert(
      !processedTuple
        .asInstanceOf[SchemaEnforceable]
        .enforceSchema(outputSchema)
        .getField[Boolean]("matched")
    )
    opExec.close()
  }

  it should "match a tuple if not present in the given dictionary entry when matching type is SUBSTRING when the entry contains more text" in {
    opDesc.dictionary = dictionarySubstring
    opDesc.matchingType = MatchingType.SUBSTRING
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val processedTuple = opExec.processTuple(tuple, 0).next()
    assert(
      processedTuple
        .asInstanceOf[SchemaEnforceable]
        .enforceSchema(outputSchema)
        .getField[Boolean]("matched")
    )
    opExec.close()
  }

  it should "close properly" in {
    opDesc.matchingType = MatchingType.SCANBASED
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    opExec.close()
    assert(opExec.dictionaryEntries == null)
    // SCANBASED never allocates the tokenized buffer, so null here is this path's own
    // contract rather than a leftover from whichever test ran last. close()'s other
    // arm -- the buffer exists and is cleared, not nulled -- is pinned by "empty the
    // tokenized dictionary on close" below.
    assert(opExec.tokenizedDictionaryEntries == null)
    assert(opExec.luceneAnalyzer == null)
    // Idempotent: a second close() re-enters with all three fields already at their
    // post-close values and must not throw.
    opExec.close()
    assert(opExec.dictionaryEntries == null)
  }

  private def tupleWith(field1: String): Tuple =
    Tuple
      .builder(tupleSchema)
      .add(new Attribute("field1", AttributeType.STRING), field1)
      .add(new Attribute("field2", AttributeType.INTEGER), 1)
      .add(new Attribute("field3", AttributeType.BOOLEAN), true)
      .build()

  private def isMatched(inputTuple: Tuple): Boolean =
    opExec
      .processTuple(inputTuple, 0)
      .next()
      .asInstanceOf[SchemaEnforceable]
      .enforceSchema(outputSchema)
      .getField[Boolean]("matched")

  it should "not label an empty field as matched even though every entry contains the empty string" in {
    opDesc.dictionary = dictionaryScan
    opDesc.matchingType = MatchingType.SUBSTRING
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    // Without the empty-text guard, SUBSTRING would ask whether "nice a a person"
    // contains "" -- which every string does -- and report a match.
    assert(!isMatched(tupleWith("")))
    opExec.close()
  }

  it should "not match under CONJUNCTION_INDEXBASED when the field tokenizes to nothing" in {
    opDesc.dictionary = "the"
    opDesc.matchingType = MatchingType.CONJUNCTION_INDEXBASED
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    // "the" is an English stop word, so both the entry and the field tokenize to
    // the empty set, and the empty set is trivially a subset of itself. Only the
    // explicit non-emptiness check keeps this from being reported as a match.
    assert(!isMatched(tupleWith("the")))
    opExec.close()
  }

  it should "ignore dictionary tokens that stem into an English stop word" in {
    opDesc.dictionary = "nice willing person"
    opDesc.matchingType = MatchingType.CONJUNCTION_INDEXBASED
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    // The analyzer stems "willing" to "will", which is an English stop word, so the
    // entry reduces to {nice, person} and is still a subset of the field's
    // {person, nice}. The field deliberately reverses the two words so that plain
    // substring containment ("nice willing person" contains "person nice") is
    // false: only the tokenized conjunction path can satisfy this assertion.
    assert(isMatched(tupleWith("person nice")))
    opExec.close()
  }

  it should "ignore dictionary tokens that are URL stop words" in {
    // Each of these survives the English analyzer unchanged and is not an English
    // stop word, so each is dropped only by the operator's own URL list. "https"
    // is deliberately absent: the Porter stemmer rewrites it to "http", so that
    // entry can never fire (recorded as dead data, not pinned). As above, the
    // field reverses the word order so substring containment cannot satisfy it.
    for (urlWord <- List("http", "org", "net", "com", "store", "www", "html")) {
      opDesc.dictionary = s"nice $urlWord person"
      opDesc.matchingType = MatchingType.CONJUNCTION_INDEXBASED
      opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
      opExec.open()
      withClue(s"URL stop word $urlWord: ")(assert(isMatched(tupleWith("person nice"))))
      opExec.close()
    }
  }

  it should "split the dictionary on commas" in {
    opDesc.dictionary = "cat,dog"
    opDesc.matchingType = MatchingType.SCANBASED
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    // Two entries, not one: "dog" is an entry on its own and the raw text is not.
    assert(isMatched(tupleWith("dog")))
    assert(!isMatched(tupleWith("cat,dog")))
    opExec.close()
  }

  it should "lower-case the tuple field before comparing it to the dictionary" in {
    opDesc.dictionary = dictionaryScan
    opDesc.matchingType = MatchingType.SCANBASED
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    // Dictionary entries are lower-cased when they are split, so the field has to
    // be folded too or an upper-case field could never match a lower-case entry.
    assert(isMatched(tupleWith(dictionaryScan.toUpperCase)))
    opExec.close()
  }

  it should "report a null field as unmatched instead of failing" in {
    opDesc.dictionary = dictionaryScan
    opDesc.matchingType = MatchingType.SCANBASED
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    // The dictionary lookup casts the field to String and lower-cases it, so a null
    // field has to be filtered out before the lookup is reached.
    assert(!isMatched(tupleWith(null)))
    opExec.close()
  }

  it should "empty the tokenized dictionary on close" in {
    opDesc.dictionary = "nice person"
    opDesc.matchingType = MatchingType.CONJUNCTION_INDEXBASED
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    assert(opExec.tokenizedDictionaryEntries.nonEmpty)
    opExec.close()
    // Asserts emptiness only, never null: close() nulls the other two fields but
    // merely clears this one, and this test takes no side on that asymmetry.
    assert(opExec.tokenizedDictionaryEntries.isEmpty)
  }

  it should "fail loudly rather than silently report no match when no matching type is configured" in {
    opDesc.dictionary = dictionaryScan
    opDesc.matchingType = null
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    // A descriptor whose "Matching type" is absent from the JSON deserializes to a
    // null matchingType -- Jackson does not enforce `required` on read -- so this is
    // a reachable runtime state. What is pinned is only that the operator refuses
    // it instead of labelling every tuple "not matched". The exception's identity
    // is scalac's business (today a MatchError from the non-exhaustive match) and
    // is deliberately not named, so replacing it with an explicit, clearer throw
    // would not break this test.
    intercept[RuntimeException] {
      opExec.processTuple(tuple, 0).next()
    }
    opExec.close()
  }
}
