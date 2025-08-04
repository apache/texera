/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.amber.operator.keywordSearch

import org.apache.lucene.analysis.{Analyzer, TokenStream}
import org.apache.lucene.analysis.core.WhitespaceTokenizer
import org.apache.lucene.analysis.CharArraySet
import org.apache.lucene.analysis.StopFilter
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents

class CaseSensitiveAnalyzer extends Analyzer {
  override protected def createComponents(fieldName: String): TokenStreamComponents = {
    val tokenizer = new WhitespaceTokenizer()
    val stream: TokenStream = new StopFilter(tokenizer, CharArraySet.EMPTY_SET)
    new TokenStreamComponents(tokenizer, stream)
  }
}
