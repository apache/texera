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

package org.apache.texera.amber.operator.source.scan.smart

import org.scalatest.flatspec.AnyFlatSpec

class CSVDialectSnifferSpec extends AnyFlatSpec {

  "CSVDialectSniffer" should "detect comma as delimiter for plain CSV" in {
    val text = "id,name,age\n1,Ada,36\n2,Lin,29\n3,Bob,42\n"
    val dialect = CSVDialectSniffer.sniff(text)
    assert(dialect.delimiter == ',')
    assert(dialect.hasHeader)
  }

  it should "detect tab as delimiter for TSV-like content" in {
    val text = "id\tname\tage\n1\tAda\t36\n2\tLin\t29\n3\tBob\t42\n"
    val dialect = CSVDialectSniffer.sniff(text)
    assert(dialect.delimiter == '\t')
    assert(dialect.hasHeader)
  }

  it should "detect semicolon as delimiter when commas are absent" in {
    val text = "id;name;age\n1;Ada;36\n2;Lin;29\n3;Bob;42\n"
    val dialect = CSVDialectSniffer.sniff(text)
    assert(dialect.delimiter == ';')
  }

  it should "detect missing header when all rows look like data" in {
    val text = "1,Ada,36\n2,Lin,29\n3,Bob,42\n4,Eve,55\n"
    val dialect = CSVDialectSniffer.sniff(text)
    assert(dialect.delimiter == ',')
    // First row is purely numeric/string mixed; later rows are the same shape.
    // The sniffer defaults to "has header" only when row 1 looks distinct.
    assert(!dialect.hasHeader)
  }

  it should "honor a preferred delimiter when the content is consistent with it" in {
    val text = "a,b,c\n1,2,3\n4,5,6\n"
    val dialect = CSVDialectSniffer.sniff(text, preferred = Some(','))
    assert(dialect.delimiter == ',')
  }
}
