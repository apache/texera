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

package org.apache.texera.amber.operator.regex

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.{FilterSearchSpecSupport, LogicalOp}
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.amber.util.PythonTestSupport.NeedsPythonPackages

class RegexOpDescSpec extends FilterSearchSpecSupport {

  private def newDesc(attr: String, re: String, caseInsensitive: Boolean): RegexOpDesc = {
    val d = new RegexOpDesc
    d.attribute = attr
    d.regex = re
    d.caseInsensitive = caseInsensitive
    d
  }

  "RegexOpDesc.operatorInfo" should
    "advertise the name, Search group, and reconfiguration support" in {
    val info = (new RegexOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Regular Expression"
    info.operatorDescription.toLowerCase should include("regular expression")
    info.operatorGroupName shouldBe OperatorGroupConstants.SEARCH_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
    info.supportReconfiguration shouldBe true
  }

  "RegexOpDesc.getPhysicalOp" should "wire the RegexOpExec class name and carry ports" in {
    assertPhysicalOpWired(
      newDesc("col", ".*foo.*", caseInsensitive = false),
      "org.apache.texera.amber.operator.regex.RegexOpExec"
    )
  }

  "RegexOpDesc JSON round-trip" should
    "preserve attribute, regex, and caseInsensitive via the polymorphic base" in {
    val json = objectMapper.writeValueAsString(newDesc("url", "^https?://", caseInsensitive = true))
    val restored = objectMapper.readValue(json, classOf[LogicalOp])
    restored shouldBe a[RegexOpDesc]
    val re = restored.asInstanceOf[RegexOpDesc]
    re.attribute shouldBe "url"
    re.regex shouldBe "^https?://"
    re.caseInsensitive shouldBe true
  }

  "RegexOpDesc.unicodeBoundaries" should
    "scope \\b and \\B to Unicode outside a class and leave the rest alone" in {
    RegexOpDesc.unicodeBoundaries("""\bcafé\B""") shouldBe """(?u:\b)café(?u:\B)"""
    RegexOpDesc.unicodeBoundaries("""\\b""") shouldBe """\\b"""
    RegexOpDesc.unicodeBoundaries("""[\b]\b""") shouldBe """[\b](?u:\b)"""
    RegexOpDesc.unicodeBoundaries("""[]\b]\b""") shouldBe """[]\b](?u:\b)"""
    RegexOpDesc.unicodeBoundaries("""[^]\b]\b""") shouldBe """[^]\b](?u:\b)"""
    RegexOpDesc.unicodeBoundaries("""\[\b""") shouldBe """\[(?u:\b)"""
  }

  "RegexOpDesc.unicodeCase" should
    "scope a leading u around the rest and keep each shorthand ASCII" in {
    RegexOpDesc.unicodeCase("""\w[é\d]""") shouldBe """\w[é\d]"""
    RegexOpDesc.unicodeCase("(?iu)é") shouldBe "(?i)(?u:é)"
    RegexOpDesc.unicodeCase("""(?u)\w[\s][é\d][^\W][^é\S][\w^]""") shouldBe
      """(?u:(?a:\w)(?a:[\s])(?:[é]|(?a:[\d]))(?a:[^\W])(?:(?![é])(?a:[^\S]))(?:[\^]|(?a:[\w])))"""
    RegexOpDesc.unicodeCase("""x(?iu:é)\d\\w""") shouldBe """x(?iu:é)(?a:\d)\\w"""
    RegexOpDesc.unicodeCase("(?iux)é # note") shouldBe "(?ix)(?u:é # note\n)"
    RegexOpDesc.unicodeCase("(?i-u)é") shouldBe "(?i-u)é"
    RegexOpDesc.unicodeCase("(?u-i)é") shouldBe "(?-i)(?u:é)"
    RegexOpDesc.unicodeCase("(?x)(?u)é # cmt") shouldBe "(?x)(?u:é # cmt\n)"
  }

  // Pattern without UNICODE_CASE ignores case for ASCII letters only, and its
  // \w, \d and \s are ASCII, while Java 17's \b takes any letter or digit as
  // part of a word. Each pattern runs through RegexOpExec and through the
  // export, and the rows each keeps are named by their input position.
  "RegexOpDesc.generateStandaloneCode" should
    "keep the rows the engine keeps on non-ASCII text" taggedAs NeedsPythonPackages in {
    val cases = Seq(
      ("é", true, Seq("É", "é", "e")),
      ("σ", true, Seq("Σ", "σ", "ς")),
      ("k", true, Seq("K", "k", "\u212a")),
      ("ǅ", true, Seq("ǆ", "Ǆ", "ǅ")),
      ("""\w+""", false, Seq("é", "e", "\u0663", "_")),
      ("""\d""", false, Seq("\u0663", "3", "x")),
      ("""\s""", false, Seq("\u00a0", " ", "\u2003")),
      ("""^\w$""", false, Seq("ß", "s")),
      ("""\bcafé\b""", false, Seq("cafés", "café", "un café noir")),
      ("""\Bé""", false, Seq("café", "é")),
      ("""\\b""", false, Seq("""\b""", "b")),
      // An inline u folds case by Unicode and leaves \w, \d and \s ASCII.
      ("(?iu)é", false, Seq("é", "É", "x", null)),
      ("(?u)é", true, Seq("É", "é", "x")),
      ("""(?iu)\w""", false, Seq("é", "e", "\u212a")),
      ("""(?iu)^[é\w]+$""", false, Seq("Éa", "e", "!")),
      ("""(?iu)[^é\W]""", false, Seq("É", "é", "e")),
      ("""x(?iu:é)\d""", false, Seq("xÉ3", "xÉ\u0663", "XÉ3")),
      ("(?iux)é # note", false, Seq("É", "x"))
    )
    assertStandaloneParity("regex-engine-parity-", "caseInsensitive", cases)(
      newDesc("v", _, _)
    )(
      new RegexOpExec(_)
    )
  }

  // The engine reads `Url` from a column `url`, since Schema ignores case. pandas
  // would raise a KeyError on `Url`, so the script reads the column as the table
  // spells it.
  "RegexOpDesc.generateStandaloneCode" should
    "read a column named in a different case from the table" in {
    val inputSchemas =
      Map(PortIdentity() -> Schema().add(new Attribute("url", AttributeType.STRING)))
    val code =
      newDesc("Url", "^https?://", caseInsensitive = false).generateStandaloneCode(inputSchemas)
    code should include("""in1df["url"]""")
    code should not include "Url"
  }
}
