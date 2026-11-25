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

package org.apache.texera.amber.operator.substringSearch

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.{FilterSearchSpecSupport, LogicalOp}
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.amber.util.PythonTestSupport.NeedsPythonPackages

class SubstringSearchOpDescSpec extends FilterSearchSpecSupport {

  private def newDesc(attr: String, sub: String, caseSensitive: Boolean): SubstringSearchOpDesc = {
    val d = new SubstringSearchOpDesc
    d.attribute = attr
    d.substring = sub
    d.isCaseSensitive = caseSensitive
    d
  }

  "SubstringSearchOpDesc.operatorInfo" should
    "advertise the name, description, Search group, and reconfiguration support" in {
    val info = (new SubstringSearchOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Substring Search"
    info.operatorDescription shouldBe "Search for Substring(s) in a string column"
    info.operatorGroupName shouldBe OperatorGroupConstants.SEARCH_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
    info.supportReconfiguration shouldBe true
  }

  "SubstringSearchOpDesc.isCaseSensitive" should "default to false" in {
    (new SubstringSearchOpDesc).isCaseSensitive shouldBe false
  }

  "SubstringSearchOpDesc.getPhysicalOp" should
    "wire the SubstringSearchOpExec class name and carry forward the operatorInfo port identities" in {
    assertPhysicalOpWired(
      newDesc("col", "ub", caseSensitive = false),
      "org.apache.texera.amber.operator.substringSearch.SubstringSearchOpExec"
    )
  }

  "SubstringSearchOpDesc JSON round-trip" should
    "preserve attribute, substring, and isCaseSensitive via the polymorphic base" in {
    val json = objectMapper.writeValueAsString(newDesc("body", "lo", caseSensitive = true))
    val restored = objectMapper.readValue(json, classOf[LogicalOp])
    restored shouldBe a[SubstringSearchOpDesc]
    val ss = restored.asInstanceOf[SubstringSearchOpDesc]
    ss.attribute shouldBe "body"
    ss.substring shouldBe "lo"
    ss.isCaseSensitive shouldBe true
  }

  // Without Case Sensitive the engine lowers both sides with toLowerCase, where
  // pandas' case=False compared upper(), which folds "ß" to "SS" and "ς" to
  // "Σ". Each search runs through SubstringSearchOpExec and through the
  // export, and the rows each keeps are named by their input position.
  "SubstringSearchOpDesc.generateStandaloneCode" should
    "keep the rows the engine keeps on non-ASCII text" taggedAs NeedsPythonPackages in {
    val cases = Seq(
      ("ss", false, Seq("ß", "SS", "Strasse")),
      ("ς", false, Seq("σ", "Σ", "ς")),
      ("σ", false, Seq("Σ", "ΟΔΟΣ", "σ")),
      ("i̇", false, Seq("İ", "i", "I")),
      ("ﬀ", false, Seq("FF", "ﬀ", "ff")),
      ("é", false, Seq("É", "é", "e")),
      ("K", false, Seq("K", "k")),
      ("é", true, Seq("É", "é"))
    )
    assertStandaloneParity("substring-engine-parity-", "caseSensitive", cases)(
      newDesc("v", _, _)
    )(
      new SubstringSearchOpExec(_)
    )
  }

  // The engine reads `Body` from a column `body`, since Schema ignores case. pandas
  // would raise a KeyError on `Body`, so the script reads the column as the table
  // spells it.
  "SubstringSearchOpDesc.generateStandaloneCode" should
    "read a column named in a different case from the table" in {
    val inputSchemas =
      Map(PortIdentity() -> Schema().add(new Attribute("body", AttributeType.STRING)))
    val code = newDesc("Body", "lo", caseSensitive = false).generateStandaloneCode(inputSchemas)
    code should include("""in1df["body"]""")
    code should not include "Body"
  }
}
